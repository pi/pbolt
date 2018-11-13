unit bolt;

interface

uses pl, hash, monotime;

type
  TTx = class;
  TDB = class;
  TBucket = class;

  pgid = int64;

  txid = int64;

  TINode = record
	  flags : cardinal;
	  pgid  : pgid;
	  key   : array of byte;
	  value : array of byte;
  end;
  PINode = ^TINode;

  TNode = class
  private
	  bucket     : TBucket;
	  isLeaf     : boolean;
	  unbalanced : boolean;
	  spilled    : boolean;
	  key        : pbyte;
	  pgid       : pgid;
	  parent     : TNode;
	  children   : array of TNode;
	  inodes     : array of TINode;
  end;

  TPage = record
    id       : pgid;
    flags    : word;
    count    : word;
    overflow : cardinal;
    ptr      : PtrUint;
  end;
  PPage = ^TPage;

  TBranchPageElement = record
    pos   : cardinal;
    ksize : cardinal;
    pgid  : pgid;
  end;
  PBranchPageElement = ^TBranchPageElement;

// leafPageElement represents a node on a leaf page.
  TLeafPageElement = record
    flags : cardinal;
    pos   : cardinal;
    ksize : cardinal;
    vsize : cardinal;
  end;
  PLeafPageElement = ^TLeafPageElement;

  TBucketRec = record
    root     : pgid;   // page id of the bucket's root-level page
    sequence : int64; // monotonically incrementing, used by NextSequence()
  end;

  TMeta = record
	  magic    : cardinal;
	  version  : cardinal;
	  pageSize : cardinal;
	  flags    : cardinal;
	  root     : TBucketRec;
	  freelist : pgid;
	  pgid     : pgid;
	  txid     : txid;
	  checksum : int64;
  end;

  TtxPending = class
  private
	  ids              : array of pgid;
	  alloctx          : array of txid; // txids allocating the ids
	  lastReleaseBegin : txid;   // beginning txid of last matching releaseRange
  end;

  TFreelist = class
  private
	  ids     : array of pgid;  // all free and available free page ids.
	  allocs  : TQQHash;      // mapping of txid that allocated a pgid.
	  pending : TQHash;         // mapping of soon-to-be free page ids by tx.
	  cache   : TQHashSet;          // fast lookup of all free and pending page ids.
  public
    constructor Create;
  end;

// Bucket represents a collection of key/value pairs inside the database.
  TBucket = class
  public
    // bucket part
    root      : pgid;
    sequence  : uint64;
    // Bucket part
	  tx        : TTx;                // the associated transaction
	  buckets   : TSPtrHash; // subbucket cache
	  page      : PPage;              // inline page reference
	  rootNode  : TNode;              // materialized node for the root page.
	  nodes     : TQPtrHash;            // node cache

	  // Sets the threshold for filling nodes when they split. By default,
	  // the bucket will fill to 50% but it can be useful to increase this
	  // amount if you know that your write workloads are mostly append-only.
	  //
	  // This is non-persisted across transactions so it must be set in every Tx.
	  FillPercent : double;
  end;

  TxStats = record
	  // Page statistics.
	  PageCount : integer; // number of page allocations
	  PageAlloc : integer; // total bytes allocated

	  // Cursor statistics.
	  CursorCount : integer; // number of cursors created

	  // Node statistics
	  NodeCount : integer; // number of node allocations
	  NodeDeref : integer; // number of node dereferences

	  // Rebalance statistics.
	  Rebalance     : integer;           // number of node rebalances
	  RebalanceTime : TMonotime; // total time spent rebalancing

	  // Split/Spill statistics.
	  Split     : integer;           // number of nodes split
	  Spill     : integer;           // number of nodes spilled
	  SpillTime : TMonotime; // total time spent spilling

	  // Write statistics.
	  Write     : integer;           // number of writes performed
	  WriteTime : TMonotime; // total time spent writing to disk
  end;


  TTx = class
  private
  end;

function PageType(p: PPage): string;

const
  pageHeaderSize = sizeof(TPage) - sizeof(PtrUint);

const minKeysPerPage = 2;

const branchPageElementSize = sizeof(TBranchPageElement);
const leafPageElementSize = sizeof(TLeafPageElement);

const
  branchPageFlag   = $01;
  leafPageFlag     = $02;
  metaPageFlag     = $04;
  freelistPageFlag = $10;

const
  bucketLeafFlag = $01;

const
  // MaxKeySize is the maximum length of a key, in bytes.
  MaxKeySize = 32768;

  // MaxValueSize is the maximum length of a value, in bytes.
  MaxValueSize = (1 shl 31) - 2;

const bucketHeaderSize = sizeof(TBucketRec);

const
  minFillPercent = 0.1;
  maxFillPercent = 1.0;

// DefaultFillPercent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
const DefaultFillPercent = 0.5;

implementation

uses sysutils;

function page.d: ppage_data;
begin
  result := ppage_data(self);
end;

function page.typ: string;
begin
  if (d^.flags and branchPageFlag) <> 0 then
    result := 'branch'
  else if (d^.flags and leafPageFlag) <> 0 then
    result := 'leaf'
  else if (d^.flags and metaPageFlag) <> 0 then
    result := 'meta'
  else if (d^.flags and freelistPageFlag) <> 0 then
    result := 'freelist'
  else
    result := Format('unknown<%02x>', [d^.flags])
end;

end.
