unit bolt_impl;

{$mode delphi} // for now

interface

uses bolt;

// Options represents the options that can be set when opening a database.
type
  TOptions = record
    // Timeout is the amount of time to wait to obtain a file lock.
    // When set to zero it will wait indefinitely. This option is only
    // available on Darwin and Linux.
    Timeout: cardinal;

    // Sets the DB.NoGrowSync flag before memory mapping the file.
    NoGrowSync: boolean;

    // Do not sync freelist to disk. This improves the database write performance
    // under normal operation, but requires a full database re-sync during recovery.
    NoFreelistSync: boolean;

    // Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
    // grab a shared lock (UNIX).
    ReadOnly: boolean;

    // Sets the DB.MmapFlags flag before memory mapping the file.
    MmapFlags: integer;

    // InitialMmapSize is the initial mmap size of the database
    // in bytes. Read transactions won't block write transaction
    // if the InitialMmapSize is large enough to hold database mmap
    // size. (See DB.Begin for more information)
    //
    // If <=0, the initial map size is 0.
    // If initialMmapSize is smaller than the previous database size,
    // it takes no effect.
    InitialMmapSize: SizeUint;

    // PageSize overrides the default OS page size.
    PageSize: SizeUint;

    // NoSync sets the initial value of DB.NoSync. Normally this can just be
    // set directly on the DB itself when returned from Open(), but this option
    // is useful in APIs which expose Options but not the underlying DB.
    NoSync: boolean;
  end;
  POptions = ^TOptions;

function OpenBoltDatabase(const name: string; opts: POptions = nil): bolt.IDB;

implementation

uses pl, hash, monotime, sysutils, crc64;

const
  Version = 2;
// Represents a marker value to indicate that a file is a Bolt DB.
  Magic = $ED0CDAED;

const
  maxMapSize = int64($FFFFFFFFFFFF); // 256TB

// maxAllocSize is the size used when creating array pointers.
  maxAllocSize = $7FFFFFFF;


procedure Error(const msg: string);
begin
  raise Exception.Create(msg);
end;

const
  DefaultMaxBatchSize   = 1000;
  DefaultMaxBatchDelay  = 10;
  DefaultAllocSize      = 16 * 1024 * 1024;

var
  DefaultOptions: TOptions; // all zeros

type
  TTx = class;
  TDB = class;
  TBucket = class;

  txid = uint64;

  pgid = uint64;

  inode = record
    flags : cardinal;
    pgid  : pgid;
    key   : array of byte;
    value : array of byte;
  end;
  pinode = ^inode;

  node = class
  public
    bucket     : TBucket;
    isLeaf     : boolean;
    unbalanced : boolean;
    spilled    : boolean;
    key        : pbyte;
    pgid       : pgid;
    parent     : node;
    children   : array of node;
    inodes     : array of pinode;
  end;

  page = record
    id       : pgid;
    flags    : word;
    count    : word;
    overflow : cardinal;
    ptr      : PtrUint;
  end;
  Ppage = ^page;

  branchPageElement = record
    pos   : cardinal;
    ksize : cardinal;
    pgid  : pgid;
  end;
  PbranchPageElement = ^branchPageElement;
  TBranchPageElementArray = array [0..0] of branchPageElement;
  PBranchPageElementArray = ^TBranchPageElementArray;

  leafPageElement = record
    flags : cardinal;
    pos   : cardinal;
    ksize : cardinal;
    vsize : cardinal;
  end;
  PleafPageElement = ^leafPageElement;

  bucket = record
    root     : pgid;   // page id of the bucket's root-level page
    sequence : uint64; // monotonically incrementing, used by NextSequence()
  end;

  meta = record
    magic    : cardinal;
    version  : cardinal;
    pageSize : cardinal;
    flags    : cardinal;
    root     : bucket;
    freelist : pgid;
    pgid     : pgid;
    txid     : txid;
    checksum : uint64;
  end;
  Pmeta = ^meta;

  TtxPending = class
  public
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
    destructor Destroy; override;
  end;

// Bucket represents a collection of key/value pairs inside the database.
  TBucket = class(TInterfacedObject, bolt.IBucket)
  public
    // bucket part
    root      : pgid;
    sequence  : uint64;
    // Bucket part
    tx        : TTx;    // the associated transaction
    buckets   : TSHash; // subbucket cache
    page      : Ppage;  // inline page reference
    rootNode  : node;   // materialized node for the root page.
    nodes     : TQHash; // node cache

    // Sets the threshold for filling nodes when they split. By default,
    // the bucket will fill to 50% but it can be useful to increase this
    // amount if you know that your write workloads are mostly append-only.
    //
    // This is non-persisted across transactions so it must be set in every Tx.
    FillPercent : double;

  protected
    procedure Dereference;
  public // interface implementation
    function Bucket(const name: string): IBucket; // get existing or create new if tx is rw

    function Cursor: ICursor; // return cursor for enumerating keys with values. subbuckets are omitted
    function BucketsCursor: ICursor; // return cursor for enumerating names of subbuckets. regular keys are omitted

    function Put(const kv: TKV; overwrite: boolean): boolean;
    function Get(const key: TDbValue; out value: TDbValue): boolean;
  end;

  TCommitHandler = procedure;

  TTx = class(TInterfacedObject, ITX)
  private
    fWritable      : boolean;
    managed        : boolean;
    fDb            : TDB;
    meta           : Pmeta;
    root           : TBucket;
    pages          : TQPtrHash; //map[pgid]*page
    stats          : TtxStats;
    commitHandlers : array of TCommitHandler;
  public
    WriteFlag : integer;
  public // interface methods
    function DB: IDB;
    function DbSize: SizeUint;

    function ID: txid;

    function Writable: boolean;

    function Bucket(const name: string; createIfNotExists: boolean): IBucket;
    procedure DeleteBucket(const name: string);

    function Cursor: ICursor; // return cursor for enumerating names of DB root buckets

    procedure Commit;
    procedure Done; // rollback if tx is active
  end;

  TDB = class(TInterfacedObject, IDB)
  private
    fPath: string;
    fOpened: boolean;
    fFd: integer;
    fWritable: boolean;
    fNoSync: boolean;
    fNoGrowSync: boolean;
    fMmapFlags: integer;
    fNoFreelistSync: boolean;
    fMaxBatchSize: SizeUint;
    fMaxBatchDelay: integer;
    fAllocSize: SizeUint;
    fPageSize: SizeUint;
    fMmapLock: TRtlCriticalSection;

    fRwTx: TTx;

    procedure InitDbFile;
    function AdjustMmapSize(size: int64): int64;
  public
    constructor Create(const aFileName: string; opt: POptions);
    destructor Destroy; override;
  public // interface methods
    procedure Close;

    function Writable: boolean;

    function BeginTx(readOnly: boolean): ITx;

    procedure Sync;
  end;

  TCursor = class(TInterfacedObject, bolt.ICursor)
  public
    function First(out kv: TKV): boolean;
    function Last(out kv: TKV): boolean;
    function Next(out kv: TKV): boolean;
    function Prev(out kv: TKV): boolean;
    function Seek(const key: TDbValue; out kv: TKV): boolean; // key can be partial
    procedure Delete;
    function Bucket: IBucket;
  end;

const
  pageHeaderSize = sizeof(page) - sizeof(PtrUint);

const minKeysPerPage = 2;

const branchPageElementSize = sizeof(branchPageElement);
const leafPageElementSize = sizeof(leafPageElement);

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

const bucketHeaderSize = sizeof(bucket);

const
  minFillPercent = 0.1;
  maxFillPercent = 1.0;

// DefaultFillPercent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
const DefaultFillPercent = 0.5;

function page_type(d: Ppage): string;
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

{ TFreelist }

constructor TFreelist.Create;
begin
  allocs  := TQQHash.Create;
  pending := TQHash.Create(false);
  cache   := TQHashSet.Create;
end;

destructor TFreelist.Destroy;
begin
  allocs.Free;
  pending.Free;
  cache.Free;
  inherited Destroy;
end;

{ TTx }

function TTx.Bucket(const name: string; createIfNotExists: boolean): IBucket;
begin

end;

procedure TTx.Commit;
begin

end;

function TTx.Cursor: ICursor;
begin

end;

function TTx.DB: IDB;
begin

end;

function TTx.DbSize: SizeUint;
begin

end;

procedure TTx.DeleteBucket(const name: string);
begin

end;

procedure TTx.Done;
begin

end;

function TTx.ID: txid;
begin

end;

function TTx.Writable: boolean;
begin

end;

{ TDB }

function TDB.BeginTx(readOnly: boolean): ITx;
begin

end;

procedure TDB.Close;
begin
  if not fOpened then
    exit;

  if fFd <> THandle(-1) then
  begin
    FileClose(fFd);
    fFd := THandle(-1);
  end;
  fOpened := false;
end;

procedure TDB.Sync;
begin

end;

function TDB.Writable: boolean;
begin
  result := fWritable;
end;

function meta_checksum(m: Pmeta): uint64;
begin
  result := uint64(UpdateCRC64(m^, sizeof(meta) - sizeof(uint64), -1));
end;

function meta_valid(m: Pmeta): boolean;
begin
  result := meta_checksum(m) = m^.checksum;
end;

procedure TDB.InitDbFile;
var
  buf: array of byte;
  i: integer;
  p: Ppage;
  m: Pmeta;
begin
  SetLength(buf, fPageSize * 4);
  // Create two meta pages on a buffer.
  for i := 0 to 1 do
  begin
    p := Ppage(@buf[fPageSize * i]);
    p^.id := pgid(i);
    p^.flags := metaPageFlag;

    // Initialize the meta page.
    m := Pmeta(@p^.ptr);
    m^.magic := magic;
    m^.version := version;
    m^.pageSize := uint32(fPageSize);
    m^.freelist := 2;
    m^.root.root := 3;
    m^.pgid := 4;
    m^.txid := txid(i);
    m^.checksum := meta_checksum(m);
  end;

  // Write an empty freelist at page 3.
  p := Ppage(@buf[fPageSize * 2]);
  p^.id := pgid(2);
  p^.flags := freelistPageFlag;
  p^.count := 0;

  // Write an empty leaf page at page 4.
  p := Ppage(@buf[fPageSize * 3]);
  p^.id := pgid(3);
  p^.flags := leafPageFlag;
  p^.count := 0;

  FileSeek(fFd, 0, fsFromBeginning);
  FileWrite(fFd, buf[0], fPageSize * 4);
  FileFlush(fFd);
end;

function TDB.AdjustMmapSize(size: int64): int64;
var
  i: cardinal;
  remainder: int64;
begin
  // Double the size from 32KB until 1GB.
  for i := 15 to 30 do
  begin
    if size <= (1 shl i) then
    begin
      result := 1 shl i;
      exit;
    end;
  end;

  // Verify the requested size is not above the maximum allowed.
  if size > maxMapSize then
    Error('mmap too large');

  // If larger than 1GB then grow by 1GB at a time.
  remainder := size mod int64(maxMmapStep);
  if remainder > 0 then
    inc(size, int64(maxMmapStep) - remainder);

  // Ensure that the mmap size is a multiple of the page size.
  // This should always be true since we're incrementing in MBs.
  pageSize := int64(db.pageSize)
  if (size mod fPageSize) <> 0 then
    size := ((size div int64(fPageSize)) + 1) * int64(fPageSize);

  // If we've exceeded the max size then only grow up to the max size.
  if size > maxMapSize then
    size := maxMapSize;

  result := size;
end;

procedure TDB.Mmap(minsz: int64);
var
  fsz: int64;
  size: int64;
begin
  EnterCriticalSection(fMmapLock);
  try
    fsz := FileSeek(fFd, int64(0), fsFromEnd);
    if fsz < 0 then
      Error('can not get file size');
    if fsz < (fPageSize * 4) then
      Error('file size is too small');
    if fsz < minsz then
      fsz := minsz;
    size := AdjustMmapSize(fsz);

    if fRwTx <> nil then
      fRwTx.root.dereference();

    {$ifdef windows}

    {$else}
    {$endif}
  finally
    LeaveCriticalsection(fMmapLock);
  end;
end;

constructor TDB.Create(const aFileName: string; opt: POptions);
var
  mode: integer;
  firstPage: array [0..4095] of byte;
  meta: Pmeta;
begin
  InitializeCriticalSection(fMmapLock);

  fPath := aFileName;
  fOpened := true;
  if opt = nil then
    opt := @DefaultOptions;

  fNoSync := opt^.NoSync;
  fNoGrowSync := opt^.NoGrowSync;
  fMmapFlags := opt^.MmapFlags;
  fNoFreelistSync := opt^.NoFreelistSync;

  // Set default values for later DB operations.
  fMaxBatchSize := DefaultMaxBatchSize;
  fMaxBatchDelay := DefaultMaxBatchDelay;
  fAllocSize := DefaultAllocSize;

  if opt^.ReadOnly then
    mode := fmOpenRead or fmShareExclusive
  else
  begin
    fWritable := true;
    if not FileExists(aFileName) then
      FileClose(FileCreate(aFileName));
    mode := fmOpenReadWrite;
  end;

  // Open data file and separate sync handler for metadata writes.
  fPath := aFileName;

  fFd := FileOpen(aFileName, mode or fmShareExclusive);
  if fFd = THandle(-1) then
    Error('can not open db file');

  fPageSize := opt^.PageSize;
  if fPageSize = 0 then
    fPageSize := 4096;

  if FileSeek(fFd, int64(0), fsFromEnd) = 0 then
    InitDbFile
  else
  begin
    FileSeek(fFd, 0, fsFromBeginning);
    if (FileRead(fFd, firstPage[0], 4096) <> 4096) or not meta_valid(Pmeta(@firstPage[0]))) then
      Error('db file is corrupted');
  end;

(*
  // Initialize page pool.
  db.pagePool = sync.Pool{
    New: func() interface{} {
      return make([]byte, db.pageSize)
    },
  }
*)

(*
  // Memory map the data file.
  if err := db.mmap(options.InitialMmapSize); err != nil {
    _ = db.close()
    return nil, err
  }

  if db.readOnly {
    return db, nil
  }
*)

  db.loadFreelist()

  // Flush freelist when transitioning from no sync to sync so
  // NoFreelistSync unaware boltdb can open the db later.
  if !db.NoFreelistSync && !db.hasSyncedFreelist() {
    tx, err := db.Begin(true)
    if tx != nil {
      err = tx.Commit()
    }
    if err != nil {
      _ = db.close()
      return nil, err
    }
  }

  // Mark the database as opened and return.
  return db, nil
}

end;

destructor TDB.Destroy;
begin
  if fOpened then
    Close;
  DeleteCriticalSection(fMmapLock);
  inherited Destroy;
end;

{ TCursor }

function TCursor.Bucket: IBucket;
begin

end;

procedure TCursor.Delete;
begin

end;

function TCursor.First(out kv: TKV): boolean;
begin

end;

function TCursor.Last(out kv: TKV): boolean;
begin

end;

function TCursor.Next(out kv: TKV): boolean;
begin

end;

function TCursor.Prev(out kv: TKV): boolean;
begin

end;

function TCursor.Seek(const key: TDbValue; out kv: TKV): boolean;
begin

end;

{ TBucket }

function TBucket.Bucket(const name: string): IBucket;
begin

end;

function TBucket.BucketsCursor: ICursor;
begin

end;

function TBucket.Cursor: ICursor;
begin

end;

function TBucket.Get(const key: TDbValue; out value: TDbValue): boolean;
begin

end;

function TBucket.Put(const kv: TKV; overwrite: boolean): boolean;
begin

end;

function OpenBoltDatabase(const name: string; opts: POptions): bolt.IDB;
begin
  result := TDB.Create(name, options);
end;

end.

