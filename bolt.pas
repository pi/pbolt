unit bolt;

interface

{$ifndef fpc}uses pl;{$endif}

type
  IDB = interface;
  ITx = interface;
  IBucket = interface;
  ICursor = interface;

  TDbValue = record
    Len: SizeUint;
    Ptr: pchar;
  end;

  TKV = record
    Key: TDbValue;
    Value: TDbValue;
  end;

  TtxStats = record
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
	  RebalanceTimeNanos : uint64; // total time spent rebalancing

	  // Split/Spill statistics.
	  Split     : integer;           // number of nodes split
	  Spill     : integer;           // number of nodes spilled
	  SpillTimeNanos : uint64; // total time spent spilling

	  // Write statistics.
	  Write     : integer;           // number of writes performed
	  WriteTimeNanos : uint64; // total time spent writing to disk
  end;

  IDB = interface
    ['{A4FC9239-6BFE-452A-B682-273397660433}']
    
    procedure Close;

    function Writable: boolean;
    
    function BeginTx(readOnly: boolean): ITx;

    procedure Sync;
  end;
  
  ITx = interface
    ['{57D22D15-3C33-4E92-9504-5DF7063442B2}']

    function DB: IDB;
    function DbSize: SizeUint;

    function Writable: boolean;

    function Bucket(const name: string; createIfNotExists: boolean): IBucket;
    procedure DeleteBucket(const name: string);

    function Cursor: ICursor; // return cursor for enumerating names of DB root buckets

    procedure Commit;
    procedure Done; // rollback if tx is active
  end;
  
  IBucket = interface
    ['{16CD7F97-AF1A-48D5-ACB8-58AFF0D549BC}']

    function Bucket(const name: string): IBucket; // get existing or create new if tx is rw

    function Cursor: ICursor; // return cursor for enumerating keys with values. subbuckets are omitted
    function BucketsCursor: ICursor; // return cursor for enumerating names of subbuckets. regular keys are omitted
    
    function Put(const kv: TKV; overwrite: boolean): boolean;
    function Get(const key: TDbValue; out value: TDbValue): boolean;
  end;

  ICursor = interface
    ['{176748A9-5566-45A0-8ECA-B17CC0C57EC1}']

    function First(out kv: TKV): boolean;
    function Last(out kv: TKV): boolean;
    function Next(out kv: TKV): boolean;
    function Prev(out kv: TKV): boolean;
    function Seek(const key: TDbValue; out kv: TKV): boolean; // key can be partial
    procedure Delete;
    function Bucket: IBucket;
  end;
  
implementation

end.

