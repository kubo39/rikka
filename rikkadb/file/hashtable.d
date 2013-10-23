module rikkadb.file.hashtable;

import rikkadb.file.filedata;
import rikkadb.file.util;

import std.stdio;
import std.math;
import std.typecons;
import core.sync.mutex;
import core.sync.rwmutex;


immutable ulong HASH_TABLE_GROWTH  = 2<<26;  // Grows every 128MB
immutable ubyte ENTRY_VALID        = 1;
immutable ubyte ENTRY_INVALID      = 0;
immutable ulong ENTRY_SIZE         = 21;  // validity + hash key + value
immutable ulong BUCKET_HEADER_SIZE = 10;  // next bucket
immutable uint HASH_TABLE_REGION_SIZE = 1024 * 4; // 4KB per locking region


// HashTable class, ulong-typed key-value pairs
class HashTable {

  FileData f;
  ulong bucketSize;
  ulong hashBits;
  ulong perBucket;
  Mutex tableGrowMutex;
  ReadWriteMutex[] regionRWMutex;

  this(string name, ulong _hashBits, ulong _perBucket) {
    if (_hashBits < 1 || _perBucket < 1) {
      throw new InvalidHashTableParameterException("Invalid hash table parameter!");
    }

    f = new FileData(name, HASH_TABLE_GROWTH);
    hashBits = _hashBits;
    perBucket = _perBucket;
    tableGrowMutex = new Mutex;
    regionRWMutex.length = cast(uint) (f.size/HASH_TABLE_REGION_SIZE+1);
    for (int i; i < regionRWMutex.length; ++i) {
      regionRWMutex[i] = new ReadWriteMutex;
    }
    bucketSize = BUCKET_HEADER_SIZE + ENTRY_SIZE*perBucket;

    // file must be big enough to contain all initial buckets
    uint minAppend = cast(uint) (pow(2, cast(double) hashBits) * bucketSize);
    if (f.append < minAppend) {
      f.checkSizeAndEnsure(minAppend - f.append);
      f.append = minAppend;
    }

    // move append position to end of final bucket
    auto extra = f.append % bucketSize;
    if (extra != 0) {
      f.append += bucketSize - extra;
    }
  }

  // Return total number of buckets
  ulong numberBuckets() {
    return f.append / bucketSize;
  }

  // Return the number of next chained bucket
  ulong nextBucket(ulong bucket) {
    uint bucketAddr = cast(uint)(bucket * bucketSize);
    if (bucketAddr < 0 || bucketAddr >= cast(ulong) f.buf.length) {
      return 0;
    } else {
      ulong next = ubytesToUlong(f.buf[bucketAddr .. bucketAddr+cast(uint)BUCKET_HEADER_SIZE]);
      if (next != 0 && next <= bucket) {
	writeln("Loop detected in hashtable %s at bucket %d", f.name, bucket);
	return 0;
      } else if (next >= f.append) {
	writeln("Bucket reference out of bound in hashtable %s at bucket %d", f.name, bucket);
	return 0;
      } else {
	return next;
      }
    }
  }

  // Return the last bucket number in chain
  ulong lastBucket(ulong bucket) {
    ulong curr = bucket;
    while (true) {
      ulong next = nextBucket(curr);
      if (next == 0) {
	return curr;
      }
      curr = next;
    }
  }

  // Grow a new bucket on the chain of buckets
  void grow(ulong bucket) {
    tableGrowMutex.lock;
    scope(exit) tableGrowMutex.unlock;

    // when file is full, we have to lock down everything before growing the file
    if (!(f.checkSize(bucketSize))) {
      ReadWriteMutex[] originalMutexes = regionRWMutex;
      foreach (region; originalMutexes) {
	region.reader.lock;
      }
      f.checkSizeAndEnsure(bucketSize);
      // make more mutexes
      ReadWriteMutex[] moreMutexes;
      moreMutexes.length = cast(int) (HASH_TABLE_GROWTH/HASH_TABLE_REGION_SIZE+1);
      for (int i; i < moreMutexes.length; ++i) {
	moreMutexes[i] = new ReadWriteMutex;
      }
      // merge mutexes together
      regionRWMutex ~= moreMutexes;
      foreach (region; originalMutexes) {
	region.reader.unlock;
      }
    }
    uint lastBucketAddr = cast(uint) (lastBucket(bucket) * bucketSize);
    putUlongToUbytes(f.buf[lastBucketAddr .. lastBucketAddr+8], numberBuckets());
    f.append += bucketSize;
  }

  // Return a hash key to be used by hash table by masking non-key bits
  ulong hashKey(ulong key) {
    return key & ((1 << hashBits) - 1);
  }

  // Put a new key-value pair
  void put(ulong key, ulong val) {
    ulong bucket = hashKey(key);
    ulong entry = 0L;
    uint region = cast(uint) (bucket / HASH_TABLE_REGION_SIZE);
    ReadWriteMutex m = regionRWMutex[region];
    m.reader.lock;
    scope(exit) m.reader.unlock;

    while (true) {
      uint entryAddr = cast(uint)(bucket*bucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE);
      // writeln(entryAddr);

      if (f.buf[entryAddr] != ENTRY_VALID) {
	f.buf[entryAddr] = ENTRY_VALID;
	debug { writeln(f.buf[entryAddr]); }
	putUlongToUbytes(f.buf[entryAddr+1 .. entryAddr+11], key);
	debug { writeln(f.buf[entryAddr+1 .. entryAddr+11]); }
	putUlongToUbytes(f.buf[entryAddr+11 .. entryAddr+21], val);
	debug { writeln(f.buf[entryAddr+11 .. entryAddr+21]); }
	break;
      }
      entry++;
      if (entry == perBucket) {
	entry = 0;
	bucket = nextBucket(bucket);
	if (bucket == 0) {
	  grow(hashKey(key));
	  put(key, val);
	  break;
	}
	region = cast(uint) (bucket / HASH_TABLE_REGION_SIZE);	
	m = regionRWMutex[region];
      }
    }
  }

  // Get key-value pairs
  Tuple!(ulong[], ulong[]) get(ulong key, ulong limit, bool delegate(ulong, ulong) filter) {
    ulong count;
    ulong entry;
    ulong bucket = hashKey(key);
    ulong[] keys;
    ulong[] vals;

    auto region = cast(uint) (bucket / HASH_TABLE_REGION_SIZE);
    auto m = regionRWMutex[region];
    m.reader.lock;
    scope(exit) m.reader.unlock;

    while (true) {
      auto entryAddr = cast(uint)(bucket*bucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE);
      ulong entryKey = ubytesToUlong(f.buf[entryAddr+1 .. entryAddr+11]);
      ulong entryVal = ubytesToUlong(f.buf[entryAddr+11 .. entryAddr+21]);
      if (f.buf[entryAddr] == ENTRY_VALID) {
	if (entryKey == key && filter(entryKey, entryVal)) {
	  keys ~= entryKey;
	  vals ~= entryVal;
	  count++;
	  if (count == limit) {
	    return tuple(keys, vals);
	  }
	}
      } else if (entryKey == 0 || entryVal == 0) {
	return tuple(keys, vals);
      }
      entry++;
      if (entry == perBucket) {
	entry = 0;
	bucket = nextBucket(bucket);
	if (bucket == 0) {
	  return tuple(keys, vals);
	}
	region = cast(uint) (bucket / HASH_TABLE_REGION_SIZE);
	m = regionRWMutex[region];
      }
    }
  }

  // remove specific key-calue pair
  void remove(ulong key, ulong limit, bool delegate(ulong, ulong) filter) {
    ulong count = 0L;
    ulong entry = 0L;
    ulong bucket = hashKey(key);

    auto region = cast(uint) (bucket / HASH_TABLE_REGION_SIZE);
    auto m = regionRWMutex[region];
    m.reader.lock;
    scope(exit) m.reader.unlock;

    while (true) {
      auto entryAddr = cast(uint)(bucket*bucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE);
      ulong entryKey = ubytesToUlong(f.buf[entryAddr+1 .. entryAddr+11]);
      ulong entryVal = ubytesToUlong(f.buf[entryAddr+11 .. entryAddr+21]);
      if (f.buf[entryAddr] == ENTRY_VALID) {
	if (entryKey == key && filter(entryKey, entryVal)) {
	  f.buf[entryAddr] = ENTRY_INVALID;
	  count++;
	  if (count == limit) {
	    return;
	  }
	}
      } else if (entryKey == 0 || entryVal == 0) {
	return;
      }
      entry++;
      if (entry == perBucket) {
	entry = 0;
	bucket = nextBucket(bucket);
	if (bucket == 0) {
	  return;
	}
	region = cast(uint) (bucket / HASH_TABLE_REGION_SIZE);
	m = regionRWMutex[region];
      }
    }
  }

  Tuple!(ulong[], ulong[]) getAll(ulong limit) {
    ulong[] keys;
    ulong[] vals;
    ulong counter;

    for (ulong head=0L; head < cast(ulong) pow(2, cast(double)hashBits) ;++head) {
      ulong entry = 0L;
      ulong bucket = head;
      uint region = cast(uint) (bucket / HASH_TABLE_REGION_SIZE);
      auto m = regionRWMutex[region];
      m.reader.lock;
      scope(exit) m.reader.unlock;

      while (true) {
	auto entryAddr = cast(uint)(bucket*bucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE);
	ulong entryKey = ubytesToUlong(f.buf[entryAddr+1 .. entryAddr+11]);
	ulong entryVal = ubytesToUlong(f.buf[entryAddr+11 .. entryAddr+21]);
	if (f.buf[entryAddr] == ENTRY_VALID) {
	  counter++;
	  keys ~= entryKey;
	  vals ~= entryVal;
	  if (counter == limit) {
	    break;
	  }
	} else if (entryKey == 0 || entryVal == 0) {
	  break;
	}
	entry++;
	if (entry == perBucket) {
	  entry = 0;
	  bucket = nextBucket(bucket);
	  if (bucket == 0) {
	    break;
	  }
	  region = cast(uint) (bucket / HASH_TABLE_REGION_SIZE);
	  m = regionRWMutex[region];
	}
      }
    }
    return tuple(keys, vals);
  }

}


// class for raise exception when invalid parameter given
class InvalidHashTableParameterException : Exception {
  this(string msg){
    super(msg);
  }
}


unittest {
  import std.file;

  void testPutGet() {
    auto tmp = "/tmp/rikka_hash_test";

    if (exists(tmp)) {
      remove(tmp);
    }
    scope(exit) {
      if (exists(tmp)) {
	remove(tmp);
      }
    }
  
    auto ht = new HashTable(tmp, 2, 2);
    scope(exit) ht.f.close();

    for (ulong i=0L; i < 30L; ++i) {
      ht.put(i, i);
    }
    for (ulong i=0L; i < 30L; ++i) {
      auto t = ht.get(i, 0L, (ulong a, ulong b){ return true; });
      ulong[] keys = t[0];
      ulong[] vals = t[1];
      assert(keys.length == 1);
      assert(keys[0] == i);
      assert(vals.length == 1);
      assert(vals[0] == i);
    }
  }

  void testPutGet2() {
    auto tmp = "/tmp/rikka_hash_test";

    if (exists(tmp)) {
      remove(tmp);
    }
    scope(exit) {
      if (exists(tmp)) {
	remove(tmp);
      }
    }

    auto ht = new HashTable(tmp, 2, 2);
    scope(exit) ht.f.close();

    ht.put(1L, 1L);
    ht.put(1L, 2L);
    ht.put(1L, 3L);
    ht.put(2L, 1L);
    ht.put(2L, 2L);
    ht.put(2L, 3L);

    auto t = ht.get(1L, 0L, (ulong a, ulong b) { return true; });
    auto keys = t[0];
    auto vals = t[1];
    assert(keys.length == 3);
    assert(vals.length == 3);

    auto t2 = ht.get(2L, 2L, (ulong a, ulong b) { return true; });
    auto keys2 = t2[0];
    auto vals2 = t2[1];
    assert(keys2.length == 2);
    assert(vals2.length == 2);
  }

  void testPutRemove() {
    auto tmp = "/tmp/rikka_hash_test";

    if (exists(tmp)) {
      remove(tmp);
    }
    scope(exit) {
      if (exists(tmp)) {
	remove(tmp);
      }
    }

    auto ht = new HashTable(tmp, 2, 2);
    scope(exit) ht.f.close();

    ht.put(1L, 1L);
    ht.put(1L, 2L);
    ht.put(1L, 3L);
    ht.put(2L, 1L);
    ht.put(2L, 2L);
    ht.put(2L, 3L);

    ht.remove(1L, 1L, (ulong a, ulong b) { return true; });
    ht.remove(2L, 2L, (ulong a, ulong b) { return b >= 2; });
    auto t = ht.get(1L, 0L, (ulong a, ulong b) { return true; });
    auto keys = t[0];
    auto vals = t[1];

    assert(keys.length == 2);
    assert(vals.length == 2);

    auto t2 = ht.get(2L, 0L, (ulong a, ulong b) { return true; });
    auto keys2 = t2[0];
    auto vals2 = t2[1];

    assert(keys2.length == 1);
    assert(vals2.length == 1);
  }

  void testGetAll() {
    auto tmp = "/tmp/rikka_hash_test";

    if (exists(tmp)) {
      remove(tmp);
    }
    scope(exit) {
      if (exists(tmp)) {
	remove(tmp);
      }
    }

    auto ht = new HashTable(tmp, 2, 2);
    scope(exit) ht.f.close();

    ht.put(1L, 1L);
    ht.put(1L, 2L);
    ht.put(1L, 3L);
    ht.put(2L, 1L);
    ht.put(2L, 2L);
    ht.put(2L, 3L);

    auto t = ht.getAll(0);
    auto keys = t[0];
    auto vals = t[1];
    assert(keys.length == 6);
    assert(vals.length == 6);
  }

  testPutGet();
  testPutGet2();
  testPutRemove();
  testGetAll();
}


//version(unittest) void main() {}