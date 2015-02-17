module rikkadb.file.hashtable;

import rikkadb.file.filedata;
import rikkadb.file.util;

import std.stdio;
import std.math;
import std.typecons;
import core.sync.mutex;
import core.sync.rwmutex;


immutable uint HASH_TABLE_GROWTH  = 2<<26;  // Grows every 128MB
immutable ubyte ENTRY_VALID        = 1;
immutable ubyte ENTRY_INVALID      = 0;
immutable uint ENTRY_SIZE         = 21;  // validity + hash key + value
immutable uint BUCKET_HEADER_SIZE = 10;  // next bucket
immutable uint HASH_TABLE_REGION_SIZE = 1024 * 4; // 4KB per locking region


// HashTable class, uint-typed key-value pairs
class HashTable {

  FileData f;
  uint bucketSize;
  uint hashBits;
  uint perBucket;
  Mutex tableGrowMutex;
  ReadWriteMutex[] regionRWMutex;

  this(string name, uint _hashBits, uint _perBucket) {
    if (_hashBits < 2 || _perBucket < 2) {
      throw new InvalidHashTableParameterException("Invalid hash table parameter!");
    }

    f = new FileData(name, HASH_TABLE_GROWTH);
    hashBits = _hashBits;
    perBucket = _perBucket;
    tableGrowMutex = new Mutex;
    regionRWMutex.length = f.size/HASH_TABLE_REGION_SIZE+1;
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
  @property
  uint numberBuckets() {
    return f.append / bucketSize;
  }

  // Return the number of next chained bucket
  uint nextBucket(uint bucket) {
    uint bucketAddr = bucket * bucketSize;
    if (bucketAddr < 0 || bucketAddr >= f.buf.length - BUCKET_HEADER_SIZE) {
      return 0;
    } else {
      uint next = cast(uint) ubytesToUlong(f.buf[bucketAddr .. bucketAddr+BUCKET_HEADER_SIZE]);
      if (next != 0 && next <= bucket) {
        writefln("Loop detected in hashtable %s at bucket %d", f.name, bucket);
        return 0;
      } else if (next >= f.append - BUCKET_HEADER_SIZE) {
        writefln("Bucket reference out of bound in hashtable %s at bucket %d", f.name, bucket);
        return 0;
      } else {
        return next;
      }
    }
  }

  // Return the last bucket number in chain
  uint lastBucket(uint bucket) {
    uint curr = bucket;
    while (true) {
      uint next = nextBucket(curr);
      if (next == 0) {
        return curr;
      }
      curr = next;
    }
  }

  // Grow a new bucket on the chain of buckets
  void grow(uint bucket) {
    tableGrowMutex.lock;
    scope(exit) tableGrowMutex.unlock;

    // when file is full, we have to lock down everything before growing the file
    if (!(f.checkSize(bucketSize))) {
      ReadWriteMutex[] originalMutexes = regionRWMutex;
      foreach (region; originalMutexes) {
        region.writer.lock;
      }
      f.checkSizeAndEnsure(bucketSize);
      // make more mutexes
      ReadWriteMutex[] moreMutexes;
      moreMutexes.length = HASH_TABLE_GROWTH/HASH_TABLE_REGION_SIZE+1;
      for (int i; i < moreMutexes.length; ++i) {
        moreMutexes[i] = new ReadWriteMutex;
      }
      // merge mutexes together
      regionRWMutex ~= moreMutexes;
      foreach (region; originalMutexes) {
        region.writer.unlock;
      }
    }
    uint lastBucketAddr = lastBucket(bucket) * bucketSize;
    putUlongToUbytes(f.buf[lastBucketAddr .. lastBucketAddr+8], cast(ulong) numberBuckets);
    f.append += bucketSize;
  }

  // Return a hash key to be used by hash table by masking non-key bits
  uint hashKey(uint key) {
    return key & ((1 << hashBits) - 1);
  }

  // Put a new key-value pair
  void put(uint key, uint val) {
    uint bucket = hashKey(key);
    uint entry = 0;
    uint region = bucket / HASH_TABLE_REGION_SIZE;
    ReadWriteMutex m = regionRWMutex[region];
    m.writer.lock;

    while (true) {
      uint entryAddr = bucket*bucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE;

      if (f.buf[entryAddr] != ENTRY_VALID) {
        f.buf[entryAddr] = ENTRY_VALID;
        putUlongToUbytes(f.buf[entryAddr+1 .. entryAddr+11], key);
        putUlongToUbytes(f.buf[entryAddr+11 .. entryAddr+21], val);
        m.writer.unlock;
        break;
      }
      entry++;
      if (entry == perBucket) {
        m.writer.unlock;
        entry = 0;
        bucket = nextBucket(bucket);
        if (bucket == 0) {
          grow(hashKey(key));
          put(key, val);
          break;
        }
        region = bucket / HASH_TABLE_REGION_SIZE;
        m = regionRWMutex[region];
        m.writer.lock;
      }
    }
  }

  // Get key-value pairs
  Tuple!(uint[], uint[]) get(uint key, uint limit, bool delegate(uint, uint) filter) {
    uint count;
    uint entry;
    uint bucket = hashKey(key);
    uint[] keys;
    uint[] vals;

    auto region = bucket / HASH_TABLE_REGION_SIZE;
    auto m = regionRWMutex[region];
    m.reader.lock;

    while (true) {
      auto entryAddr = bucket*bucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE;
      uint entryKey = cast(uint)ubytesToUlong(f.buf[entryAddr+1 .. entryAddr+11]);
      uint entryVal = cast(uint)ubytesToUlong(f.buf[entryAddr+11 .. entryAddr+21]);
      if (f.buf[entryAddr] == ENTRY_VALID) {
        if (entryKey == key && filter(entryKey, entryVal)) {
          keys ~= entryKey;
          vals ~= entryVal;
          count++;
          if (count == limit) {
            m.reader.unlock;
            return tuple(keys, vals);
          }
        }
      } else if (entryKey == 0 || entryVal == 0) {
        m.reader.unlock;
        return tuple(keys, vals);
      }
      entry++;
      if (entry == perBucket) {
        m.reader.unlock;
        entry = 0;
        bucket = nextBucket(bucket);
        if (bucket == 0) {
          return tuple(keys, vals);
        }
        region = bucket / HASH_TABLE_REGION_SIZE;
        m = regionRWMutex[region];
        m.reader.lock;
      }
    }
  }

  // remove specific key-calue pair
  void remove(uint key, uint limit, bool delegate(uint, uint) filter) {
    uint count = 0;
    uint entry = 0;
    uint bucket = hashKey(key);

    auto region = bucket / HASH_TABLE_REGION_SIZE;
    auto m = regionRWMutex[region];
    m.writer.lock;

    while (true) {
      auto entryAddr = bucket*bucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE;
      uint entryKey = cast(uint)ubytesToUlong(f.buf[entryAddr+1 .. entryAddr+11]);
      uint entryVal = cast(uint)ubytesToUlong(f.buf[entryAddr+11 .. entryAddr+21]);
      if (f.buf[entryAddr] == ENTRY_VALID) {
        if (entryKey == key && filter(entryKey, entryVal)) {
          f.buf[entryAddr] = ENTRY_INVALID;
          count++;
          if (count == limit) {
            m.writer.unlock;
            return;
          }
        }
      } else if (entryKey == 0 || entryVal == 0) {
        m.writer.unlock;
        return;
      }
      entry++;
      if (entry == perBucket) {
        m.writer.unlock;
        entry = 0;
        bucket = nextBucket(bucket);
        if (bucket == 0) {
          return;
        }
        region = bucket / HASH_TABLE_REGION_SIZE;
        m = regionRWMutex[region];
        m.writer.lock;
      }
    }
  }

  Tuple!(uint[], uint[]) getAll(uint limit) {
    uint[] keys;
    uint[] vals;
    uint counter;

    for (uint head=0; head < cast(uint) pow(2, cast(double)hashBits) ;++head) {
      uint entry = 0;
      uint bucket = head;
      uint region = bucket / HASH_TABLE_REGION_SIZE;
      auto m = regionRWMutex[region];
      m.reader.lock;

      while (true) {
        auto entryAddr = bucket*bucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE;
        uint entryKey = cast(uint)ubytesToUlong(f.buf[entryAddr+1 .. entryAddr+11]);
        uint entryVal = cast(uint)ubytesToUlong(f.buf[entryAddr+11 .. entryAddr+21]);
        if (f.buf[entryAddr] == ENTRY_VALID) {
          counter++;
          keys ~= entryKey;
          vals ~= entryVal;
          if (counter == limit) {
            m.reader.unlock;
            break;
          }
        } else if (entryKey == 0 || entryVal == 0) {
          m.reader.unlock;
          break;
        }
        entry++;
        if (entry == perBucket) {
          m.reader.unlock;
          entry = 0;
          bucket = nextBucket(bucket);
          if (bucket == 0) {
            break;
          }
          region = bucket / HASH_TABLE_REGION_SIZE;
          m = regionRWMutex[region];
          m.reader.lock;
        }
      }
    }
    return tuple(keys, vals);
  }

}


// class for raise exception when invalid parameter given
class InvalidHashTableParameterException : Exception {
  pure this(string msg){
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

    for (uint i=0; i < 30; ++i) {
      ht.put(i, i);
    }
    for (uint i=0; i < 30; ++i) {
      auto t = ht.get(i, 0, (uint a, uint b){ return true; });
      uint[] keys = t[0];
      uint[] vals = t[1];
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

    ht.put(1, 1);
    ht.put(1, 2);
    ht.put(1, 3);
    ht.put(2, 1);
    ht.put(2, 2);
    ht.put(2, 3);

    auto t = ht.get(1, 0, (uint a, uint b) { return true; });
    auto keys = t[0];
    auto vals = t[1];
    assert(keys.length == 3);
    assert(vals.length == 3);

    auto t2 = ht.get(2, 2, (uint a, uint b) { return true; });
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

    ht.put(1, 1);
    ht.put(1, 2);
    ht.put(1, 3);
    ht.put(2, 1);
    ht.put(2, 2);
    ht.put(2, 3);

    ht.remove(1, 1, (uint a, uint b) { return true; });
    ht.remove(2, 2, (uint a, uint b) { return b >= 2; });
    auto t = ht.get(1, 0, (uint a, uint b) { return true; });
    auto keys = t[0];
    auto vals = t[1];

    assert(keys.length == 2);
    assert(vals.length == 2);

    auto t2 = ht.get(2, 0, (uint a, uint b) { return true; });
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

    ht.put(1, 1);
    ht.put(1, 2);
    ht.put(1, 3);
    ht.put(2, 1);
    ht.put(2, 2);
    ht.put(2, 3);

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