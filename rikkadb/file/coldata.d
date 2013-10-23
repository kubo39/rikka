module rikkadb.file.coldata;

import rikkadb.file.filedata;
import rikkadb.file.util;

import std.stdio;
import core.sync.mutex;
import core.sync.rwmutex;


immutable ulong COL_FILE_GROWTH    = 134217728;  // Grows every 128MB
immutable uint DOC_MAX_ROOM         = 33554432;  // Maximum single document size
int DOC_HEADER                     = 1 + 10;    // byte(validity), ulong(document room)
immutable byte DOC_VALID           = 1;
immutable byte DOC_INVALID         = 0;
immutable uint COL_FILE_REGION_SIZE = 1024 * 512; // 512 KB per locking region
immutable ubyte[2048] PADDING;
immutable uint LEN_PADDING = PADDING.length;


class ColData {

  FileData f;
  Mutex docInsertMutex;
  ReadWriteMutex[] regionRWMutex;

  this(string name) {
    f = new FileData(name, COL_FILE_GROWTH);
    docInsertMutex = new Mutex;
    regionRWMutex.length = cast(int)f.size / COL_FILE_REGION_SIZE;
    for (int i; i < regionRWMutex.length; ++i) {
      regionRWMutex[i] = new ReadWriteMutex;
    }
  }

  ubyte[] read(ulong id) {
    if (id < 0 || id >= f.append) {
      return null;
    }
    auto region = cast(uint) id / COL_FILE_REGION_SIZE;
    auto m = regionRWMutex[region];
    m.reader.lock;
    scope(exit) m.reader.unlock;

    if (f.buf[cast(uint)id] != DOC_VALID) { 
      return null;
    }

    ulong room = ubytesToUlong(f.buf[cast(uint)id+1 .. cast(uint)id+11]);
    if (room > DOC_MAX_ROOM) {
      return null;
    } else {
      return cast(ubyte[])(f.buf[cast(int)(id)+DOC_HEADER .. cast(int)(id+room)+DOC_HEADER].idup);
    }
  }

  // insert document
  ulong insert(ubyte[] data) {
    ulong len = cast(ulong) data.length;
    ulong room = len + len;
    if (room >= DOC_MAX_ROOM) {
      return 0;
      //raise
    }
    docInsertMutex.lock;
    scope(exit) docInsertMutex.unlock;

    auto id = f.append;
    // when file is full, we have lots to do
    if (!(f.checkSize(DOC_HEADER + room))) {
      auto originalMutexes = regionRWMutex;
      foreach (region; originalMutexes) {
	region.reader.lock;
      }
      f.checkSizeAndEnsure(DOC_HEADER + room);
      // make more mutexes
      ReadWriteMutex[] moreMutexes;
      moreMutexes.length = COL_FILE_GROWTH/COL_FILE_REGION_SIZE+1;
      for (int i; i < moreMutexes.length; ++i) {
	moreMutexes[i] = new ReadWriteMutex;
      }
      // merge mutexes together
      regionRWMutex ~= moreMutexes;
      foreach (region; originalMutexes) {
	region.reader.unlock;
      }
    }
    // reposition next append
    f.append = id + DOC_HEADER + room;
    // make doc header and copy data
    f.buf[cast(uint)id] = 1;
    putUlongToUbytes(f.buf[cast(uint)id+1 .. cast(uint)(id+DOC_HEADER)], room);
    auto paddingBegin = cast(uint)(id + DOC_HEADER + len);
    auto paddingEnd = cast(uint)(id + DOC_HEADER + room);
    f.buf[cast(uint)(id+DOC_HEADER) .. paddingBegin] = data.idup;

    // make padding
    for (uint segBegin = paddingBegin; segBegin < paddingEnd; segBegin += LEN_PADDING) {
      uint segSize = LEN_PADDING;
      uint segEnd = segBegin + LEN_PADDING;

      if (segEnd >= paddingEnd) {
	segEnd = paddingEnd;
	segSize = paddingEnd - segBegin;
      }
      f.buf[segBegin .. segEnd] = PADDING[0 .. segSize];
    }
    return id;
  }

  // update document
  ulong update(ulong id, ubyte[] data) {
    ulong len = cast(ulong) data.length;
    uint region = cast(uint) (id / COL_FILE_REGION_SIZE);

    auto m = regionRWMutex[region];
    m.reader.lock;
    scope(exit) m.reader.unlock;

    if (f.buf[cast(uint)id] != DOC_VALID) {
      // raise
    }

    auto room = ubytesToUlong(f.buf[cast(uint)id+1 .. cast(uint)id+11]);
    if (room > DOC_MAX_ROOM) {
      // raise
      return id;
    } else {
      if (len <= room) {
	// overwrite data
	uint paddingBegin = cast(uint)(id + DOC_HEADER + len);
	f.buf[cast(uint)(id+DOC_HEADER) .. paddingBegin] = data.idup;
	uint paddingEnd = cast(uint)(id + DOC_HEADER + room);

	// overwrite padding
	for (uint segBegin = paddingBegin; segBegin < paddingEnd; segBegin += LEN_PADDING) {
	  uint segSize = LEN_PADDING;
	  uint segEnd = segBegin + LEN_PADDING;

	  if (segEnd >= paddingEnd) {
	    segEnd = paddingEnd;
	    segSize = paddingEnd - segBegin;
	  }
	  f.buf[segBegin .. segEnd] = PADDING[0 .. segSize];
	}
      }
      // re-insert because there is not enough room
      del(id);
      return insert(data);
    }
  }

  // delete document
  void del(ulong id) {
    if (id < 0) {
      return;
    }
    uint region = cast(uint) (id / COL_FILE_REGION_SIZE);
    auto m = regionRWMutex[region];
    m.reader.lock;
    scope(exit) m.reader.unlock;

    if (f.buf[cast(uint)id] == DOC_VALID) {
      f.buf[cast(uint)id] = DOC_INVALID;
    }
  }

  // Apply function for all documents in the collection
  void forAll(bool delegate(ulong id, ubyte[] doc) func) {
    ulong addr = 0L;
    while (true) {
      if (addr >= f.append) {
	break;
      }
      uint region = cast(uint) addr / COL_FILE_REGION_SIZE;
      auto m = regionRWMutex[region];
      m.reader.lock;
      scope(exit) m.reader.lock;

      auto validity = f.buf[cast(uint)addr];
      ulong room = ubytesToUlong(f.buf[cast(uint)addr+1 .. cast(uint)addr+11]);

      if (validity != DOC_VALID && validity != DOC_INVALID || room > DOC_MAX_ROOM) {
	addr++;
	for (; f.buf[cast(uint)addr] != DOC_VALID && f.buf[cast(uint)addr] != DOC_INVALID; addr++) {
	}
	continue;
      }

      if (validity == DOC_VALID && !func(addr, f.buf[cast(uint)addr+DOC_HEADER .. cast(uint)(addr+DOC_HEADER+room)])) {
	break;
      }
      addr += cast(ulong)(DOC_HEADER + room);
    }
  }

}


unittest {
  import std.file;

  auto tmp = "/tmp/rikka_col_test";

  void testInsertRead() {
    if (exists(tmp)) {
      remove(tmp);
    }
    scope(exit) {
      if (exists(tmp)) {
	remove(tmp);
      }
    }

    auto col = new ColData(tmp);
    scope(exit) col.f.close();

    auto docs = [cast(ubyte[])"abc", cast(ubyte[])"1234"];

    ulong[2] ids;
    ids[0] = col.insert(docs[0]);
    ids[1] = col.insert(docs[1]);

    assert(col.read(ids[0])[0 .. 3] == docs[0]);  // looks ugly.....
    assert(col.read(ids[1])[0 .. 4] == docs[1]);  // looks ugly, too
  }

  void testInsertUpdateRead() {
    if (exists(tmp)) {
      remove(tmp);
    }
    scope(exit) {
      if (exists(tmp)) {
	remove(tmp);
      }
    }

    auto col = new ColData(tmp);
    scope(exit) col.f.close();

    auto docs = [cast(ubyte[])"abc", cast(ubyte[])"1234"];

    ulong[2] ids;
    ids[0] = col.insert(docs[0]);
    ids[1] = col.insert(docs[1]);

    ulong[2] updated;
    updated[0] = col.update(ids[0], cast(ubyte[])"abcdef");
    updated[1] = col.update(ids[1], cast(ubyte[])"longlonglonglonglong");

    assert(updated[0] != ids[0]);
    assert(updated[1] != ids[1]);

    auto doc0 = col.read(updated[0]);
    auto doc1 = col.read(updated[1]);

    assert(doc0[0 .. 6] == "abcdef");                  // terrble!!
    assert(doc1[0 .. 20] == "longlonglonglonglong");   // what's a teribble!!
  }

  void testInsertDeleteRead() {
    if (exists(tmp)) {
      remove(tmp);
    }
    scope(exit) {
      if (exists(tmp)) {
	remove(tmp);
      }
    }

    auto col = new ColData(tmp);
    scope(exit) col.f.close();

    auto docs = [cast(ubyte[])"abc", cast(ubyte[])"1234", cast(ubyte[])"2345"];

    ulong[3] ids;
    ids[0] = col.insert(docs[0]);
    ids[1] = col.insert(docs[1]);
    ids[2] = col.insert(docs[2]);

    col.del(ids[0]);

    auto doc0 = col.read(ids[0]);
    auto doc1 = col.read(ids[1]);
    assert(doc0 == null);
    assert(doc1[0 .. 4] == cast(ubyte[])"1234");
  }

  testInsertRead();
  testInsertUpdateRead();
  testInsertDeleteRead();
}


//version(unittest) void main() {};