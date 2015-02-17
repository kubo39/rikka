module rikkadb.file.filedata;

import std.conv;
import std.mmfile;
import std.string;
import core.sync.rwmutex;
import core.sys.posix.unistd;
import core.sys.posix.fcntl;


immutable FILE_GROWTH_INCREMENTAL = 2<<23;  // 16MB


class FileData {

  string name;
  uint f;
  uint append;
  uint size;
  uint growth;
  MmFile mmap;
  ubyte[] buf;

  this(string _name, uint _growth) {
    name = _name;
    growth = _growth;
    f = open(name.toStringz, O_CREAT|O_RDWR, octal!"600");
    size = cast(uint) lseek(f, 0, 2);

    if (size == 0) {
      checkSizeAndEnsure(_growth);
      return;
    }

    mmap = new MmFile(name, MmFile.Mode.readWrite, size, null, 0);
    buf = cast(ubyte[]) mmap[0 .. uint.max];

    // find append position
    for (uint pos = size -1; pos > 0; pos--) {
      if (buf[pos] != 0) {
        append = pos + 1;
        break;
      }
    }
  }

  // Ensure the file has room for more data
  bool checkSize(uint more) {
    return append+more <= size;
  }

  // Ensure the file ahs room for more data
  void checkSizeAndEnsure(uint more) {
    if (append+more <= size) {
      return;
    }

    size = cast(uint) lseek(f, 0, 2);

    // grow the file incrementally
    ubyte[] zeroBuf;
    zeroBuf.length = FILE_GROWTH_INCREMENTAL;
    for (uint i = 0; i < growth; i+= FILE_GROWTH_INCREMENTAL) {
      ubyte[] slice;
      if (i > growth) {
        slice = zeroBuf[0 .. growth];
      } else {
        slice = zeroBuf;
      }
      core.sys.posix.unistd.write(f, cast(const void*)slice, growth);
    }
    fsync(f);

    mmap = new MmFile(name, MmFile.Mode.readWrite, size, null, 0);
    buf = cast(ubyte[]) mmap[0 .. uint.max];

    size += growth;
    checkSizeAndEnsure(more);
  }

  // Synchronize mapped region with underlying storage device
  void flush() {
    mmap.flush();
  }

  // close this file
  void close() {
    mmap.flush();
    delete mmap; 
    f.close();
  }
}


unittest {
  import std.file;
  auto tmp = "/tmp/rikka_file_test";

  if (exists(tmp)) {
    remove(tmp);
  }
  scope(exit) {
    if (exists(tmp)) {
      remove(tmp);
    }
  }

  auto tmpFile = new FileData(tmp, 1000);

  assert(tmpFile.growth == 1000);
  assert(tmpFile.append == 0);

  ubyte[] arr = cast(ubyte[])"1234567890";
  ubyte[] buf = cast(ubyte[])tmpFile.buf[0 .. 10];
  buf[] = arr[];

  assert(tmpFile.buf[1] == cast(ubyte)'2');
  tmpFile.close();
  delete tmpFile;

  auto tmpFile2 = new FileData(tmp, 1000);
  assert(tmpFile2.append == 10);
  tmpFile2.buf[10] = cast(ubyte)'b';
  tmpFile2.close();
  delete tmpFile2;

  auto tmpFile3 = new FileData(tmp, 1000);
  assert(tmpFile3.append == 11);
  assert(tmpFile3.buf[0 .. 11] == cast(ubyte[])"1234567890b");
  tmpFile3.close();
  delete tmpFile3;
}