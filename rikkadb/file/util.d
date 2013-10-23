module rikkadb.file.util;


// encodes a ulong into buf
void putUlongToUbytes(ref ubyte[] buf, ulong x) {
  int i;
  while (x >= 0x80) {
    buf[i] = cast(ubyte) x | 0x80;
    x >>= 7;
    i++;
  }
  buf[i] = cast(ubyte) x;
}


// decodes a ulong from buf
ulong ubytesToUlong(ubyte[] buf) {
  ulong x;
  uint s;

  foreach (i, b; buf) {
    if (b < 0x80) {
      if (i > 9 || i == 9 && b > 1) {
	throw new BufferOverflow("value larger than 64 bits");
      }
      return x | (cast(ulong) b) << s;
    }
    x |= (cast(ulong) b&0x7f) << s;
    s += 7;
  }
  throw new BufferTooSmall("buf is too small");
}


class BufferOverflow : Exception {
  this(string name) {
    super(name);
  }
}


class BufferTooSmall : Exception {
  this(string name) {
    super(name);
  }
}


unittest {
  void f(ulong x) {
    ubyte[] buf;
    buf.length = 64;
    putUlongToUbytes(buf, x);
    ulong y = ubytesToUlong(buf);
    assert(x == y);
  }
  f(1L);
  f(2L);
  f(64L);
  f(63L);
  f(65L);
  f(127L);
  f(128L);
  f(129L);
  f(255L);
  f(256L);
  f(257L);
  f(1L<<63-1);
}


// version(unittest) void main() {}