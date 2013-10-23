module rikkadb.rikkadb;

import rikkadb.collection;

import std.conv;
import std.file;
import std.stdio;
import std.string;


class RikkaDB {

  string dir;
  Collection[string] strCol;

  this(string _dir) {
    mkdirRecurse(_dir);
    dir = _dir;

    foreach (string f; dirEntries(dir, SpanMode.breadth)) {
      if (isDir(f)) {
	strCol[f] = new Collection([dir, f].join("/"));
      }
      // Successfully opened collection
    }
  }

  // Create a new collection
  void create(string name) {
    if (name in strCol) {
      writeln("already exists");
      return;
    }
    strCol[name] = new Collection([dir, name].join("/"));
  }

  // return collection
  Collection use(string name) {
    if (name in strCol) {
      return strCol[name];
    }
    return null;
  }

  // drop a collection
  void drop(string name) {
    if (name in strCol) {
      strCol[name].close();
      strCol.remove(name);
      rmdirRecurse([dir, name].join("/"));
    } else {
      writeln("There's no collection such name.");
    }
  }

  // Flush all collection data files
  void flush() {
    foreach(col; strCol) {
      col.flush();
    }
  }

  // close all collections
  void close() {
    foreach (col; strCol) {
      col.close();
    }
  }
  
}


unittest {
  string tmp = "/tmp/rikka_db_test";

  void testCUD() {
    if (exists(tmp) && isDir(tmp)) {
      rmdirRecurse(tmp);
    }
    scope(exit) {
      if (exists(tmp) && isDir(tmp)) {
	rmdirRecurse(tmp);
      }
    }

    auto db = new RikkaDB(tmp);
    scope(exit) db.close();

    // create
    db.create("a");
    db.create("b");

    // use
    db.use("a");
    db.use("b");

    // drop
    db.drop("a");

    assert(db.strCol.keys == ["b"]);

    db.flush();
  }

  testCUD();
}


//version(unittest) void main() {}