module rikkadb.rikkadb;

import rikkadb.collection;

import std.conv;
import std.file;
import std.path;
import std.stdio;
import std.string;


class RikkaDB {

  string dir;
  Collection[string] collections;

  this(string _dir) {
    mkdirRecurse(_dir);
    dir = _dir;

    foreach (string f; dirEntries(dir, SpanMode.breadth)) {
      if (isDir(f)) {
	collections[f] = new Collection(buildPath(dir, f));
      }
      // Successfully opened collection
    }
  }

  // Create a new collection
  void create(string name) {
    if (name in collections) {
      writeln("already exists");
      return;
    }
    collections[name] = new Collection(buildPath(dir, name));
  }

  // return collection
  Collection use(string name) {
    if (name in collections) {
      return collections[name];
    }
    return null;
  }

  // drop a collection
  void drop(string name) {
    if (name in collections) {
      collections[name].close();
      collections.remove(name);
      rmdirRecurse(buildPath(dir, name));
    } else {
      writeln("There's no collection such name.");
    }
  }

  // Flush all collection data files
  void flush() {
    foreach(col; collections) {
      col.flush();
    }
  }

  // close all collections
  void close() {
    foreach (col; collections) {
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

    assert(db.collections.keys == ["b"]);

    db.flush();
  }

  testCUD();
}


//version(unittest) void main() {}