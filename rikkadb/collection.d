module rikkadb.collection;

import rikkadb.file.coldata;
import rikkadb.file.hashtable;

import std.array;
import std.conv;
import std.file;
import std.format;
import std.json;
import std.path;
import std.stdio;
import std.string;
import std.typecons;
import core.sys.posix.fcntl;
import core.sys.posix.unistd;
import core.sys.posix.sys.stat;


struct config {
  JSONValue[] indexes;
}


// Return string hash code
ulong jsonToHash(JSONValue thing) {
  string str = toJSON(&thing);
  auto h = 0;
  foreach (c; str) {
    h = to!int(c) + (h<<6) + (h<<16) - h;
  }
  return cast(ulong)h;
}


// collection class
class Collection {

  ColData data;
  config conf;
  string dir;
  string configFileName;
  string configBackup;
  HashTable[string] strHT;
  JSONValue[string] strIC;

  // open collection
  this(string _dir) {
    mkdirRecurse(_dir);

    configFileName = buildPath(_dir, "config");
    configBackup = buildPath(_dir, "config.bak");
    dir = _dir;

    data = new ColData(buildPath(dir, "data"));

    // make sure the config file exists
    auto tryOpen = open(configFileName.toStringz, O_CREAT|O_RDWR, octal!"600");
    core.sys.posix.unistd.close(tryOpen);

    loadConf();
  }

  // Copy existing config file content to backup config file
  void backupAndSaveConf() {
    auto oldConfig = readText(configFileName);
    std.file.write(configBackup, cast(ubyte[])oldConfig);

    if (conf.indexes.length != 0) {
      string[] tmp;
      for (int i; i < conf.indexes.length; ++i) {
	tmp ~= toJSON(&(conf.indexes[i]));
      }
      std.file.write(configFileName, tmp.join("\n"));
    }
  }

  // load configuration to collection
  void loadConf() {
    auto tmp = readText(configFileName);
    if (to!string(tmp) != "") {
      conf.indexes = [];
      auto jsonConf = tmp.split("\n");
      foreach (c; jsonConf) {
	conf.indexes ~= parseJSON(c);
      }
    }
    conf.indexes ~= parseJSON(`{"fname":"_uid", "perBucket":200, "hashBits":14, "indexedPath":["_uid"]}`);

    foreach (i, index; conf.indexes) {
      auto obj = index.object;
      auto ht = new HashTable(buildPath(dir, obj["fname"].str), obj["hashBits"].integer,
				obj["perBucket"].integer);
      auto k = to!(string[])(toJSON(&obj["indexedPath"])).join(",");
      strHT[k] = ht;
      strIC[k] = index;
    }
  }

  JSONValue[] getIn(JSONValue doc, string[] path) {
    JSONValue thing = doc;

    foreach (seg; path) {
      auto aMap = thing.object;
      if (seg in aMap) {
	if (aMap[seg].init == JSONValue.init) // TODO: fix, it's bug
	  thing = aMap[seg];
      }
    }
    return [thing];
  }

  JSONValue read(ulong id) {
    ubyte[] doc = data.read(id);
    return parseJSON(doc);
  }

  void indexDoc(ulong id, JSONValue doc) {
    foreach (k, v; strIC) {
      foreach (thing; getIn(doc, to!(string[])(toJSON(&(v.object["indexedPath"]))))) {
	strHT[k].put(jsonToHash(thing), id);
      }
    }
  }

  // Remove the document from all indexes
  void unindexDoc(ulong id, JSONValue doc) {
    foreach (k, v; strIC) {
      foreach (thing; getIn(doc, to!(string[])(toJSON(&(v.object["indexedPath"]))))) {
	strHT[k].remove(jsonToHash(thing), 1L, (ulong k, ulong v) { return v == id; });
      }
    }
  }

  ulong insert(JSONValue doc) {
    ulong id = data.insert(cast(ubyte[])toJSON(&doc));
    indexDoc(id, doc);
    return id;
  }

  ulong update(ulong id, JSONValue doc) {
    auto newData = toJSON(&doc);

    // read original document
    auto oldData = data.read(id);
    JSONValue oldDoc = parseJSON(oldData);
    unindexDoc(id, oldDoc);
    auto newID = data.update(id, cast(ubyte[])newData);
    indexDoc(newID, doc);
    return newID;
  }

  void del(ulong id) {
    JSONValue oldDoc = read(id);
    data.del(id);
    unindexDoc(id, oldDoc);
  }

  // Apply function for all documents (deserialized into generic interface)
  void forAll(bool delegate(ulong, JSONValue) func) {
    data.forAll((ulong id, ubyte[] jsonData) {
  	JSONValue parsed;
  	try {
  	  parsed = parseJSON(jsonData);
  	} catch {
  	  return true;
  	}
  	return func(id, parsed);
      });
  }

  // Flush collection data files.
  void flush() {
    data.f.flush();
    foreach (ht; strHT) {
      ht.f.flush();
    }
  }

  void close() {
    data.f.close();
    foreach (ht; strHT) {
      ht.f.close();
    }
  }

}


unittest {
  string tmp = "/tmp/rikka_col_test";

  void testInsertRead() {
    if (exists(tmp) && isDir(tmp)) {
      rmdirRecurse(tmp);
    }
    scope(exit) {
      if (exists(tmp) && isDir(tmp)) {
	rmdirRecurse(tmp);
      }
    }

    auto col = new Collection(tmp);
    scope(exit) col.close;

    auto docs = [`{"a": 1}`, `{"b": 2}`];
    JSONValue[2] jsonDoc;
    jsonDoc[0] = parseJSON(docs[0]);
    jsonDoc[1] = parseJSON(docs[1]);

    ulong[2] ids;
    ids[0] = col.insert(jsonDoc[0]);
    ids[1] = col.insert(jsonDoc[1]);

    auto doc1 = col.read(ids[0]);
    auto doc2 = col.read(ids[1]);

    assert(doc1.object["a"].integer == 1);
    assert(doc2.object["b"].integer == 2);
  }

  void testInsertUpdateReadAll() {
    if (exists(tmp) && isDir(tmp)) {
      rmdirRecurse(tmp);
    }
    scope(exit) {
      if (exists(tmp) && isDir(tmp)) {
	rmdirRecurse(tmp);
      }
    }

    auto col = new Collection(tmp);
    scope(exit) col.close();

    auto docs = [`{"a": 1}`, `{"b": 2}`];
    JSONValue[2] jsonDoc;
    jsonDoc[0] = parseJSON(docs[0]);
    jsonDoc[1] = parseJSON(docs[1]);

    auto updatedDocs = [`{"a": 2}`, `{"b": "abcdefghijklmnopqrstuvwxyz"}`];
    JSONValue[2] updatedJsonDoc;
    updatedJsonDoc[0] = parseJSON(updatedDocs[0]);
    updatedJsonDoc[1] = parseJSON(updatedDocs[1]);

    ulong[2] ids;
    ids[0] = col.insert(jsonDoc[0]);
    ids[1] = col.insert(jsonDoc[1]);

    ids[0] = col.update(ids[0], updatedJsonDoc[0]);
    ids[1] = col.update(ids[1], updatedJsonDoc[1]);

    JSONValue doc1 = col.read(ids[0]);
    JSONValue doc2 = col.read(ids[1]);

    assert(doc1.object["a"].integer == 2);
    assert(doc2.object["b"].str == "abcdefghijklmnopqrstuvwxyz");

    int counter;
    col.forAll((ulong id, JSONValue doc) {
	counter++;
	return true;
      });

    assert(counter == 2);
  }

  void testInsertDeleteRead() {
    if (exists(tmp) && isDir(tmp)) {
      rmdirRecurse(tmp);
    }
    scope(exit) {
      if (exists(tmp) && isDir(tmp)) {
	rmdirRecurse(tmp);
      }
    }

    auto col = new Collection(tmp);
    scope(exit) col.close();

    auto docs = [`{"a": 1}`, `{"b": 2}`];

    JSONValue[2] jsonDoc;
    jsonDoc[0] = parseJSON(docs[0]);
    jsonDoc[1] = parseJSON(docs[1]);

    ulong[2] ids;
    ids[0] = col.insert(jsonDoc[0]);
    ids[1] = col.insert(jsonDoc[1]);

    col.del(ids[0]);

    auto doc1 = col.read(ids[0]);
    assert(doc1.type == JSON_TYPE.NULL);

    JSONValue doc2 = col.read(ids[1]);
    assert(doc2.object["b"].uinteger == 2);
  }

  testInsertRead();
  testInsertUpdateReadAll();
  testInsertDeleteRead();
}


//version(unittest) void main() {}