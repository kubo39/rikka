module rikkadb.collection;

import rikkadb.file.coldata;
import rikkadb.file.hashtable;

import std.array;
import std.conv;
import std.file;
import std.format;
import std.json;
import std.path;
import std.string;
import core.sys.posix.fcntl;
import core.sys.posix.unistd;
import core.sys.posix.sys.stat;


// Return string hash code
uint jsonToHash(JSONValue thing) {
  string str = toJSON(&thing);
  uint h = 0;
  foreach (c; str) {
    h = to!uint(c) + (h<<6) + (h<<16) - h;
  }
  return h;
}


// exception class for fail to open file
class FailedOpenFileException : Exception {
  this(string msg) {
    super(msg);
  }
}


// collection class
class Collection {

  ColData data;
  JSONValue[] configs;
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
    if (tryOpen < 0) {
      throw new FailedOpenFileException("Failed to open config file");
    }
    core.sys.posix.unistd.close(tryOpen);

    loadConf();
  }

  // Copy existing config file content to backup config file
  void backupAndSaveConf() {
    auto oldConfig = readText(configFileName);
    std.file.write(configBackup, cast(ubyte[])oldConfig);

    if (configs.length != 0) {
      string[] tmp;
      for (int i; i < configs.length; ++i) {
	tmp ~= toJSON(&(configs[i]));
      }
      std.file.write(configFileName, tmp.join("\n"));
    }
  }

  // load configuration to collection
  void loadConf() {
    auto tmp = readText(configFileName);
    if (to!string(tmp) != "") {
      configs = [];
      auto jsonConf = tmp.split("\n");
      foreach (c; jsonConf) {
	configs ~= parseJSON(c);
      }
    }
    configs ~= parseJSON(`{"fname":"_uid", "perBucket":200, "hashBits":14, "indexedPath":["_uid"]}`);

    foreach (i, index; configs) {
      auto obj = index.object;
      auto ht = new HashTable(buildPath(dir, obj["fname"].str), 
			      cast(uint) obj["hashBits"].uinteger,
			      cast(uint) obj["perBucket"].uinteger);
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
	if (is(aMap[seg] == JSONValue))
	  thing = aMap[seg];
      }
    }
    return [thing];
  }

  JSONValue read(uint id) {
    ubyte[] doc = data.read(id);
    return parseJSON(doc);
  }

  void indexDoc(uint id, JSONValue doc) {
    foreach (k, v; strIC) {
      foreach (thing; getIn(doc, to!(string[])(toJSON(&(v.object["indexedPath"]))))) {
	strHT[k].put(jsonToHash(thing), id);
      }
    }
  }

  // Remove the document from all indexes
  void unindexDoc(uint id, JSONValue doc) {
    foreach (k, v; strIC) {
      foreach (thing; getIn(doc, to!(string[])(toJSON(&(v.object["indexedPath"]))))) {
	strHT[k].remove(jsonToHash(thing), 1, (uint k, uint v) { return v == id; });
      }
    }
  }

  // insert new document
  uint insert(JSONValue doc) {
    uint id = data.insert(cast(ubyte[])toJSON(&doc));
    indexDoc(id, doc);
    return id;
  }

  // insert document and flush all bufs
  uint durableInsert(JSONValue doc) {
    auto id = insert(doc);
    flush();
    return id;
  }

  // update document
  uint update(uint id, JSONValue doc) {
    auto newData = toJSON(&doc);

    // read original document
    auto oldData = data.read(id);
    JSONValue oldDoc = parseJSON(oldData);
    unindexDoc(id, oldDoc);
    auto newID = data.update(id, cast(ubyte[])newData);
    indexDoc(newID, doc);
    return newID;
  }

  // update document and fush all bufs
  uint durableUpdate(uint id, JSONValue doc) {
    auto newID = update(id, doc);
    flush();
    return newID;
  }

  // delete document
  void del(uint id) {
    JSONValue oldDoc = read(id);
    data.del(id);
    unindexDoc(id, oldDoc);
  }

  // delete document and flush all bufs
  void durableDelete(uint id) {
    del(id);
    flush();
  }

  // Apply function for all documents
  void forAll(bool delegate(uint, JSONValue) func) {
    data.forAll((uint id, ubyte[] jsonData) {
	JSONValue parsed = parseJSON(jsonData);
  	return func(id, parsed);
      });
  }

  // Flush collection data files
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

    uint[2] ids;
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

    uint[2] ids;
    ids[0] = col.insert(jsonDoc[0]);
    ids[1] = col.insert(jsonDoc[1]);

    ids[0] = col.update(ids[0], updatedJsonDoc[0]);
    ids[1] = col.update(ids[1], updatedJsonDoc[1]);

    JSONValue doc1 = col.read(ids[0]);
    JSONValue doc2 = col.read(ids[1]);

    assert(doc1.object["a"].integer == 2);
    assert(doc2.object["b"].str == "abcdefghijklmnopqrstuvwxyz");

    int counter;
    col.forAll((uint id, JSONValue doc) {
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

    uint[2] ids;
    ids[0] = col.insert(jsonDoc[0]);
    ids[1] = col.insert(jsonDoc[1]);

    col.del(ids[0]);

    auto doc1 = col.read(ids[0]);
    assert(doc1.type == JSON_TYPE.NULL);

    JSONValue doc2 = col.read(ids[1]);
    assert(doc2.object["b"].uinteger == 2);
  }

  void testDurableInsertUpdateDelete() {
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

    uint[2] ids;
    ids[0] = col.durableInsert(jsonDoc[0]);
    ids[1] = col.durableInsert(jsonDoc[1]);

    ids[0] = col.durableUpdate(ids[0], updatedJsonDoc[0]);
    ids[1] = col.durableUpdate(ids[1], updatedJsonDoc[1]);

    col.durableDelete(12345);

    JSONValue doc1 = col.read(ids[0]);
    JSONValue doc2 = col.read(ids[1]);

    assert(doc1.object["a"].integer == 2);
    assert(doc2.object["b"].str == "abcdefghijklmnopqrstuvwxyz");

    int counter;
    col.forAll((uint id, JSONValue doc) {
	counter++;
	return true;
      });

    assert(counter == 2);
  }

  testInsertRead();
  testInsertUpdateReadAll();
  testInsertDeleteRead();
  testDurableInsertUpdateDelete();
}


//version(unittest) void main() {}