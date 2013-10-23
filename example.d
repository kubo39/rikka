import rikkadb.rikkadb;

import std.file;
import std.json;
import std.stdio;


void embeddedExample() {
  auto dir = "/tmp/MyDatabase";

  if (exists(dir)) {
    rmdirRecurse(dir);
  }
  scope(exit) {
    if (exists(dir)) {
      rmdirRecurse(dir);
    }
  }

  auto db = new RikkaDB(dir);

  // Create collection
  db.create("A");
  db.create("B");

  // show collections
  foreach (i, name; db.strCol) {
    writeln("collection ", i, " ", name);
  }

  // Drop collection
  db.drop("B");

  // use collection
  auto colA = db.use("A");

  // insert document
  auto doc = `{"a": 1}`;
  JSONValue jsonDoc = parseJSON(doc);
  uint id = colA.insert(jsonDoc);

  // update document
  auto updatedDoc = `{"b": "abcdefghijklmnopqrstuvwxyz"}`;
  JSONValue updatedJsonDoc = parseJSON(updatedDoc);
  auto newID = colA.update(id, updatedJsonDoc);
  auto ret = colA.read(newID);
  // writeln(toJSON(&ret));
  assert(ret.object["b"].str == "abcdefghijklmnopqrstuvwxyz");

  // delete document
  colA.del(newID);

  auto doc1 = colA.read(newID);
  assert(doc1.type == JSON_TYPE.NULL);
}


void main() {
  embeddedExample();
}  