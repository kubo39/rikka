import rikkadb.rikkadb;

import std.file;
import std.json;
import std.stdio;


void embeddedExample() {
  auto dir = "/tmp/MyDatabase";

  if ( dir.exists() ) {
    dir.rmdirRecurse();
  }
  scope(exit) {
    if ( dir.exists() ) {
      dir.rmdirRecurse();
    }
  }

  // create database
  auto db = new RikkaDB(dir);

  // Create collection
  db.create("A");
  db.create("B");

  // show collections
  foreach (i, name; db.collections) {
    writeln("collection ", i, " ", name);
  }

  // Drop collection
  db.drop("B");

  // use collection
  auto colA = db.use("A");

  // insert document
  JSONValue jsonDoc = parseJSON(`{"a": 1}`);
  uint id = colA.insert(jsonDoc);

  // update document
  JSONValue updatedJsonDoc = parseJSON(`{"b": "abcdefghijklmnopqrstuvwxyz"}`);
  auto newID = colA.update(id, updatedJsonDoc);
  auto ret = colA.read(newID);
  assert(ret.object["b"].str == "abcdefghijklmnopqrstuvwxyz");

  // show all documents
  colA.forAll((uint id, JSONValue doc) {
      writeln(id, ": ", toJSON(&doc));
      return true;
    });

  // delete document
  colA.del(newID);

  auto doc1 = colA.read(newID);
  assert(doc1.type == JSON_TYPE.NULL);
}


void main() {
  embeddedExample();
}  