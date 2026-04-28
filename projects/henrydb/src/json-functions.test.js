// json-functions.test.js — JSON function correctness tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('JSON Functions', () => {
  it('JSON_BUILD_OBJECT creates JSON object', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT, age INT)');
    db.execute("INSERT INTO t VALUES ('alice', 25)");
    const r = db.execute("SELECT JSON_BUILD_OBJECT('name', name, 'age', age) as j FROM t");
    const obj = JSON.parse(r.rows[0].j);
    assert.equal(obj.name, 'alice');
    assert.equal(obj.age, 25);
  });

  it('JSON_AGG with scalar values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');
    const r = db.execute('SELECT JSON_AGG(val) as arr FROM t');
    const arr = JSON.parse(r.rows[0].arr);
    assert.deepEqual(arr.sort(), [1, 2, 3]);
  });

  it('JSON_AGG with JSON_BUILD_OBJECT (no double-encoding)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT, score INT)');
    db.execute("INSERT INTO t VALUES ('alice',90),('bob',85)");
    const r = db.execute("SELECT JSON_AGG(JSON_BUILD_OBJECT('name', name, 'score', score)) as people FROM t");
    const arr = JSON.parse(r.rows[0].people);
    assert.equal(arr.length, 2);
    assert.equal(typeof arr[0], 'object'); // Should be object, not string
    assert.equal(arr[0].name, 'alice');
    assert.equal(arr[1].name, 'bob');
  });

  it('JSON_AGG with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',10),('B',20),('B',30)");
    const r = db.execute('SELECT grp, JSON_AGG(val) as vals FROM t GROUP BY grp ORDER BY grp');
    const a = JSON.parse(r.rows[0].vals);
    const b = JSON.parse(r.rows[1].vals);
    assert.equal(a.length, 2);
    assert.equal(b.length, 3);
  });

  it('JSON_EXTRACT basic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (data TEXT)');
    db.execute('INSERT INTO t VALUES (\'{"name":"alice","age":25}\')');
    const r = db.execute("SELECT JSON_EXTRACT(data, '$.name') as name FROM t");
    assert.equal(r.rows[0].name, 'alice');
  });

  it('JSON_ARRAY creates JSON array', () => {
    const db = new Database();
    const r = db.execute("SELECT JSON_ARRAY(1, 'hello', 3.14) as arr");
    const arr = JSON.parse(r.rows[0].arr);
    assert.equal(arr.length, 3);
    assert.equal(arr[0], 1);
    assert.equal(arr[1], 'hello');
  });

  it('JSON_VALID detects valid JSON', () => {
    const db = new Database();
    const r1 = db.execute("SELECT JSON_VALID('{\"a\":1}') as v");
    assert.ok(r1.rows[0].v, 'valid JSON should be truthy');
    const r2 = db.execute("SELECT JSON_VALID('not json') as v");
    assert.ok(!r2.rows[0].v, 'invalid JSON should be falsy');
  });

  it('JSON_ARRAY_LENGTH', () => {
    const db = new Database();
    const r = db.execute("SELECT JSON_ARRAY_LENGTH('[1,2,3,4,5]') as len");
    assert.equal(r.rows[0].len, 5);
  });

  it('JSON_TYPE', () => {
    const db = new Database();
    const r1 = db.execute("SELECT JSON_TYPE('{\"a\":1}') as t");
    assert.equal(r1.rows[0].t, 'object');
    const r2 = db.execute("SELECT JSON_TYPE('[1,2]') as t");
    assert.equal(r2.rows[0].t, 'array');
    const r3 = db.execute("SELECT JSON_TYPE('42') as t");
    assert.equal(r3.rows[0].t, 'integer'); // SQLite convention: integer for whole numbers, real for decimals
  });

  it('ARRAY_AGG returns JS array', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('a'),('b'),('c')");
    const r = db.execute('SELECT ARRAY_AGG(val) as arr FROM t');
    assert.ok(Array.isArray(r.rows[0].arr));
    assert.equal(r.rows[0].arr.length, 3);
  });

  it('JSON_BUILD_OBJECT with NULL values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (name TEXT, age INT)');
    db.execute("INSERT INTO t VALUES ('alice', NULL)");
    const r = db.execute("SELECT JSON_BUILD_OBJECT('name', name, 'age', age) as j FROM t");
    const obj = JSON.parse(r.rows[0].j);
    assert.equal(obj.name, 'alice');
    assert.equal(obj.age, null);
  });
});
