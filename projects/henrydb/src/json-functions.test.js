// json-functions.test.js — JSON function tests

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('JSON Construction', () => {
  it('JSON_OBJECT', () => {
    const db = new Database();
    const r = db.execute("SELECT JSON_OBJECT('name', 'Alice', 'age', 30) as j");
    const obj = JSON.parse(r.rows[0].j);
    assert.equal(obj.name, 'Alice');
    assert.equal(obj.age, 30);
  });

  it('JSON_ARRAY', () => {
    const db = new Database();
    const r = db.execute("SELECT JSON_ARRAY(1, 2, 3) as j");
    assert.deepStrictEqual(JSON.parse(r.rows[0].j), [1, 2, 3]);
  });

  it('JSON_BUILD_OBJECT', () => {
    const db = new Database();
    const r = db.execute("SELECT JSON_BUILD_OBJECT('x', 1, 'y', 2) as j");
    const obj = JSON.parse(r.rows[0].j);
    assert.equal(obj.x, 1);
    assert.equal(obj.y, 2);
  });
});

describe('JSON Extraction', () => {
  let db;

  it('JSON_EXTRACT with dot path', () => {
    db = new Database();
    const r = db.execute(`SELECT JSON_EXTRACT('{"name":"Alice","age":30}', '$.name') as val`);
    assert.equal(r.rows[0].val, 'Alice');
  });

  it('nested JSON_EXTRACT', () => {
    db = new Database();
    const r = db.execute(`SELECT JSON_EXTRACT('{"a":{"b":42}}', '$.a.b') as val`);
    assert.equal(r.rows[0].val, 42);
  });

  it('JSON_EXTRACT array element', () => {
    db = new Database();
    const r = db.execute(`SELECT JSON_EXTRACT('[10,20,30]', '$[1]') as val`);
    assert.equal(r.rows[0].val, 20);
  });

  it('-> operator', () => {
    db = new Database();
    db.execute("CREATE TABLE jt (id INT PRIMARY KEY, data TEXT)");
    db.execute(`INSERT INTO jt VALUES (1, '{"name":"Bob"}')`);
    const r = db.execute("SELECT data -> 'name' as name FROM jt");
    assert.equal(r.rows[0].name, 'Bob');
  });

  it('->> operator', () => {
    db = new Database();
    db.execute("CREATE TABLE jt (id INT PRIMARY KEY, data TEXT)");
    db.execute(`INSERT INTO jt VALUES (1, '{"count":42}')`);
    const r = db.execute("SELECT data ->> 'count' as cnt FROM jt");
    assert.ok(r.rows[0].cnt == 42);
  });
});

describe('JSON Inspection', () => {
  it('JSON_TYPE object', () => {
    const db = new Database();
    assert.equal(db.execute(`SELECT JSON_TYPE('{"a":1}') as t`).rows[0].t, 'object');
  });

  it('JSON_TYPE array', () => {
    const db = new Database();
    assert.equal(db.execute(`SELECT JSON_TYPE('[1,2]') as t`).rows[0].t, 'array');
  });

  it('JSON_VALID', () => {
    const db = new Database();
    assert.equal(db.execute(`SELECT JSON_VALID('{"a":1}') as v`).rows[0].v, 1);
    assert.equal(db.execute(`SELECT JSON_VALID('not json') as v`).rows[0].v, 0);
  });

  it('JSON_ARRAY_LENGTH', () => {
    const db = new Database();
    assert.equal(db.execute(`SELECT JSON_ARRAY_LENGTH('[1,2,3,4]') as len`).rows[0].len, 4);
    assert.equal(db.execute(`SELECT JSON_ARRAY_LENGTH('[]') as len`).rows[0].len, 0);
  });
});

describe('JSON Modification', () => {
  it('JSON_SET adds property', () => {
    const db = new Database();
    const r = db.execute(`SELECT JSON_SET('{"a":1}', '$.b', 2) as j`);
    const obj = JSON.parse(r.rows[0].j);
    assert.equal(obj.a, 1);
    assert.equal(obj.b, 2);
  });
});

describe('JSON in Queries', () => {
  it('WHERE with JSON_EXTRACT', () => {
    const db = new Database();
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, data TEXT)");
    db.execute(`INSERT INTO docs VALUES (1, '{"score":85}')`);
    db.execute(`INSERT INTO docs VALUES (2, '{"score":92}')`);
    db.execute(`INSERT INTO docs VALUES (3, '{"score":67}')`);
    
    const r = db.execute("SELECT id FROM docs WHERE JSON_EXTRACT(data, '$.score') > 80 ORDER BY id");
    assert.equal(r.rows.length, 2);
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2]);
  });

  it('GROUP BY with JSON_EXTRACT', () => {
    const db = new Database();
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, data TEXT)");
    db.execute(`INSERT INTO items VALUES (1, '{"type":"A"}')`);
    db.execute(`INSERT INTO items VALUES (2, '{"type":"B"}')`);
    db.execute(`INSERT INTO items VALUES (3, '{"type":"A"}')`);
    
    const r = db.execute(`
      SELECT JSON_EXTRACT(data, '$.type') as type, COUNT(*) as cnt
      FROM items
      GROUP BY JSON_EXTRACT(data, '$.type')
      ORDER BY cnt DESC
    `);
    assert.equal(r.rows.length, 2);
  });

  it('JSON_EXTRACT NULL returns NULL', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT JSON_EXTRACT(NULL, '$.x') as r").rows[0].r, null);
  });
});
