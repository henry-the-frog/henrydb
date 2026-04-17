// json-depth.test.js — JSON function depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-json-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE docs (id INT, data TEXT)');
  db.execute(`INSERT INTO docs VALUES (1, '{"name": "Alice", "age": 30, "tags": ["admin", "user"]}')`);
  db.execute(`INSERT INTO docs VALUES (2, '{"name": "Bob", "age": 25, "tags": ["user"]}')`);
  db.execute(`INSERT INTO docs VALUES (3, '{"name": "Carol", "age": 35, "nested": {"city": "Denver"}}')`);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('JSON_EXTRACT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('extract string field', () => {
    const r = rows(db.execute("SELECT JSON_EXTRACT(data, '$.name') AS name FROM docs WHERE id = 1"));
    assert.equal(r[0].name, 'Alice');
  });

  it('extract number field', () => {
    const r = rows(db.execute("SELECT JSON_EXTRACT(data, '$.age') AS age FROM docs WHERE id = 2"));
    assert.equal(r[0].age, 25);
  });

  it('extract nested field', () => {
    const r = rows(db.execute("SELECT JSON_EXTRACT(data, '$.nested.city') AS city FROM docs WHERE id = 3"));
    assert.equal(r[0].city, 'Denver');
  });

  it('extract array element', () => {
    const r = rows(db.execute("SELECT JSON_EXTRACT(data, '$.tags[0]') AS tag FROM docs WHERE id = 1"));
    assert.equal(r[0].tag, 'admin');
  });

  it('extract non-existent field returns NULL', () => {
    const r = rows(db.execute("SELECT JSON_EXTRACT(data, '$.missing') AS val FROM docs WHERE id = 1"));
    assert.equal(r[0].val, null);
  });
});

describe('JSON_ARRAY', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('creates JSON array from values', () => {
    const r = rows(db.execute('SELECT JSON_ARRAY(1, 2, 3) AS arr'));
    const arr = JSON.parse(r[0].arr);
    assert.deepEqual(arr, [1, 2, 3]);
  });

  it('JSON_ARRAY with strings', () => {
    const r = rows(db.execute("SELECT JSON_ARRAY('a', 'b', 'c') AS arr"));
    const arr = JSON.parse(r[0].arr);
    assert.deepEqual(arr, ['a', 'b', 'c']);
  });
});

describe('JSON_OBJECT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('creates JSON object from key-value pairs', () => {
    const r = rows(db.execute("SELECT JSON_OBJECT('name', 'Alice', 'age', 30) AS obj"));
    const obj = JSON.parse(r[0].obj);
    assert.equal(obj.name, 'Alice');
    assert.equal(obj.age, 30);
  });
});

describe('JSON in WHERE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('filter by JSON field', () => {
    const r = rows(db.execute(
      "SELECT id FROM docs WHERE JSON_EXTRACT(data, '$.age') > 28 ORDER BY id"
    ));
    assert.equal(r.length, 2); // Alice (30) and Carol (35)
    assert.equal(r[0].id, 1);
    assert.equal(r[1].id, 3);
  });

  it('filter by JSON string field', () => {
    const r = rows(db.execute(
      "SELECT id FROM docs WHERE JSON_EXTRACT(data, '$.name') = 'Bob'"
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 2);
  });
});
