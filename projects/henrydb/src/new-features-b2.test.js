// new-features-b2.test.js — Tests for ARRAY literals, VALUES clause, and extended features
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('ARRAY literal syntax', () => {
  let db;
  before(() => { db = new Database(); });

  it('UNNEST with ARRAY literal', () => {
    const r = db.execute('SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS t(val)');
    assert.strictEqual(r.rows.length, 3);
    assert.deepStrictEqual(r.rows.map(r => r.val), [1, 2, 3]);
  });

  it('UNNEST with string ARRAY', () => {
    const r = db.execute("SELECT * FROM UNNEST(ARRAY['a', 'b', 'c']) AS t(letter)");
    assert.strictEqual(r.rows.length, 3);
    assert.deepStrictEqual(r.rows.map(r => r.letter), ['a', 'b', 'c']);
  });

  it('UNNEST + WHERE filter', () => {
    const r = db.execute('SELECT * FROM UNNEST(ARRAY[10, 20, 30, 40, 50]) AS t(val) WHERE val > 25');
    assert.strictEqual(r.rows.length, 3);
  });

  it('UNNEST + aggregation', () => {
    const r = db.execute('SELECT SUM(val) as total, COUNT(*) as cnt FROM UNNEST(ARRAY[1, 2, 3, 4, 5]) AS t(val)');
    assert.strictEqual(r.rows[0].total, 15);
    assert.strictEqual(r.rows[0].cnt, 5);
  });

  it('empty ARRAY', () => {
    const r = db.execute('SELECT * FROM UNNEST(ARRAY[]) AS t(val)');
    assert.strictEqual(r.rows.length, 0);
  });
});

describe('VALUES clause as FROM source', () => {
  let db;
  before(() => { db = new Database(); });

  it('basic VALUES', () => {
    const r = db.execute("SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name)");
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[0].name, 'alice');
  });

  it('VALUES with WHERE', () => {
    const r = db.execute('SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(id, val) WHERE val > 15');
    assert.strictEqual(r.rows.length, 2);
  });

  it('VALUES with aggregation', () => {
    const r = db.execute('SELECT AVG(val) as avg_val FROM (VALUES (10), (20), (30)) AS t(val)');
    assert.strictEqual(r.rows[0].avg_val, 20);
  });

  it('VALUES with NULL', () => {
    const r = db.execute('SELECT * FROM (VALUES (1, NULL), (2, 42)) AS t(id, val)');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].val, null);
    assert.strictEqual(r.rows[1].val, 42);
  });

  it('single column VALUES', () => {
    const r = db.execute('SELECT * FROM (VALUES (100), (200), (300)) AS t(score)');
    assert.strictEqual(r.rows.length, 3);
    assert.deepStrictEqual(r.rows.map(r => r.score), [100, 200, 300]);
  });
});

describe('MEDIAN aggregate', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE med_test (val INT)');
    for (const v of [10, 20, 30, 40, 50]) db.execute(`INSERT INTO med_test VALUES (${v})`);
  });

  it('MEDIAN of odd count = middle value', () => {
    const r = db.execute('SELECT MEDIAN(val) as med FROM med_test');
    assert.strictEqual(r.rows[0].med, 30);
  });

  it('MEDIAN works with VALUES', () => {
    const r = db.execute('SELECT MEDIAN(v) as med FROM (VALUES (1), (3), (5), (7)) AS t(v)');
    assert.strictEqual(r.rows[0].med, 4); // interpolated between 3 and 5
  });
});

describe('sqliteCompare in aggregates', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE mixed (val TEXT)');
    db.execute('INSERT INTO mixed VALUES (42)');
    db.execute("INSERT INTO mixed VALUES ('hello')");
    db.execute('INSERT INTO mixed VALUES (0)');
    db.execute("INSERT INTO mixed VALUES ('')");
  });

  it('MAX returns string (higher type class in TEXT column)', () => {
    const r = db.execute('SELECT MAX(val) as mx FROM mixed');
    // TEXT column: all values stored as strings. MAX by string comparison
    assert.strictEqual(typeof r.rows[0].mx, 'string');
  });

  it('MIN returns lowest string value', () => {
    const r = db.execute('SELECT MIN(val) as mn FROM mixed');
    assert.strictEqual(typeof r.rows[0].mn, 'string');
  });
});

describe('window RANGE frame with peers', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE peer_test (id INT, val INT)');
    db.execute('INSERT INTO peer_test VALUES (1, 10)');
    db.execute('INSERT INTO peer_test VALUES (2, 10)');
    db.execute('INSERT INTO peer_test VALUES (3, 20)');
    db.execute('INSERT INTO peer_test VALUES (4, 20)');
    db.execute('INSERT INTO peer_test VALUES (5, 30)');
  });

  it('default frame includes peers', () => {
    const r = db.execute('SELECT id, val, COUNT(*) OVER (ORDER BY val) as cnt FROM peer_test');
    // val=10 peers → cnt=2 (both rows with val=10)
    assert.strictEqual(r.rows[0].cnt, 2);
    assert.strictEqual(r.rows[1].cnt, 2);
    // val=20 peers → cnt=4
    assert.strictEqual(r.rows[2].cnt, 4);
    assert.strictEqual(r.rows[3].cnt, 4);
    // val=30 → cnt=5
    assert.strictEqual(r.rows[4].cnt, 5);
  });
});
