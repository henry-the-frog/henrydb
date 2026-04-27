import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('json_each() TVF', () => {
  it('iterates array elements', () => {
    const db = new Database();
    const r = db.execute("SELECT key, value FROM json_each('[10, 20, 30]')");
    assert.strictEqual(r.rows.length, 3);
    assert.strictEqual(r.rows[0].key, 0);
    assert.strictEqual(r.rows[0].value, 10);
    assert.strictEqual(r.rows[2].value, 30);
  });

  it('iterates object keys', () => {
    const db = new Database();
    const r = db.execute(`SELECT key, value FROM json_each('{"a": 1, "b": 2}')`);
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].key, 'a');
    assert.strictEqual(r.rows[0].value, 1);
  });

  it('supports WHERE filtering', () => {
    const db = new Database();
    const r = db.execute("SELECT value FROM json_each('[1,2,3,4,5]') WHERE value > 3");
    assert.strictEqual(r.rows.length, 2);
  });

  it('supports aggregation', () => {
    const db = new Database();
    const r = db.execute("SELECT SUM(value) as total FROM json_each('[10, 20, 30]')");
    assert.strictEqual(r.rows[0].total, 60);
  });

  it('handles nested JSON values', () => {
    const db = new Database();
    const r = db.execute(`SELECT key, type FROM json_each('[1, "hello", [1,2], {"x": 1}]')`);
    assert.strictEqual(r.rows[0].type, 'number');
    assert.strictEqual(r.rows[1].type, 'string');
    assert.strictEqual(r.rows[2].type, 'array');
    assert.strictEqual(r.rows[3].type, 'object');
  });

  it('returns empty for invalid JSON', () => {
    const db = new Database();
    const r = db.execute("SELECT * FROM json_each('not-json')");
    assert.strictEqual(r.rows.length, 0);
  });
});

describe('generate_series() TVF', () => {
  it('generates basic series', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM generate_series(1, 5)');
    assert.deepStrictEqual(r.rows.map(r => r.value), [1, 2, 3, 4, 5]);
  });

  it('generates series with step', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM generate_series(0, 20, 5)');
    assert.deepStrictEqual(r.rows.map(r => r.value), [0, 5, 10, 15, 20]);
  });

  it('supports WHERE filtering', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM generate_series(1, 10) WHERE value % 2 = 0');
    assert.deepStrictEqual(r.rows.map(r => r.value), [2, 4, 6, 8, 10]);
  });

  it('supports aggregation', () => {
    const db = new Database();
    const r = db.execute('SELECT SUM(value) as total FROM generate_series(1, 100)');
    assert.strictEqual(r.rows[0].total, 5050);
  });

  it.skip('works with JOIN (TODO: cross-join with TVF)', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT, name TEXT)');
    db.execute("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    const r = db.execute(`
      SELECT i.name, s.value 
      FROM items i, generate_series(1, 2) s 
      ORDER BY i.id, s.value
    `);
    assert.strictEqual(r.rows.length, 6); // 3 items × 2 series values
  });
});
