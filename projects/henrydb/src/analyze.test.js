// analyze.test.js — Tests for ANALYZE command
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('ANALYZE', () => {
  it('analyzes single table', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO users VALUES (${i}, 'user${i}', ${20 + i})`);
    
    const r = db.execute('ANALYZE users');
    assert.strictEqual(r.type, 'ANALYZE');
    assert.strictEqual(r.tables.length, 1);
    assert.strictEqual(r.tables[0].table, 'users');
    assert.strictEqual(r.tables[0].rows, 10);
  });

  it('collects per-column statistics', () => {
    const db = new Database();
    db.execute('CREATE TABLE stats (id INT PRIMARY KEY, category TEXT, value INT)');
    db.execute("INSERT INTO stats VALUES (1, 'A', 100)");
    db.execute("INSERT INTO stats VALUES (2, 'B', 200)");
    db.execute("INSERT INTO stats VALUES (3, 'A', 300)");
    db.execute("INSERT INTO stats VALUES (4, 'C', 400)");
    
    const r = db.execute('ANALYZE stats');
    const cols = r.tables[0].columns;
    
    // Check category column
    const catCol = cols.find(c => c.name === 'category');
    assert.ok(catCol);
    assert.strictEqual(catCol.ndv, 3); // A, B, C
    assert.strictEqual(catCol.nulls, 0);
    
    // Check value column
    const valCol = cols.find(c => c.name === 'value');
    assert.ok(valCol);
    assert.strictEqual(valCol.min, 100);
    assert.strictEqual(valCol.max, 400);
  });

  it('ANALYZE all tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE t3 (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t1 VALUES (1)');
    
    const r = db.execute('ANALYZE');
    assert.strictEqual(r.tables.length, 3);
  });

  it('handles empty table', () => {
    const db = new Database();
    db.execute('CREATE TABLE empty (id INT PRIMARY KEY, val TEXT)');
    
    const r = db.execute('ANALYZE empty');
    assert.strictEqual(r.tables[0].rows, 0);
  });

  it('returns message with table info', () => {
    const db = new Database();
    db.execute('CREATE TABLE msgs (id INT PRIMARY KEY)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO msgs VALUES (${i})`);
    
    const r = db.execute('ANALYZE msgs');
    assert.ok(r.message.includes('msgs'));
    assert.ok(r.message.includes('50'));
  });

  it('null detection works', () => {
    const db = new Database();
    db.execute('CREATE TABLE nullable (id INT PRIMARY KEY, opt TEXT)');
    db.execute("INSERT INTO nullable VALUES (1, 'yes')");
    db.execute('INSERT INTO nullable VALUES (2, NULL)');
    db.execute("INSERT INTO nullable VALUES (3, 'no')");
    db.execute('INSERT INTO nullable VALUES (4, NULL)');
    
    const r = db.execute('ANALYZE nullable');
    const optCol = r.tables[0].columns.find(c => c.name === 'opt');
    assert.ok(optCol);
    assert.strictEqual(optCol.nulls, 2);
    assert.strictEqual(optCol.ndv, 2); // 'yes' and 'no'
  });
});
