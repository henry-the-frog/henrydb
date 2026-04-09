// table-utils.test.js — Tests for table utilities
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { copyTable, sampleTable, describeTable, tableInfo, truncateTable, topN } from './table-utils.js';
import { Database } from './db.js';

describe('Table Utilities', () => {
  function makeDb() {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO users VALUES (${i}, 'User${i}', ${i * 5})`);
    }
    return db;
  }

  it('copyTable: creates exact copy', () => {
    const db = makeDb();
    copyTable(db, 'users', 'users_copy');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM users_copy').rows[0].cnt, 20);
  });

  it('copyTable: with WHERE filter', () => {
    const db = makeDb();
    copyTable(db, 'users', 'top_users', { where: 'score > 50' });
    const r = db.execute('SELECT COUNT(*) as cnt FROM top_users');
    assert.ok(r.rows[0].cnt > 0 && r.rows[0].cnt < 20);
  });

  it('sampleTable: returns N random rows', () => {
    const db = makeDb();
    const sample = sampleTable(db, 'users', 5);
    assert.equal(sample.length, 5);
    // All rows should be valid
    sample.forEach(row => assert.ok(row.id >= 1 && row.id <= 20));
  });

  it('sampleTable: returns all if N > total', () => {
    const db = makeDb();
    const sample = sampleTable(db, 'users', 100);
    assert.equal(sample.length, 20);
  });

  it('describeTable: returns column info', () => {
    const db = makeDb();
    const desc = describeTable(db, 'users');
    assert.equal(desc.table, 'users');
    assert.equal(desc.columns.length, 3);
    assert.equal(desc.columns[0].name, 'id');
    assert.equal(desc.columns[0].type, 'INTEGER');
  });

  it('tableInfo: returns row count and metadata', () => {
    const db = makeDb();
    const info = tableInfo(db, 'users');
    assert.equal(info.rowCount, 20);
    assert.equal(info.columnCount, 3);
    assert.ok(info.columns.includes('name'));
  });

  it('truncateTable: removes all rows', () => {
    const db = makeDb();
    truncateTable(db, 'users');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM users').rows[0].cnt, 0);
  });

  it('topN: returns top rows by column', () => {
    const db = makeDb();
    const top = topN(db, 'users', 'score', 3, 'DESC');
    assert.equal(top.rows.length, 3);
    assert.equal(top.rows[0].score, 100); // Highest score
  });

  it('topN: ascending order', () => {
    const db = makeDb();
    const bottom = topN(db, 'users', 'score', 3, 'ASC');
    assert.equal(bottom.rows[0].score, 5); // Lowest score
  });
});
