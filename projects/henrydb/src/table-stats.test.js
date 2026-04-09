// table-stats.test.js — Tests for table statistics collector
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { collectTableStats, collectAllStats, formatStatsReport } from './table-stats.js';
import { Database } from './db.js';

describe('Table Stats', () => {
  function makeDb() {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, score REAL)');
    for (let i = 1; i <= 100; i++) {
      const age = 18 + (i % 50);
      const score = (i * 1.5).toFixed(2);
      db.execute(`INSERT INTO users VALUES (${i}, 'User${i}', ${age}, ${score})`);
    }
    return db;
  }

  it('collects row count', () => {
    const db = makeDb();
    const stats = collectTableStats(db, 'users');
    assert.equal(stats.rowCount, 100);
  });

  it('collects column statistics', () => {
    const db = makeDb();
    const stats = collectTableStats(db, 'users');
    assert.ok(stats.columns.id);
    assert.equal(stats.columns.id.distinctValues, 100);
    assert.equal(stats.columns.id.min, 1);
    assert.equal(stats.columns.id.max, 100);
  });

  it('calculates distinct value count', () => {
    const db = makeDb();
    const stats = collectTableStats(db, 'users');
    assert.ok(stats.columns.age.distinctValues <= 50);
  });

  it('calculates selectivity', () => {
    const db = makeDb();
    const stats = collectTableStats(db, 'users');
    assert.equal(stats.columns.id.selectivity, 1);
    assert.ok(stats.columns.age.selectivity < 1);
  });

  it('calculates numeric stats', () => {
    const db = makeDb();
    const stats = collectTableStats(db, 'users');
    assert.ok(stats.columns.score.avg > 0);
    assert.ok(stats.columns.score.sum > 0);
  });

  it('handles empty table', () => {
    const db = new Database();
    db.execute('CREATE TABLE empty (id INTEGER PRIMARY KEY)');
    const stats = collectTableStats(db, 'empty');
    assert.equal(stats.rowCount, 0);
  });

  it('estimates size', () => {
    const db = makeDb();
    const stats = collectTableStats(db, 'users');
    assert.ok(stats.sizeEstimate > 0);
  });

  it('collectAllStats covers all tables', () => {
    const db = makeDb();
    db.execute('CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)');
    db.execute("INSERT INTO posts VALUES (1, 'Hello')");
    
    const all = collectAllStats(db);
    assert.ok(all.users);
    assert.ok(all.posts);
    assert.equal(all.users.rowCount, 100);
    assert.equal(all.posts.rowCount, 1);
  });

  it('formatStatsReport generates readable output', () => {
    const db = makeDb();
    const stats = collectTableStats(db, 'users');
    const report = formatStatsReport(stats);
    assert.ok(report.includes('users'));
    assert.ok(report.includes('100'));
    assert.ok(report.includes('id'));
    assert.ok(report.includes('name'));
  });

  it('handles null values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute("INSERT INTO t VALUES (2, 'hello')");
    
    const stats = collectTableStats(db, 't');
    assert.equal(stats.columns.val.nullCount, 1);
    assert.equal(stats.columns.val.nullRate, 50);
  });
});
