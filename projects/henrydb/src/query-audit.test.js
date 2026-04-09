// query-audit.test.js — Tests for query audit log
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { QueryAudit } from './query-audit.js';

describe('Query Audit Log', () => {
  let audit;

  beforeEach(() => {
    audit = new QueryAudit({ maxEntries: 100 });
  });

  it('logs queries', () => {
    audit.log({ sql: 'SELECT * FROM users', duration: 12 });
    assert.equal(audit.size, 1);
  });

  it('classifies SQL types', () => {
    audit.log({ sql: 'SELECT * FROM t' });
    audit.log({ sql: 'INSERT INTO t VALUES (1)' });
    audit.log({ sql: 'UPDATE t SET x = 1' });
    audit.log({ sql: 'DELETE FROM t' });
    audit.log({ sql: 'CREATE TABLE t (id INT)' });
    
    const s = audit.summary();
    assert.equal(s.selects, 1);
    assert.equal(s.inserts, 1);
    assert.equal(s.updates, 1);
    assert.equal(s.deletes, 1);
    assert.equal(s.ddl, 1);
  });

  it('finds slow queries', () => {
    audit.log({ sql: 'SELECT 1', duration: 5 });
    audit.log({ sql: 'SELECT * FROM big', duration: 500 });
    audit.log({ sql: 'SELECT * FROM huge', duration: 2000 });
    
    const slow = audit.slowQueries(100);
    assert.equal(slow.length, 2);
    assert.equal(slow[0].duration, 2000); // Sorted by duration desc
  });

  it('finds frequent queries', () => {
    for (let i = 0; i < 10; i++) audit.log({ sql: 'SELECT * FROM users' });
    for (let i = 0; i < 5; i++) audit.log({ sql: 'SELECT * FROM posts' });
    audit.log({ sql: 'SELECT * FROM rare' });
    
    const freq = audit.frequentQueries(2);
    assert.equal(freq.length, 2);
    assert.equal(freq[0].count, 10);
    assert.equal(freq[1].count, 5);
  });

  it('filters by table', () => {
    audit.log({ sql: 'SELECT * FROM users' });
    audit.log({ sql: 'SELECT * FROM posts' });
    audit.log({ sql: 'SELECT * FROM users JOIN posts ON users.id = posts.user_id' });
    
    const userQueries = audit.queriesForTable('users');
    assert.equal(userQueries.length, 2);
  });

  it('tracks errors', () => {
    audit.log({ sql: 'SELECT * FROM t', error: null });
    audit.log({ sql: 'INVALID SQL', error: 'Parse error' });
    
    assert.equal(audit.errors().length, 1);
    assert.equal(audit.summary().errorRate, 50);
  });

  it('recent returns last N', () => {
    for (let i = 0; i < 30; i++) audit.log({ sql: `SELECT ${i}` });
    const recent = audit.recent(5);
    assert.equal(recent.length, 5);
    assert.ok(recent[0].sql.includes('29')); // Most recent first
  });

  it('calculates percentiles', () => {
    for (let i = 1; i <= 100; i++) {
      audit.log({ sql: 'SELECT 1', duration: i });
    }
    const s = audit.summary();
    assert.equal(s.p50, 50);
    assert.equal(s.p95, 95);
    assert.equal(s.p99, 99);
  });

  it('evicts old entries when over limit', () => {
    for (let i = 0; i < 150; i++) {
      audit.log({ sql: `SELECT ${i}` });
    }
    assert.equal(audit.size, 100); // maxEntries
  });

  it('clear resets everything', () => {
    audit.log({ sql: 'SELECT 1', duration: 10 });
    audit.clear();
    assert.equal(audit.size, 0);
    assert.equal(audit.summary().total, 0);
  });

  it('extracts table names from SQL', () => {
    const r = audit.log({ sql: 'SELECT * FROM users JOIN orders ON users.id = orders.user_id' });
    assert.ok(r.tables.includes('users'));
    assert.ok(r.tables.includes('orders'));
  });

  it('summary includes all stats', () => {
    audit.log({ sql: 'SELECT 1', duration: 10, rows: 1 });
    const s = audit.summary();
    assert.ok('total' in s);
    assert.ok('avgDuration' in s);
    assert.ok('p50' in s);
    assert.ok('p95' in s);
    assert.ok('tablesAccessed' in s);
    assert.ok('errorRate' in s);
  });
});
