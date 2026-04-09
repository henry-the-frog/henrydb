// health-check.test.js — Tests for database health checker
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { healthCheck, formatHealthCheck } from './health-check.js';
import { Database } from './db.js';

describe('Health Check', () => {
  it('healthy database passes all checks', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    
    const r = healthCheck(db);
    assert.equal(r.status, 'healthy');
    assert.equal(r.summary.failing, 0);
    assert.ok(r.summary.passing >= 4);
  });

  it('reports connectivity', () => {
    const db = new Database();
    const r = healthCheck(db);
    const conn = r.checks.find(c => c.name === 'connectivity');
    assert.equal(conn.status, 'pass');
  });

  it('reports table count', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INTEGER PRIMARY KEY)');
    db.execute('CREATE TABLE t2 (id INTEGER PRIMARY KEY)');
    const r = healthCheck(db);
    const tables = r.checks.find(c => c.name === 'tables');
    assert.equal(tables.count, 2);
  });

  it('measures performance', () => {
    const db = new Database();
    const r = healthCheck(db);
    const perf = r.checks.find(c => c.name === 'performance');
    assert.ok(perf.qps > 0);
    assert.ok(perf.latencyMs >= 0);
  });

  it('counts total rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const r = healthCheck(db);
    const integrity = r.checks.find(c => c.name === 'integrity');
    assert.equal(integrity.totalRows, 10);
  });

  it('formatHealthCheck generates readable output', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    const r = healthCheck(db);
    const output = formatHealthCheck(r);
    assert.ok(output.includes('HEALTHY'));
    assert.ok(output.includes('✅'));
  });

  it('summary includes duration', () => {
    const db = new Database();
    const r = healthCheck(db);
    assert.ok(r.summary.durationMs >= 0);
  });
});
