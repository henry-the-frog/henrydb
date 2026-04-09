// connection-pool.test.js — Tests for connection pool
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { ConnectionPool } from './connection-pool.js';

describe('Connection Pool', () => {
  let pool;

  afterEach(() => {
    if (pool) pool.shutdown();
  });

  it('creates pool with defaults', () => {
    pool = new ConnectionPool();
    assert.equal(pool.size, 0);
    assert.equal(pool.stats().maxConnections, 10);
  });

  it('acquires and releases connections', () => {
    pool = new ConnectionPool({ max: 5 });
    const conn = pool.acquire();
    assert.ok(conn);
    assert.equal(pool.stats().active, 1);
    
    pool.release(conn);
    assert.equal(pool.stats().active, 0);
    assert.equal(pool.stats().idle, 1);
  });

  it('reuses released connections', () => {
    pool = new ConnectionPool({ max: 5 });
    const conn1 = pool.acquire();
    pool.release(conn1);
    const conn2 = pool.acquire();
    assert.equal(conn1, conn2); // Same instance
    pool.release(conn2);
  });

  it('creates up to max connections', () => {
    pool = new ConnectionPool({ max: 3 });
    const conns = [];
    for (let i = 0; i < 3; i++) conns.push(pool.acquire());
    assert.equal(pool.stats().active, 3);
    conns.forEach(c => pool.release(c));
  });

  it('throws when pool exhausted', () => {
    pool = new ConnectionPool({ max: 2 });
    pool.acquire();
    pool.acquire();
    assert.throws(() => pool.acquire(), /exhausted/);
  });

  it('execute auto-acquires and releases', () => {
    pool = new ConnectionPool({ max: 5 });
    const conn = pool.acquire();
    conn.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    pool.release(conn);
    
    const result = pool.execute('SELECT 1 as val');
    assert.ok(result);
    assert.equal(pool.stats().active, 0);
  });

  it('pre-warms minimum connections', () => {
    pool = new ConnectionPool({ min: 3, max: 10 });
    assert.equal(pool.size, 3);
    assert.equal(pool.stats().idle, 3);
  });

  it('tracks statistics', () => {
    pool = new ConnectionPool({ max: 5 });
    const conn = pool.acquire();
    pool.release(conn);
    pool.acquire();
    pool.release(conn);
    
    const stats = pool.stats();
    assert.ok(stats.acquired >= 2);
    assert.ok(stats.released >= 2);
    assert.ok(stats.created >= 1);
  });

  it('shutdown closes all connections', () => {
    pool = new ConnectionPool({ max: 5 });
    pool.acquire();
    pool.acquire();
    pool.shutdown();
    assert.equal(pool.size, 0);
    assert.throws(() => pool.acquire(), /closed/);
  });

  it('prune removes idle connections', () => {
    pool = new ConnectionPool({ max: 5, idleTimeout: 1 });
    const conn = pool.acquire();
    pool.release(conn);
    
    // Wait for timeout
    const start = Date.now();
    while (Date.now() - start < 5) {}
    
    const pruned = pool.prune();
    assert.equal(pruned, 1);
    assert.equal(pool.size, 0);
  });

  it('handles errors in execute', () => {
    pool = new ConnectionPool({ max: 5 });
    assert.throws(() => pool.execute('INVALID SQL'));
    // Connection should be released back
    assert.equal(pool.stats().active, 0);
    assert.equal(pool.stats().errors, 1);
  });
});
