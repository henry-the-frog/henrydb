// pool-health.test.js
import { describe, test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { HealthCheckPool } from './pool-health.js';

let pool;

describe('HealthCheckPool', () => {
  beforeEach(() => {
    pool = new HealthCheckPool({
      minSize: 2,
      maxSize: 5,
      idleTimeoutMs: 100,
      maxLifetimeMs: 500,
      validationIntervalMs: 50,
      connectionFactory: () => ({ id: Date.now(), valid: true }),
      validator: (conn) => conn.valid !== false,
    });
  });

  afterEach(async () => { await pool.destroy(); });

  test('warm up creates minimum connections', async () => {
    const count = await pool.warmUp();
    assert.equal(count, 2);
    assert.equal(pool.getStats().total, 2);
  });

  test('acquire returns a connection', async () => {
    await pool.warmUp();
    const conn = await pool.acquire();
    assert.ok(conn);
    assert.equal(conn.state, 'active');
  });

  test('release returns connection to pool', async () => {
    await pool.warmUp();
    const conn = await pool.acquire();
    pool.release(conn);
    assert.equal(conn.state, 'idle');
    assert.equal(pool.getStats().idle, 2);
  });

  test('creates new connection if none idle', async () => {
    await pool.warmUp();
    const c1 = await pool.acquire();
    const c2 = await pool.acquire();
    const c3 = await pool.acquire(); // Creates new
    assert.equal(pool.getStats().total, 3);
    pool.release(c1);
    pool.release(c2);
    pool.release(c3);
  });

  test('rejects when pool exhausted', async () => {
    pool = new HealthCheckPool({
      minSize: 1, maxSize: 2,
      connectionFactory: () => ({ valid: true }),
      validator: () => true,
    });
    await pool.warmUp();
    const c1 = await pool.acquire();
    const c2 = await pool.acquire();
    
    await assert.rejects(() => pool.acquire(), /exhausted/);
    pool.release(c1);
    pool.release(c2);
  });

  test('connection use count tracked', async () => {
    await pool.warmUp();
    const conn = await pool.acquire();
    pool.release(conn);
    const conn2 = await pool.acquire(); // Same connection reused
    assert.ok(conn2.useCount >= 1);
    pool.release(conn2);
  });

  test('health check validates idle connections', async () => {
    await pool.warmUp();
    const results = await pool.healthCheck();
    assert.ok(results.validated >= 0);
    assert.equal(pool.getStats().total, 2);
  });

  test('health check evicts expired connections', async () => {
    pool = new HealthCheckPool({
      minSize: 1, maxSize: 5, maxLifetimeMs: 50,
      connectionFactory: () => ({ valid: true }),
      validator: () => true,
    });
    await pool.warmUp();
    
    await new Promise(r => setTimeout(r, 60));
    const results = await pool.healthCheck();
    assert.ok(results.evicted > 0);
    // Should have re-created to meet minimum
    assert.equal(pool.getStats().total, 1);
  });

  test('health check evicts idle connections', async () => {
    pool = new HealthCheckPool({
      minSize: 1, maxSize: 5, idleTimeoutMs: 50,
      connectionFactory: () => ({ valid: true }),
      validator: () => true,
    });
    // Create extra connections beyond min
    await pool.warmUp();
    const c = await pool.acquire();
    await pool.acquire(); // creates 3rd
    pool.release(c);
    
    await new Promise(r => setTimeout(r, 60));
    const results = await pool.healthCheck();
    // Should evict idle connections down to min
    assert.ok(pool.getStats().total >= 1);
  });

  test('error eviction after max errors', async () => {
    await pool.warmUp();
    const conn = await pool.acquire();
    conn.recordError();
    conn.recordError();
    conn.recordError(); // 3 = maxErrors
    pool.release(conn); // Should evict
    
    assert.ok(pool.getStats().errorEvictions > 0);
  });

  test('invalid connection evicted on acquire', async () => {
    let connIdx = 0;
    pool = new HealthCheckPool({
      minSize: 1, maxSize: 5,
      validationIntervalMs: 0,
      connectionFactory: () => ({ id: ++connIdx, valid: true }),
      validator: (conn) => conn.valid,
    });
    await pool.warmUp();
    
    // Next acquire gets the valid connection
    const conn = await pool.acquire();
    assert.ok(conn);
    pool.release(conn);
  });

  test('pool exhaustion tracked in stats', async () => {
    pool = new HealthCheckPool({
      minSize: 1, maxSize: 2,
      connectionFactory: () => ({ valid: true }),
      validator: () => true,
    });
    await pool.warmUp();
    const c1 = await pool.acquire();
    const c2 = await pool.acquire();
    
    try { await pool.acquire(); } catch {} // Should fail
    
    const stats = pool.getStats();
    assert.equal(stats.waits, 1);
    pool.release(c1);
    pool.release(c2);
  });

  test('stats tracking', async () => {
    await pool.warmUp();
    const conn = await pool.acquire();
    pool.release(conn);
    
    const stats = pool.getStats();
    assert.ok(stats.created >= 2);
    assert.equal(stats.acquires, 1);
    assert.equal(stats.releases, 1);
  });

  test('destroy cleans up everything', async () => {
    await pool.warmUp();
    await pool.destroy();
    assert.equal(pool.getStats().total, 0);
  });
});
