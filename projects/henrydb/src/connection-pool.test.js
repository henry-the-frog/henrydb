// connection-pool.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ConnectionPool } from './connection-pool.js';

describe('ConnectionPool', () => {
  it('acquire and release', async () => {
    const pool = new ConnectionPool({ maxSize: 3 });
    const conn = await pool.acquire();
    assert.ok(conn);
    const stats = pool.getStats();
    assert.equal(stats.inUse, 1);
    assert.equal(stats.idle, 0);
    await pool.release(conn);
    const after = pool.getStats();
    assert.equal(after.inUse, 0);
    assert.equal(after.idle, 1);
    await pool.close();
  });

  it('respects maxSize', async () => {
    const pool = new ConnectionPool({ maxSize: 2, acquireTimeoutMs: 100 });
    const c1 = await pool.acquire();
    const c2 = await pool.acquire();
    assert.ok(c1 && c2);
    // Third acquire should timeout
    await assert.rejects(() => pool.acquire(), /timeout/i);
    const stats = pool.getStats();
    assert.equal(stats.timeoutCount, 1);
    await pool.release(c1);
    await pool.release(c2);
    await pool.close();
  });

  it('waiters get connections when released', async () => {
    const pool = new ConnectionPool({ maxSize: 1, acquireTimeoutMs: 2000 });
    const c1 = await pool.acquire();
    // Start waiting for a connection
    const pending = pool.acquire();
    // Release after a short delay
    setTimeout(() => pool.release(c1), 50);
    const c2 = await pending;
    assert.ok(c2);
    assert.equal(c1, c2); // Same connection reused
    await pool.release(c2);
    await pool.close();
  });

  it('custom factory and destroy', async () => {
    let created = 0, destroyed = 0;
    const pool = new ConnectionPool({
      maxSize: 3,
      factory: () => ({ id: ++created }),
      destroy: () => { destroyed++; },
    });
    const c1 = await pool.acquire();
    const c2 = await pool.acquire();
    assert.equal(created, 2);
    await pool.release(c1);
    await pool.close();
    assert.ok(destroyed >= 2);
  });

  it('validates connections on acquire', async () => {
    let healthy = true;
    const pool = new ConnectionPool({
      maxSize: 5,
      validate: (conn) => healthy,
    });
    const c1 = await pool.acquire();
    await pool.release(c1);
    // Mark unhealthy — next acquire should skip the idle conn and create new
    healthy = false;
    const c2 = await pool.acquire();
    assert.notEqual(c1, c2);
    await pool.release(c2, true);
    await pool.close();
  });

  it('double release is safe', async () => {
    const pool = new ConnectionPool({ maxSize: 3 });
    const conn = await pool.acquire();
    await pool.release(conn);
    await pool.release(conn); // Should not throw or add duplicate
    assert.equal(pool.getStats().idle, 1);
    await pool.close();
  });

  it('close rejects pending waiters', async () => {
    const pool = new ConnectionPool({ maxSize: 1, acquireTimeoutMs: 5000 });
    const c1 = await pool.acquire();
    const pending = pool.acquire();
    await pool.close();
    await assert.rejects(pending, /closed/i);
  });

  it('acquire after close throws', async () => {
    const pool = new ConnectionPool({ maxSize: 3 });
    await pool.close();
    await assert.rejects(() => pool.acquire(), /closed/i);
  });

  it('tracks metrics', async () => {
    const pool = new ConnectionPool({ maxSize: 2, acquireTimeoutMs: 50 });
    const c1 = await pool.acquire();
    const c2 = await pool.acquire();
    await pool.release(c1);
    await pool.release(c2);
    // Try to trigger a timeout
    const c3 = await pool.acquire();
    const c4 = await pool.acquire();
    await assert.rejects(() => pool.acquire(), /timeout/i);
    const stats = pool.getStats();
    assert.equal(stats.acquireCount, 4);
    assert.equal(stats.releaseCount, 2);
    assert.equal(stats.timeoutCount, 1);
    await pool.release(c3);
    await pool.release(c4);
    await pool.close();
  });

  it('max lifetime expires connections', async () => {
    let created = 0;
    const pool = new ConnectionPool({
      maxSize: 5,
      maxLifetimeMs: 50,
      factory: () => ({ id: ++created }),
      idleTimeoutMs: 0,
    });
    const c1 = await pool.acquire();
    await pool.release(c1);
    // Wait for lifetime to expire
    await new Promise(r => setTimeout(r, 100));
    const c2 = await pool.acquire();
    assert.notEqual(c1.id, c2.id); // New connection created
    await pool.release(c2);
    await pool.close();
  });

  it('concurrent acquire stress', async () => {
    const pool = new ConnectionPool({ maxSize: 5, acquireTimeoutMs: 2000 });
    const work = async () => {
      const conn = await pool.acquire();
      await new Promise(r => setTimeout(r, Math.random() * 20));
      await pool.release(conn);
    };
    // 20 concurrent workers, 5 pool size
    await Promise.all(Array.from({ length: 20 }, () => work()));
    const stats = pool.getStats();
    assert.equal(stats.acquireCount, 20);
    assert.equal(stats.releaseCount, 20);
    assert.equal(stats.inUse, 0);
    await pool.close();
  });
});
