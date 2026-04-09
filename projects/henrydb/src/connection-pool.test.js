// connection-pool.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ConnectionPool } from './connection-pool.js';

describe('ConnectionPool', () => {
  it('acquire and release', () => {
    const pool = new ConnectionPool(3);
    const c1 = pool.acquire();
    const c2 = pool.acquire();
    assert.ok(c1); assert.ok(c2);
    assert.equal(pool.getStats().available, 1);
    pool.release(c1);
    assert.equal(pool.getStats().available, 2);
  });

  it('returns null when exhausted', () => {
    const pool = new ConnectionPool(1);
    pool.acquire();
    assert.equal(pool.acquire(), null);
  });

  it('reuse released connections', () => {
    const pool = new ConnectionPool(1);
    const c1 = pool.acquire();
    pool.release(c1);
    const c2 = pool.acquire();
    assert.equal(c1, c2); // Same connection reused
  });
});
