// object-pool.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ObjectPool } from './object-pool.js';

describe('ObjectPool', () => {
  it('acquire and release', () => {
    const pool = new ObjectPool(() => ({ buf: Buffer.alloc(1024) }), null, 5);
    const obj = pool.acquire();
    assert.ok(obj.buf);
    pool.release(obj);
    assert.equal(pool.available, 5); // Back in pool
  });

  it('reuses objects', () => {
    const pool = new ObjectPool(() => ({}), null, 1);
    const a = pool.acquire();
    pool.release(a);
    const b = pool.acquire();
    assert.equal(a, b); // Same object
  });
});
