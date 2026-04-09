// lock-manager.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LockManager, SHARED, EXCLUSIVE } from './lock-manager.js';

describe('LockManager', () => {
  it('exclusive lock blocks others', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'row-1', EXCLUSIVE), true);
    assert.equal(lm.acquire(2, 'row-1', EXCLUSIVE), false); // Blocked
  });

  it('shared locks are compatible', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'row-1', SHARED), true);
    assert.equal(lm.acquire(2, 'row-1', SHARED), true); // Both can read
  });

  it('shared blocks exclusive', () => {
    const lm = new LockManager();
    lm.acquire(1, 'row-1', SHARED);
    assert.equal(lm.acquire(2, 'row-1', EXCLUSIVE), false);
  });

  it('release frees lock', () => {
    const lm = new LockManager();
    lm.acquire(1, 'row-1', EXCLUSIVE);
    lm.release(1);
    assert.equal(lm.acquire(2, 'row-1', EXCLUSIVE), true); // Now free
  });

  it('2PL: all locks then release all', () => {
    const lm = new LockManager();
    // Growing phase
    lm.acquire(1, 'A', EXCLUSIVE);
    lm.acquire(1, 'B', EXCLUSIVE);
    lm.acquire(1, 'C', EXCLUSIVE);
    // Shrinking phase
    lm.release(1);
    
    // All resources now available
    assert.equal(lm.acquire(2, 'A', EXCLUSIVE), true);
    assert.equal(lm.acquire(2, 'B', EXCLUSIVE), true);
  });

  it('lock upgrade: S → X when only holder', () => {
    const lm = new LockManager();
    lm.acquire(1, 'row-1', SHARED);
    assert.equal(lm.acquire(1, 'row-1', EXCLUSIVE), true); // Upgrade
  });
});
