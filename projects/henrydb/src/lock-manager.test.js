// lock-manager.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LockManager, LockMode } from './lock-manager.js';

describe('Lock Manager', () => {
  it('grants exclusive lock on free resource', () => {
    const lm = new LockManager();
    assert.equal(lm.lock(1, 'row:1', LockMode.EXCLUSIVE), true);
  });

  it('grants shared lock on free resource', () => {
    const lm = new LockManager();
    assert.equal(lm.lock(1, 'row:1', LockMode.SHARED), true);
  });

  it('multiple shared locks are compatible', () => {
    const lm = new LockManager();
    assert.equal(lm.lock(1, 'row:1', LockMode.SHARED), true);
    assert.equal(lm.lock(2, 'row:1', LockMode.SHARED), true);
  });

  it('exclusive lock conflicts with shared lock', () => {
    const lm = new LockManager();
    lm.lock(1, 'row:1', LockMode.SHARED);
    assert.equal(lm.lock(2, 'row:1', LockMode.EXCLUSIVE), false); // Cannot grant
  });

  it('exclusive lock conflicts with exclusive lock', () => {
    const lm = new LockManager();
    lm.lock(1, 'row:1', LockMode.EXCLUSIVE);
    assert.equal(lm.lock(2, 'row:1', LockMode.EXCLUSIVE), false);
  });

  it('unlock releases locks and grants waiting', () => {
    const lm = new LockManager();
    lm.lock(1, 'row:1', LockMode.EXCLUSIVE);
    lm.lock(2, 'row:1', LockMode.EXCLUSIVE); // Queued
    
    lm.unlock(1); // Release tx 1's lock
    
    // Now tx 2's waiting lock should be granted
    const state = lm.state();
    const row1Locks = state.locks['row:1'];
    assert.ok(row1Locks.some(l => l.tx === 2 && l.granted));
  });

  it('detects simple deadlock', () => {
    const lm = new LockManager();
    
    // Tx 1 holds row:1, wants row:2
    lm.lock(1, 'row:1', LockMode.EXCLUSIVE);
    // Tx 2 holds row:2, wants row:1
    lm.lock(2, 'row:2', LockMode.EXCLUSIVE);
    
    // Tx 1 tries to get row:2 (held by tx 2)
    lm.lock(1, 'row:2', LockMode.EXCLUSIVE); // Queued, tx 1 waits for tx 2
    
    // Tx 2 tries to get row:1 (held by tx 1) → DEADLOCK!
    assert.throws(() => {
      lm.lock(2, 'row:1', LockMode.EXCLUSIVE);
    }, /Deadlock detected/);
  });

  it('same tx can re-acquire its own lock', () => {
    const lm = new LockManager();
    lm.lock(1, 'row:1', LockMode.EXCLUSIVE);
    assert.equal(lm.lock(1, 'row:1', LockMode.EXCLUSIVE), true); // Re-entrant
  });

  it('lock different resources independently', () => {
    const lm = new LockManager();
    assert.equal(lm.lock(1, 'row:1', LockMode.EXCLUSIVE), true);
    assert.equal(lm.lock(2, 'row:2', LockMode.EXCLUSIVE), true);
  });

  it('unlock releases all locks for a transaction', () => {
    const lm = new LockManager();
    lm.lock(1, 'row:1', LockMode.EXCLUSIVE);
    lm.lock(1, 'row:2', LockMode.EXCLUSIVE);
    lm.lock(1, 'row:3', LockMode.EXCLUSIVE);
    
    lm.unlock(1);
    
    // All resources should be available
    assert.equal(lm.lock(2, 'row:1', LockMode.EXCLUSIVE), true);
    assert.equal(lm.lock(2, 'row:2', LockMode.EXCLUSIVE), true);
  });
});
