// advisory-locks.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { AdvisoryLockManager } from './advisory-locks.js';

let lm;

describe('AdvisoryLockManager', () => {
  beforeEach(() => {
    lm = new AdvisoryLockManager();
  });

  test('acquire exclusive lock', () => {
    const ok = lm.tryLock('s1', 100);
    assert.ok(ok);
    assert.ok(lm.isLocked(100));
    assert.ok(lm.isLockedBy('s1', 100));
  });

  test('exclusive lock blocks other sessions', () => {
    lm.tryLock('s1', 100);
    const ok = lm.tryLock('s2', 100);
    assert.ok(!ok);
  });

  test('shared locks allow multiple holders', () => {
    lm.tryLock('s1', 100, { mode: 'shared' });
    const ok = lm.tryLock('s2', 100, { mode: 'shared' });
    assert.ok(ok);
    assert.ok(lm.isLockedBy('s1', 100));
    assert.ok(lm.isLockedBy('s2', 100));
  });

  test('exclusive blocks shared', () => {
    lm.tryLock('s1', 100, { mode: 'exclusive' });
    const ok = lm.tryLock('s2', 100, { mode: 'shared' });
    assert.ok(!ok);
  });

  test('recursive locking (same session)', () => {
    lm.tryLock('s1', 100);
    const ok = lm.tryLock('s1', 100); // Re-acquire
    assert.ok(ok);
    
    lm.unlock('s1', 100); // Release once
    assert.ok(lm.isLockedBy('s1', 100)); // Still held (count > 0)
    
    lm.unlock('s1', 100); // Release fully
    assert.ok(!lm.isLockedBy('s1', 100));
  });

  test('unlock releases lock', () => {
    lm.tryLock('s1', 100);
    assert.ok(lm.isLocked(100));
    
    lm.unlock('s1', 100);
    assert.ok(!lm.isLocked(100));
  });

  test('unlock returns false for non-held lock', () => {
    assert.ok(!lm.unlock('s1', 999));
  });

  test('releaseSession cleans up all locks', () => {
    lm.tryLock('s1', 100);
    lm.tryLock('s1', 200);
    lm.tryLock('s1', 300);
    
    const released = lm.releaseSession('s1');
    assert.equal(released, 3);
    assert.ok(!lm.isLocked(100));
    assert.ok(!lm.isLocked(200));
    assert.ok(!lm.isLocked(300));
  });

  test('releaseTransaction only releases tx-level locks', () => {
    lm.tryLock('s1', 100, { level: 'session' });
    lm.tryLock('s1', 200, { level: 'transaction' });
    
    const released = lm.releaseTransaction('s1');
    assert.equal(released, 1);
    assert.ok(lm.isLockedBy('s1', 100)); // Session lock still held
    assert.ok(!lm.isLockedBy('s1', 200)); // Tx lock released
  });

  test('async lock blocks and then succeeds', async () => {
    lm.tryLock('s1', 100);
    
    // Start waiting for lock
    const lockPromise = lm.lock('s2', 100, { timeoutMs: 1000 });
    
    // Release after delay
    setTimeout(() => lm.unlock('s1', 100), 20);
    
    const ok = await lockPromise;
    assert.ok(ok);
    assert.ok(lm.isLockedBy('s2', 100));
    lm.unlock('s2', 100);
  });

  test('async lock times out', async () => {
    lm.tryLock('s1', 100);
    
    await assert.rejects(
      () => lm.lock('s2', 100, { timeoutMs: 50 }),
      /timeout/
    );
    lm.unlock('s1', 100);
  });

  test('array key support', () => {
    lm.tryLock('s1', [1, 2]);
    assert.ok(lm.isLocked([1, 2]));
    assert.ok(!lm.isLocked([1, 3]));
    lm.unlock('s1', [1, 2]);
  });

  test('listLocks shows all active locks', () => {
    lm.tryLock('s1', 100);
    lm.tryLock('s2', 200);
    
    const locks = lm.listLocks();
    assert.equal(locks.length, 2);
    assert.ok(locks.some(l => l.sessionId === 's1'));
    assert.ok(locks.some(l => l.sessionId === 's2'));
  });

  test('stats tracking', () => {
    lm.tryLock('s1', 100);
    lm.tryLock('s2', 100); // fail
    lm.unlock('s1', 100);
    
    const stats = lm.getStats();
    assert.equal(stats.acquired, 1);
    assert.equal(stats.released, 1);
    assert.equal(stats.tryFailed, 1);
  });

  test('different keys are independent', () => {
    lm.tryLock('s1', 100);
    assert.ok(lm.tryLock('s2', 200)); // Different key
    lm.unlock('s1', 100);
    lm.unlock('s2', 200);
  });
});
