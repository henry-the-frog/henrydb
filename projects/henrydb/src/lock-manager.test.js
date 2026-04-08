// lock-manager.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LockManager } from './lock-manager.js';

describe('LockManager', () => {
  it('grant exclusive lock', async () => {
    const lm = new LockManager();
    assert.ok(await lm.acquire('T1', 'table1', 'X'));
    assert.ok(lm.isHeldBy('T1', 'table1'));
    assert.equal(lm.getLockMode('T1', 'table1'), 'X');
  });

  it('multiple shared locks are compatible', async () => {
    const lm = new LockManager();
    assert.ok(await lm.acquire('T1', 'row1', 'S'));
    assert.ok(await lm.acquire('T2', 'row1', 'S'));
    assert.ok(lm.isHeldBy('T1', 'row1'));
    assert.ok(lm.isHeldBy('T2', 'row1'));
  });

  it('exclusive blocks shared', async () => {
    const lm = new LockManager();
    await lm.acquire('T1', 'row1', 'X');
    assert.ok(!lm.tryAcquire('T2', 'row1', 'S'));
  });

  it('shared blocks exclusive', async () => {
    const lm = new LockManager();
    await lm.acquire('T1', 'row1', 'S');
    assert.ok(!lm.tryAcquire('T2', 'row1', 'X'));
  });

  it('intention locks are compatible', async () => {
    const lm = new LockManager();
    assert.ok(await lm.acquire('T1', 'db', 'IS'));
    assert.ok(await lm.acquire('T2', 'db', 'IX'));
    assert.ok(await lm.acquire('T3', 'db', 'IS'));
  });

  it('release grants waiters', async () => {
    const lm = new LockManager();
    await lm.acquire('T1', 'row1', 'X');
    
    let granted = false;
    const waitPromise = lm.acquire('T2', 'row1', 'S').then(() => { granted = true; });
    
    assert.ok(!granted);
    lm.release('T1', 'row1');
    await waitPromise;
    assert.ok(granted);
  });

  it('releaseAll releases everything', async () => {
    const lm = new LockManager();
    await lm.acquire('T1', 'r1', 'X');
    await lm.acquire('T1', 'r2', 'S');
    await lm.acquire('T1', 'r3', 'IX');
    
    lm.releaseAll('T1');
    assert.ok(!lm.isHeldBy('T1', 'r1'));
    assert.ok(!lm.isHeldBy('T1', 'r2'));
    assert.ok(!lm.isHeldBy('T1', 'r3'));
  });

  it('lock upgrade: S → X', async () => {
    const lm = new LockManager();
    await lm.acquire('T1', 'row1', 'S');
    assert.ok(await lm.acquire('T1', 'row1', 'X')); // Upgrade
    assert.equal(lm.getLockMode('T1', 'row1'), 'X');
  });

  it('idempotent: re-acquiring same lock', async () => {
    const lm = new LockManager();
    await lm.acquire('T1', 'row1', 'X');
    assert.ok(await lm.acquire('T1', 'row1', 'X'));
  });

  it('stats tracking', async () => {
    const lm = new LockManager();
    await lm.acquire('T1', 'r1', 'X');
    await lm.acquire('T2', 'r2', 'S');
    lm.release('T1', 'r1');
    
    const stats = lm.getStats();
    assert.equal(stats.grants, 2);
    assert.equal(stats.releases, 1);
  });

  it('SIX compatibility', async () => {
    const lm = new LockManager();
    await lm.acquire('T1', 'table', 'SIX');
    // IS is compatible with SIX
    assert.ok(lm.tryAcquire('T2', 'table', 'IS'));
    // S is NOT compatible with SIX
    assert.ok(!lm.tryAcquire('T3', 'table', 'S'));
    // IX is NOT compatible with SIX
    assert.ok(!lm.tryAcquire('T4', 'table', 'IX'));
  });
});
