// two-phase-locking.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TwoPhaseLocking } from './two-phase-locking.js';

describe('TwoPhaseLocking', () => {
  it('basic lock and commit', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    assert.ok(tpl.lockExclusive('T1', 'users', 1));
    assert.ok(tpl.holdsLock('T1', 'users', 1));
    tpl.commit('T1');
    assert.ok(!tpl.holdsLock('T1', 'users', 1));
  });

  it('shared locks are compatible', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    tpl.begin('T2');
    assert.ok(tpl.lockShared('T1', 'users', 1));
    assert.ok(tpl.lockShared('T2', 'users', 1)); // Compatible
  });

  it('exclusive blocks other exclusive', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    tpl.begin('T2');
    assert.ok(tpl.lockExclusive('T1', 'users', 1));
    assert.ok(!tpl.lockExclusive('T2', 'users', 1)); // Blocked
  });

  it('exclusive blocks shared', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    tpl.begin('T2');
    assert.ok(tpl.lockExclusive('T1', 'users', 1));
    assert.ok(!tpl.lockShared('T2', 'users', 1));
  });

  it('shared blocks exclusive', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    tpl.begin('T2');
    assert.ok(tpl.lockShared('T1', 'users', 1));
    assert.ok(!tpl.lockExclusive('T2', 'users', 1));
  });

  it('2PL violation: cannot acquire after release', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    tpl.lockExclusive('T1', 'users', 1);
    tpl.commit('T1'); // Enters shrinking phase
    
    // T1 no longer exists, so it throws
    assert.throws(() => tpl.lockShared('T1', 'users', 2), /not found/);
  });

  it('lock upgrade: S → X when sole holder', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    assert.ok(tpl.lockShared('T1', 'users', 1));
    assert.ok(tpl.lockExclusive('T1', 'users', 1)); // Upgrade
  });

  it('abort releases all locks', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    tpl.lockExclusive('T1', 'users', 1);
    tpl.lockExclusive('T1', 'users', 2);
    tpl.abort('T1');
    
    // Locks should be released
    tpl.begin('T2');
    assert.ok(tpl.lockExclusive('T2', 'users', 1));
    assert.ok(tpl.lockExclusive('T2', 'users', 2));
  });

  it('stats tracking', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    tpl.begin('T2');
    tpl.lockExclusive('T1', 'users', 1);
    tpl.lockExclusive('T2', 'users', 1); // Blocked
    tpl.abort('T2');
    
    assert.equal(tpl.stats.grants, 1);
    assert.equal(tpl.stats.blocks, 1);
    assert.equal(tpl.stats.aborts, 1);
  });

  it('multiple rows locked by same txn', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    for (let i = 0; i < 100; i++) {
      assert.ok(tpl.lockExclusive('T1', 'orders', i));
    }
    assert.equal(tpl.getStats().activeLocks, 100);
    tpl.commit('T1');
    assert.equal(tpl.getStats().activeLocks, 0);
  });
});
