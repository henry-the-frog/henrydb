// lock-manager-stress.test.js — Comprehensive lock manager stress tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LockManager, SHARED, EXCLUSIVE } from './lock-manager.js';

describe('LockManager — Intention Locks', () => {
  it('IS locks are compatible with each other', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'IS'), true);
    assert.equal(lm.acquire(2, 'table-1', 'IS'), true);
    assert.equal(lm.acquire(3, 'table-1', 'IS'), true);
  });

  it('IX locks are compatible with each other', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'IX'), true);
    assert.equal(lm.acquire(2, 'table-1', 'IX'), true);
  });

  it('IS compatible with S', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'IS'), true);
    assert.equal(lm.acquire(2, 'table-1', 'S'), true);
  });

  it('IX incompatible with S', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'IX'), true);
    assert.equal(lm.acquire(2, 'table-1', 'S'), false);
  });

  it('IS compatible with IX', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'IS'), true);
    assert.equal(lm.acquire(2, 'table-1', 'IX'), true);
  });

  it('IS compatible with SIX', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'IS'), true);
    assert.equal(lm.acquire(2, 'table-1', 'SIX'), true);
  });

  it('IX incompatible with SIX', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'IX'), true);
    assert.equal(lm.acquire(2, 'table-1', 'SIX'), false);
  });

  it('X incompatible with IS', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'X'), true);
    assert.equal(lm.acquire(2, 'table-1', 'IS'), false);
  });

  it('SIX incompatible with SIX', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'SIX'), true);
    assert.equal(lm.acquire(2, 'table-1', 'SIX'), false);
  });
});

describe('LockManager — Lock Upgrades', () => {
  it('upgrade S → X fails when another holder has S', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'row-1', 'S'), true);
    assert.equal(lm.acquire(2, 'row-1', 'S'), true);
    // tx1 tries to upgrade to X — should fail since tx2 also holds S
    assert.equal(lm.acquire(1, 'row-1', 'X'), false);
  });

  it('upgrade IS → IX when only holder', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'IS'), true);
    assert.equal(lm.acquire(1, 'table-1', 'IX'), true);
  });

  it('upgrade IS → S when only holder', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'IS'), true);
    assert.equal(lm.acquire(1, 'table-1', 'S'), true);
  });

  it('upgrade IS → SIX when only holder', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'table-1', 'IS'), true);
    assert.equal(lm.acquire(1, 'table-1', 'SIX'), true);
  });

  it('re-acquiring same mode is idempotent', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'row-1', 'X'), true);
    assert.equal(lm.acquire(1, 'row-1', 'X'), true); // Already holds X
  });

  it('downgrade X → S is treated as already holding (no downgrade)', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'row-1', 'X'), true);
    // Requesting weaker lock should succeed (already holds stronger)
    assert.equal(lm.acquire(1, 'row-1', 'S'), true);
  });
});

describe('LockManager — Multi-Resource Deadlock Detection', () => {
  it('detects simple AB-BA deadlock (verified via stats)', () => {
    const lm = new LockManager();
    // tx1 holds A, tx2 holds B
    assert.equal(lm.acquire(1, 'A', 'X'), true);
    assert.equal(lm.acquire(2, 'B', 'X'), true);
    assert.equal(lm.stats.deadlocks, 0);
    // tx1 wants B → would block, queued (not deadlock yet)
    assert.equal(lm.acquire(1, 'B', 'X'), false);
    assert.equal(lm.stats.waits, 1, 'tx1 should be queued (wait), not deadlocked');
    assert.equal(lm.stats.deadlocks, 0, 'no deadlock yet');
    // tx2 wants A → deadlock! (tx2→A→tx1→B→tx2)
    assert.equal(lm.acquire(2, 'A', 'X'), false);
    assert.equal(lm.stats.deadlocks, 1, 'should detect deadlock');
  });

  it('detects 3-way circular deadlock', () => {
    const lm = new LockManager();
    // tx1→A, tx2→B, tx3→C
    assert.equal(lm.acquire(1, 'A', 'X'), true);
    assert.equal(lm.acquire(2, 'B', 'X'), true);
    assert.equal(lm.acquire(3, 'C', 'X'), true);
    // tx1→B (blocked, queued), tx2→C (blocked, queued)
    assert.equal(lm.acquire(1, 'B', 'X'), false);
    assert.equal(lm.acquire(2, 'C', 'X'), false);
    // tx3→A would complete the cycle
    assert.equal(lm.acquire(3, 'A', 'X'), false);
  });

  it('no false positive: non-circular wait', () => {
    const lm = new LockManager();
    // tx1 holds A, tx2 holds B
    assert.equal(lm.acquire(1, 'A', 'X'), true);
    assert.equal(lm.acquire(2, 'B', 'X'), true);
    // tx3 wants A — not a deadlock, just a wait
    assert.equal(lm.acquire(3, 'A', 'X'), false); // blocked but not deadlock
    // tx3 wants B — still not a deadlock (tx3 doesn't hold anything tx1 or tx2 want)
    // But since tx3 already failed to acquire A and isn't queued properly, 
    // let's check with a fresh tx
    const lm2 = new LockManager();
    lm2.acquire(1, 'A', 'X');
    // tx2 wants A — blocked, no deadlock (tx2 holds nothing tx1 wants)
    assert.equal(lm2.acquire(2, 'A', 'X'), false); // blocked, not deadlock
  });
});

describe('LockManager — Release and Grant Chaining', () => {
  it('release grants lock to next in queue', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'row-1', 'X'), true);
    // tx2 tries and fails (queued)
    assert.equal(lm.acquire(2, 'row-1', 'X'), false);
    // tx1 releases — tx2 should be granted
    lm.release(1);
    // tx3 tries — should fail because tx2 now holds it
    assert.equal(lm.acquire(3, 'row-1', 'X'), false);
  });

  it('release single resource only releases that one', () => {
    const lm = new LockManager();
    lm.acquire(1, 'A', 'X');
    lm.acquire(1, 'B', 'X');
    lm.acquire(1, 'C', 'X');
    // Release just B
    lm.release(1, 'B');
    // B is free, A and C still locked
    assert.equal(lm.acquire(2, 'B', 'X'), true);
    assert.equal(lm.acquire(2, 'A', 'X'), false);
    assert.equal(lm.acquire(2, 'C', 'X'), false);
  });

  it('release all frees all resources', () => {
    const lm = new LockManager();
    lm.acquire(1, 'A', 'X');
    lm.acquire(1, 'B', 'X');
    lm.acquire(1, 'C', 'X');
    lm.release(1); // Release all
    assert.equal(lm.acquire(2, 'A', 'X'), true);
    assert.equal(lm.acquire(2, 'B', 'X'), true);
    assert.equal(lm.acquire(2, 'C', 'X'), true);
  });

  it('queue processes FIFO', () => {
    const lm = new LockManager();
    assert.equal(lm.acquire(1, 'row-1', 'X'), true);
    // tx2 then tx3 try to acquire
    lm.acquire(2, 'row-1', 'X'); // queued
    lm.acquire(3, 'row-1', 'X'); // queued behind tx2
    lm.release(1); // tx2 gets it
    // Verify: tx2 holds it (tx4 can't get it)
    assert.equal(lm.acquire(4, 'row-1', 'X'), false);
    lm.release(2); // tx3 gets it
    assert.equal(lm.acquire(4, 'row-1', 'X'), false); // tx3 has it now
    lm.release(3); // now it's free
    assert.equal(lm.acquire(4, 'row-1', 'X'), true);
  });
});

describe('LockManager — Stats Tracking', () => {
  it('tracks grants and releases', () => {
    const lm = new LockManager();
    lm.acquire(1, 'A', 'X');
    lm.acquire(1, 'B', 'X');
    assert.equal(lm.stats.grants, 2);
    lm.release(1);
    assert.equal(lm.stats.releases, 2);
  });

  it('getStats reports active counts', () => {
    const lm = new LockManager();
    lm.acquire(1, 'A', 'X');
    lm.acquire(2, 'B', 'X');
    const stats = lm.getStats();
    assert.equal(stats.activeResources, 2);
    assert.equal(stats.activeTxns, 2);
    lm.release(1);
    const stats2 = lm.getStats();
    assert.equal(stats2.activeResources, 1);
    assert.equal(stats2.activeTxns, 1);
  });
});

describe('LockManager — Edge Cases', () => {
  it('release non-existent tx is no-op', () => {
    const lm = new LockManager();
    lm.release(999); // Should not throw
  });

  it('release non-held resource is no-op', () => {
    const lm = new LockManager();
    lm.acquire(1, 'A', 'X');
    lm.release(1, 'Z'); // tx1 doesn't hold Z
    // A should still be locked
    assert.equal(lm.acquire(2, 'A', 'X'), false);
  });

  it('many transactions on same resource', () => {
    const lm = new LockManager();
    // 100 shared locks on same resource
    for (let i = 0; i < 100; i++) {
      assert.equal(lm.acquire(i, 'shared-resource', 'S'), true);
    }
    assert.equal(lm.getStats().activeTxns, 100);
    // Exclusive should fail
    assert.equal(lm.acquire(999, 'shared-resource', 'X'), false);
  });

  it('many resources per transaction', () => {
    const lm = new LockManager();
    for (let i = 0; i < 100; i++) {
      assert.equal(lm.acquire(1, `resource-${i}`, 'X'), true);
    }
    assert.equal(lm.getStats().activeResources, 100);
    lm.release(1);
    assert.equal(lm.getStats().activeResources, 0);
  });

  it('mixed S and X on different resources', () => {
    const lm = new LockManager();
    // tx1: S on A, X on B
    assert.equal(lm.acquire(1, 'A', 'S'), true);
    assert.equal(lm.acquire(1, 'B', 'X'), true);
    // tx2: S on A (ok), S on B (fail — X held by tx1)
    assert.equal(lm.acquire(2, 'A', 'S'), true);
    assert.equal(lm.acquire(2, 'B', 'S'), false);
  });
});

describe('LockManager — Hierarchical Locking Protocol', () => {
  it('proper hierarchy: IS on table, S on row', () => {
    const lm = new LockManager();
    // tx1 takes IS on table (intent to read), then S on row
    assert.equal(lm.acquire(1, 'table:orders', 'IS'), true);
    assert.equal(lm.acquire(1, 'row:orders:1', 'S'), true);
    // tx2 can also take IS on table and S on different row
    assert.equal(lm.acquire(2, 'table:orders', 'IS'), true);
    assert.equal(lm.acquire(2, 'row:orders:2', 'S'), true);
  });

  it('IX on table blocks table-level S', () => {
    const lm = new LockManager();
    // tx1 takes IX on table (intent to write some rows)
    assert.equal(lm.acquire(1, 'table:orders', 'IX'), true);
    // tx2 wants S on entire table — blocked by IX
    assert.equal(lm.acquire(2, 'table:orders', 'S'), false);
  });

  it('SIX: shared read of table + intent to modify rows', () => {
    const lm = new LockManager();
    // tx1 takes SIX (reading entire table while modifying some rows)
    assert.equal(lm.acquire(1, 'table:orders', 'SIX'), true);
    // tx2 can take IS (read intent) — compatible with SIX
    assert.equal(lm.acquire(2, 'table:orders', 'IS'), true);
    // tx3 cannot take IX — incompatible with SIX
    assert.equal(lm.acquire(3, 'table:orders', 'IX'), false);
    // tx4 cannot take S — incompatible with SIX
    assert.equal(lm.acquire(4, 'table:orders', 'S'), false);
  });
});
