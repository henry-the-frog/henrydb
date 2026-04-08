// concurrency2.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { IntentLockManager, GapLockManager, RCU, GroupCommitWAL } from './concurrency2.js';

describe('IntentLockManager', () => {
  it('IS compatible with IS', () => {
    const lm = new IntentLockManager();
    assert.equal(lm.lock('t1', 1, 'IS').granted, true);
    assert.equal(lm.lock('t1', 2, 'IS').granted, true);
  });

  it('S compatible with S', () => {
    const lm = new IntentLockManager();
    assert.equal(lm.lock('r1', 1, 'S').granted, true);
    assert.equal(lm.lock('r1', 2, 'S').granted, true);
  });

  it('X conflicts with S', () => {
    const lm = new IntentLockManager();
    lm.lock('r1', 1, 'S');
    assert.equal(lm.lock('r1', 2, 'X').granted, false);
  });

  it('IX conflicts with S', () => {
    const lm = new IntentLockManager();
    lm.lock('r1', 1, 'S');
    assert.equal(lm.lock('r1', 2, 'IX').granted, false);
  });

  it('unlock releases lock', () => {
    const lm = new IntentLockManager();
    lm.lock('r1', 1, 'X');
    lm.unlock('r1', 1);
    assert.equal(lm.lock('r1', 2, 'X').granted, true);
  });

  it('unlockAll', () => {
    const lm = new IntentLockManager();
    lm.lock('r1', 1, 'X');
    lm.lock('r2', 1, 'X');
    lm.unlockAll(1);
    assert.equal(lm.lock('r1', 2, 'X').granted, true);
    assert.equal(lm.lock('r2', 2, 'X').granted, true);
  });
});

describe('GapLockManager', () => {
  it('shared gap locks compatible', () => {
    const gm = new GapLockManager();
    assert.equal(gm.lockGap(1, 10, 20, 'S').granted, true);
    assert.equal(gm.lockGap(2, 15, 25, 'S').granted, true);
  });

  it('exclusive gap conflict', () => {
    const gm = new GapLockManager();
    gm.lockGap(1, 10, 20, 'X');
    assert.equal(gm.lockGap(2, 15, 25, 'S').granted, false);
  });

  it('non-overlapping gaps OK', () => {
    const gm = new GapLockManager();
    gm.lockGap(1, 10, 20, 'X');
    assert.equal(gm.lockGap(2, 30, 40, 'X').granted, true);
  });

  it('unlockAll', () => {
    const gm = new GapLockManager();
    gm.lockGap(1, 10, 20, 'X');
    gm.unlockAll(1);
    assert.equal(gm.lockGap(2, 10, 20, 'X').granted, true);
  });
});

describe('RCU', () => {
  it('publish and read', () => {
    const rcu = new RCU();
    rcu.publish({ count: 1 });
    const data = rcu.readLock('r1');
    assert.deepEqual(data, { count: 1 });
    rcu.readUnlock('r1');
  });

  it('readers see latest version', () => {
    const rcu = new RCU();
    rcu.publish({ v: 1 });
    rcu.publish({ v: 2 });
    assert.deepEqual(rcu.readLock('r1'), { v: 2 });
    rcu.readUnlock('r1');
  });

  it('canReclaim checks active readers', () => {
    const rcu = new RCU();
    rcu.publish('v1');
    rcu.readLock('r1');
    rcu.publish('v2');
    assert.equal(rcu.canReclaim(1), false); // r1 still reading v1
    rcu.readUnlock('r1');
    assert.equal(rcu.canReclaim(1), true);
  });

  it('synchronize', () => {
    const rcu = new RCU();
    assert.equal(rcu.synchronize(), true);
    rcu.readLock('r1');
    assert.equal(rcu.synchronize(), false);
    rcu.readUnlock('r1');
    assert.equal(rcu.synchronize(), true);
  });
});

describe('GroupCommitWAL', () => {
  it('append and flush', () => {
    const wal = new GroupCommitWAL();
    wal.append({ op: 'INSERT', key: 1 });
    wal.append({ op: 'INSERT', key: 2 });
    const result = wal.flush();
    assert.equal(result.flushed, 2);
    assert.equal(wal.pendingCount, 0);
  });

  it('isDurable after flush', () => {
    const wal = new GroupCommitWAL();
    const lsn = wal.append({ op: 'INSERT' });
    assert.equal(wal.isDurable(lsn), false);
    wal.flush();
    assert.equal(wal.isDurable(lsn), true);
  });

  it('group commit batching', () => {
    const wal = new GroupCommitWAL();
    for (let i = 0; i < 100; i++) wal.append({ op: 'INSERT', key: i });
    wal.flush();
    assert.equal(wal.stats.avgBatchSize, 100);
  });

  it('multiple flushes', () => {
    const wal = new GroupCommitWAL();
    for (let i = 0; i < 10; i++) wal.append({});
    wal.flush();
    for (let i = 0; i < 20; i++) wal.append({});
    wal.flush();
    assert.equal(wal.stats.flushCount, 2);
    assert.equal(wal.stats.totalRecords, 30);
  });
});
