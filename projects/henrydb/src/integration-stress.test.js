// integration-stress.test.js — Stress tests for HenryDB subsystems
// Exercises: MVCC concurrent transactions, deadlock detection,
// ARIES crash recovery, lock manager
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCManager, MVCCHeap } from './mvcc.js';
import { HeapFile } from './page.js';
import { WaitForGraph, DeadlockDetector } from './deadlock-detector.js';
import { ARIESRecovery } from './aries-recovery.js';
import { LockManager } from './lock-manager.js';

describe('MVCC Concurrent Transactions', () => {
  let mgr, heap;

  beforeEach(() => {
    mgr = new MVCCManager();
    heap = new MVCCHeap(new HeapFile('accounts'));
  });

  it('snapshot isolation: readers see consistent snapshots', () => {
    const tx1 = mgr.begin();
    heap.insert([1, 'Alice', 1000], tx1);
    heap.insert([2, 'Bob', 500], tx1);
    tx1.commit();

    // TX2 takes snapshot
    const tx2 = mgr.begin();
    const snapshot = [...heap.scan(tx2)];
    assert.equal(snapshot.length, 2);

    // TX3 modifies and commits
    const tx3 = mgr.begin();
    const aliceRow = [...heap.scan(tx3)].find(r => r.values[1] === 'Alice');
    heap.update(aliceRow.pageId, aliceRow.slotIdx, [1, 'Alice', 2000], tx3);
    tx3.commit();

    // TX2 should still see original
    const afterModify = [...heap.scan(tx2)];
    const aliceInTx2 = afterModify.find(r => r.values[1] === 'Alice');
    assert.equal(aliceInTx2.values[2], 1000);
    tx2.commit();

    // New TX sees update
    const tx4 = mgr.begin();
    const aliceInTx4 = [...heap.scan(tx4)].find(r => r.values[1] === 'Alice');
    assert.equal(aliceInTx4.values[2], 2000);
    tx4.commit();
  });

  it('5 concurrent readers + 1 writer', () => {
    const setup = mgr.begin();
    heap.insert([1, 'counter', 0], setup);
    setup.commit();

    const readers = [];
    for (let i = 0; i < 5; i++) readers.push(mgr.begin());

    const writer = mgr.begin();
    const row = [...heap.scan(writer)][0];
    heap.update(row.pageId, row.slotIdx, [1, 'counter', 999], writer);
    writer.commit();

    for (const r of readers) {
      const rows = [...heap.scan(r)];
      assert.equal(rows[0].values[2], 0);
      r.commit();
    }

    const newR = mgr.begin();
    assert.equal([...heap.scan(newR)][0].values[2], 999);
    newR.commit();
  });

  it('rollback undoes inserts', () => {
    const tx1 = mgr.begin();
    heap.insert([1, 'temp', 100], tx1);
    tx1.rollback();

    const tx2 = mgr.begin();
    assert.equal([...heap.scan(tx2)].length, 0);
    tx2.commit();
  });

  it('50 serial inserts', () => {
    for (let i = 0; i < 50; i++) {
      const tx = mgr.begin();
      heap.insert([i, `row_${i}`, i * 10], tx);
      tx.commit();
    }
    const reader = mgr.begin();
    assert.equal([...heap.scan(reader)].length, 50);
    reader.commit();
  });

  it('WAL records operations', () => {
    const tx1 = mgr.begin();
    heap.insert([1, 'a', 1], tx1);
    heap.insert([2, 'b', 2], tx1);
    tx1.commit();
    assert.ok(mgr.wal.length >= 4);
  });

  it('bank transfer atomicity', () => {
    const setup = mgr.begin();
    heap.insert([1, 'alice', 1000], setup);
    heap.insert([2, 'bob', 500], setup);
    setup.commit();

    const transfer = mgr.begin();
    const rows = [...heap.scan(transfer)];
    const alice = rows.find(r => r.values[1] === 'alice');
    const bob = rows.find(r => r.values[1] === 'bob');
    heap.update(alice.pageId, alice.slotIdx, [1, 'alice', 800], transfer);
    heap.update(bob.pageId, bob.slotIdx, [2, 'bob', 700], transfer);
    transfer.commit();

    const verify = mgr.begin();
    const final = [...heap.scan(verify)];
    const a = final.find(r => r.values[1] === 'alice');
    const b = final.find(r => r.values[1] === 'bob');
    assert.equal(a.values[2], 800);
    assert.equal(b.values[2], 700);
    assert.equal(a.values[2] + b.values[2], 1500);
    verify.commit();
  });

  it('delete makes row invisible', () => {
    const tx1 = mgr.begin();
    const rid = heap.insert([1, 'temp', 0], tx1);
    tx1.commit();

    const tx2 = mgr.begin();
    heap.delete(rid.pageId, rid.slotIdx, tx2);
    assert.equal([...heap.scan(tx2)].length, 0);
    tx2.commit();

    const tx3 = mgr.begin();
    assert.equal([...heap.scan(tx3)].length, 0);
    tx3.commit();
  });
});

describe('Deadlock Detection', () => {
  it('detects 2-way deadlock', () => {
    const wfg = new WaitForGraph();
    wfg.addEdge(1, 2, 'lock_A');
    wfg.addEdge(2, 1, 'lock_B');
    assert.ok(wfg.detectCycles().length > 0);
  });

  it('detects 3-way cycle', () => {
    const wfg = new WaitForGraph();
    wfg.addEdge(1, 2, 'lock_A');
    wfg.addEdge(2, 3, 'lock_B');
    wfg.addEdge(3, 1, 'lock_C');
    assert.ok(wfg.detectCycles().length > 0);
  });

  it('no false positive on acyclic graph', () => {
    const wfg = new WaitForGraph();
    wfg.addEdge(1, 2, 'lock_A');
    wfg.addEdge(2, 3, 'lock_B');
    wfg.addEdge(4, 3, 'lock_C');
    assert.equal(wfg.detectCycles().length, 0);
  });

  it('removing edge breaks deadlock', () => {
    const wfg = new WaitForGraph();
    wfg.addEdge(1, 2, 'lock_A');
    wfg.addEdge(2, 1, 'lock_B');
    assert.ok(wfg.detectCycles().length > 0);
    wfg.removeEdge(1, 2);
    assert.equal(wfg.detectCycles().length, 0);
  });

  it('20-node chain: no cycle until closed', () => {
    const wfg = new WaitForGraph();
    for (let i = 1; i < 20; i++) wfg.addEdge(i, i + 1, `lock_${i}`);
    assert.equal(wfg.detectCycles().length, 0);
    wfg.addEdge(20, 1, 'lock_20');
    assert.ok(wfg.detectCycles().length > 0);
  });

  it('DeadlockDetector with recordWait + check', () => {
    const dd = new DeadlockDetector();
    dd.registerTransaction(1);
    dd.registerTransaction(2);
    dd.recordWait(1, 2, 'resource_A');
    dd.recordWait(2, 1, 'resource_B');
    const result = dd.check();
    assert.ok(result.length > 0);
  });
});

describe('ARIES Crash Recovery', () => {
  it('redo committed transactions', () => {
    const r = new ARIESRecovery();
    r.begin('tx1');
    r.write('tx1', 'balance_A', 1000);
    r.write('tx1', 'balance_B', 2000);
    r.commit('tx1');

    r.crashAndRecover();

    assert.equal(r._data.get('balance_A'), 1000);
    assert.equal(r._data.get('balance_B'), 2000);
    assert.ok(r.stats.redone > 0);
  });

  it('undo uncommitted transactions', () => {
    const r = new ARIESRecovery();
    r.begin('tx1');
    r.write('tx1', 'key1', 'committed');
    r.commit('tx1');

    r.begin('tx2');
    r.write('tx2', 'key1', 'uncommitted');

    r.crashAndRecover();
    assert.equal(r._data.get('key1'), 'committed');
    assert.ok(r.stats.undone > 0);
  });

  it('checkpoint reduces redo work', () => {
    const r = new ARIESRecovery();
    for (let i = 0; i < 20; i++) {
      r.begin(`tx${i}`);
      r.write(`tx${i}`, `key${i}`, i * 100);
      r.commit(`tx${i}`);
    }
    r.checkpoint();

    r.begin('txLate');
    r.write('txLate', 'lateKey', 9999);
    r.commit('txLate');

    r.crashAndRecover();

    for (let i = 0; i < 20; i++) {
      assert.equal(r._data.get(`key${i}`), i * 100);
    }
    assert.equal(r._data.get('lateKey'), 9999);
  });

  it('mixed committed and uncommitted (crash before commit)', () => {
    const r = new ARIESRecovery();
    r.begin('tx1'); r.write('tx1', 'a', 100); r.commit('tx1');
    // tx2 never commits or aborts — active at crash time
    r.begin('tx2'); r.write('tx2', 'b', 200);
    r.begin('tx3'); r.write('tx3', 'c', 300); r.commit('tx3');

    r.crashAndRecover();
    assert.equal(r._data.get('a'), 100);
    assert.equal(r._data.get('c'), 300);
    // tx2 was active — should be undone
    assert.ok(r.stats.undone > 0);
  });

  it('idempotent double recovery', () => {
    const r = new ARIESRecovery();
    r.begin('tx1'); r.write('tx1', 'x', 42); r.commit('tx1');

    r.crashAndRecover();
    assert.equal(r._data.get('x'), 42);

    r.crashAndRecover();
    assert.equal(r._data.get('x'), 42);
  });

  it('100 committed transactions', () => {
    const r = new ARIESRecovery();
    for (let i = 0; i < 100; i++) {
      r.begin(`tx${i}`);
      r.write(`tx${i}`, `key${i}`, `val${i}`);
      r.commit(`tx${i}`);
    }

    r.crashAndRecover();

    for (let i = 0; i < 100; i++) {
      assert.equal(r._data.get(`key${i}`), `val${i}`);
    }
  });

  it('multiple checkpoints', () => {
    const r = new ARIESRecovery();
    for (let i = 0; i < 10; i++) {
      r.begin(`b1_${i}`); r.write(`b1_${i}`, `k1_${i}`, i); r.commit(`b1_${i}`);
    }
    r.checkpoint();
    for (let i = 0; i < 10; i++) {
      r.begin(`b2_${i}`); r.write(`b2_${i}`, `k2_${i}`, i + 100); r.commit(`b2_${i}`);
    }
    r.checkpoint();
    r.begin('fin'); r.write('fin', 'last', 42); r.commit('fin');

    r.crashAndRecover();

    for (let i = 0; i < 10; i++) {
      assert.equal(r._data.get(`k1_${i}`), i);
      assert.equal(r._data.get(`k2_${i}`), i + 100);
    }
    assert.equal(r._data.get('last'), 42);
  });
});

describe('Lock Manager', () => {
  it('exclusive lock grants and releases', async () => {
    const lm = new LockManager();
    await lm.acquire('tx1', 'r1', 'X');
    lm.release('tx1', 'r1');
  });

  it('shared locks allow concurrency', async () => {
    const lm = new LockManager();
    await lm.acquire('tx1', 'r1', 'S');
    await lm.acquire('tx2', 'r1', 'S');
    await lm.acquire('tx3', 'r1', 'S');
    lm.release('tx1', 'r1');
    lm.release('tx2', 'r1');
    lm.release('tx3', 'r1');
  });

  it('releaseAll frees all locks', async () => {
    const lm = new LockManager();
    for (let i = 0; i < 10; i++) {
      await lm.acquire('tx1', `r_${i}`, 'X');
    }
    lm.release('tx1');
  });

  it('stats track grants and releases', async () => {
    const lm = new LockManager();
    await lm.acquire('tx1', 'r1', 'S');
    await lm.acquire('tx2', 'r1', 'S');
    lm.release('tx1', 'r1');
    lm.release('tx2', 'r1');
    assert.ok(lm.stats.grants >= 2);
    assert.ok(lm.stats.releases >= 2);
  });

  it('intention locks: IS compatible with S', async () => {
    const lm = new LockManager();
    await lm.acquire('tx1', 'table', 'IS');
    await lm.acquire('tx2', 'table', 'S');
    lm.release('tx1');
    lm.release('tx2');
  });

  it('lock upgrade from S to X', async () => {
    const lm = new LockManager();
    await lm.acquire('tx1', 'r1', 'S');
    // Upgrade: acquire X on same resource
    await lm.acquire('tx1', 'r1', 'X');
    lm.release('tx1', 'r1');
  });
});
