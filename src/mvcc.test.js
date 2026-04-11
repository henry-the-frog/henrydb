// mvcc.test.js — Tests for MVCC snapshot isolation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCManager, MVCCTransaction } from './mvcc.js';

describe('MVCC Core', () => {
  it('basic read/write within a transaction', () => {
    const mgr = new MVCCManager();
    const tx = mgr.begin();
    mgr.write(tx, 'x', 10);
    assert.equal(mgr.read(tx, 'x'), 10);
    tx.commit();
  });

  it('committed writes visible to new transactions', () => {
    const mgr = new MVCCManager();
    const tx1 = mgr.begin();
    mgr.write(tx1, 'x', 42);
    tx1.commit();
    
    const tx2 = mgr.begin();
    assert.equal(mgr.read(tx2, 'x'), 42);
    tx2.commit();
  });

  it('dirty reads prevented (uncommitted writes invisible)', () => {
    const mgr = new MVCCManager();
    const tx1 = mgr.begin();
    const tx2 = mgr.begin();
    
    mgr.write(tx1, 'x', 100);
    // tx2 should NOT see tx1's uncommitted write
    assert.equal(mgr.read(tx2, 'x'), undefined);
    
    tx1.commit();
    // tx2 still shouldn't see it (snapshot was taken before tx1 committed)
    assert.equal(mgr.read(tx2, 'x'), undefined);
    tx2.commit();
  });

  it('snapshot isolation: non-repeatable reads prevented', () => {
    const mgr = new MVCCManager();
    
    // Setup: write initial value
    const setup = mgr.begin();
    mgr.write(setup, 'x', 1);
    setup.commit();
    
    // tx1 reads x=1
    const tx1 = mgr.begin();
    assert.equal(mgr.read(tx1, 'x'), 1);
    
    // tx2 updates x=2 and commits
    const tx2 = mgr.begin();
    mgr.write(tx2, 'x', 2);
    tx2.commit();
    
    // tx1 still sees x=1 (repeatable read)
    assert.equal(mgr.read(tx1, 'x'), 1);
    tx1.commit();
    
    // New transaction sees x=2
    const tx3 = mgr.begin();
    assert.equal(mgr.read(tx3, 'x'), 2);
    tx3.commit();
  });

  it('write-write conflict detection', () => {
    const mgr = new MVCCManager();
    const setup = mgr.begin();
    mgr.write(setup, 'x', 1);
    setup.commit();
    
    const tx1 = mgr.begin();
    const tx2 = mgr.begin();
    
    mgr.write(tx1, 'x', 10);
    
    // tx2 tries to write same key → conflict
    assert.throws(() => mgr.write(tx2, 'x', 20), /Write-write conflict/);
    
    tx1.commit();
  });

  it('rollback removes versions', () => {
    const mgr = new MVCCManager();
    const tx = mgr.begin();
    mgr.write(tx, 'x', 99);
    assert.equal(mgr.read(tx, 'x'), 99);
    tx.rollback();
    
    const tx2 = mgr.begin();
    assert.equal(mgr.read(tx2, 'x'), undefined);
    tx2.commit();
  });

  it('delete marks key as deleted', () => {
    const mgr = new MVCCManager();
    const tx1 = mgr.begin();
    mgr.write(tx1, 'x', 42);
    tx1.commit();
    
    const tx2 = mgr.begin();
    assert.equal(mgr.read(tx2, 'x'), 42);
    mgr.delete(tx2, 'x');
    assert.equal(mgr.read(tx2, 'x'), undefined);
    tx2.commit();
    
    const tx3 = mgr.begin();
    assert.equal(mgr.read(tx3, 'x'), undefined);
    tx3.commit();
  });

  it('scan returns visible key-value pairs', () => {
    const mgr = new MVCCManager();
    const tx1 = mgr.begin();
    mgr.write(tx1, 'a', 1);
    mgr.write(tx1, 'b', 2);
    mgr.write(tx1, 'c', 3);
    tx1.commit();
    
    const tx2 = mgr.begin();
    const pairs = [...mgr.scan(tx2)];
    assert.equal(pairs.length, 3);
    const keys = pairs.map(p => p.key).sort();
    assert.deepEqual(keys, ['a', 'b', 'c']);
    tx2.commit();
  });

  it('multiple transactions interleaved correctly', () => {
    const mgr = new MVCCManager();
    
    const tx1 = mgr.begin();
    mgr.write(tx1, 'x', 1);
    
    const tx2 = mgr.begin();
    mgr.write(tx2, 'y', 2);
    
    // tx1 sees x=1 but not y
    assert.equal(mgr.read(tx1, 'x'), 1);
    assert.equal(mgr.read(tx1, 'y'), undefined);
    
    // tx2 sees y=2 but not x
    assert.equal(mgr.read(tx2, 'y'), 2);
    assert.equal(mgr.read(tx2, 'x'), undefined);
    
    tx1.commit();
    
    // tx2 still can't see x (snapshot frozen at begin)
    assert.equal(mgr.read(tx2, 'x'), undefined);
    
    tx2.commit();
  });

  it('garbage collection removes old versions', () => {
    const mgr = new MVCCManager();
    
    // Create 5 versions of key 'x'
    for (let i = 0; i < 5; i++) {
      const tx = mgr.begin();
      mgr.write(tx, 'x', i);
      tx.commit();
    }
    
    assert.equal(mgr.getStats().totalVersions, 5);
    
    const result = mgr.gc();
    assert.ok(result.cleaned > 0);
    assert.ok(mgr.getStats().totalVersions < 5);
  });

  it('vacuum removes all old versions', () => {
    const mgr = new MVCCManager();
    
    for (let i = 0; i < 10; i++) {
      const tx = mgr.begin();
      mgr.write(tx, 'x', i);
      tx.commit();
    }
    
    const result = mgr.vacuum();
    assert.ok(result.cleaned > 0);
    assert.equal(mgr.getStats().totalVersions, 1);
    
    // Latest value still readable
    const tx = mgr.begin();
    assert.equal(mgr.read(tx, 'x'), 9);
    tx.commit();
  });

  it('vacuum fails with active transactions', () => {
    const mgr = new MVCCManager();
    const tx = mgr.begin();
    assert.throws(() => mgr.vacuum(), /active/i);
    tx.commit();
  });
});

describe('MVCC Snapshot Isolation Invariants', () => {
  it('write skew scenario detectable', () => {
    // Classic write skew: tx1 reads x, tx2 reads y, tx1 writes y, tx2 writes x
    // Both see the old values — snapshot isolation allows this (SSI would catch it)
    const mgr = new MVCCManager();
    
    const setup = mgr.begin();
    mgr.write(setup, 'x', 100);
    mgr.write(setup, 'y', 100);
    setup.commit();
    
    const tx1 = mgr.begin();
    const tx2 = mgr.begin();
    
    const x1 = mgr.read(tx1, 'x'); // 100
    const y2 = mgr.read(tx2, 'y'); // 100
    
    mgr.write(tx1, 'y', x1 - 200); // y = -100
    mgr.write(tx2, 'x', y2 - 200); // x = -100
    
    tx1.commit();
    tx2.commit();
    
    // Both committed — write skew occurred (x + y = -200, invariant violated)
    // This is expected behavior for snapshot isolation
    const check = mgr.begin();
    assert.equal(mgr.read(check, 'x'), -100);
    assert.equal(mgr.read(check, 'y'), -100);
    check.commit();
  });

  it('phantom reads prevented in snapshot isolation', () => {
    const mgr = new MVCCManager();
    
    const setup = mgr.begin();
    for (let i = 0; i < 5; i++) mgr.write(setup, `row:${i}`, { val: i });
    setup.commit();
    
    const tx1 = mgr.begin();
    const count1 = [...mgr.scan(tx1)].length; // 5 rows
    
    // Another tx inserts a new row
    const tx2 = mgr.begin();
    mgr.write(tx2, 'row:5', { val: 5 });
    tx2.commit();
    
    // tx1 still sees 5 rows (phantom prevented)
    const count2 = [...mgr.scan(tx1)].length;
    assert.equal(count1, count2);
    tx1.commit();
  });

  it('long-running transaction sees consistent snapshot', () => {
    const mgr = new MVCCManager();
    
    const setup = mgr.begin();
    mgr.write(setup, 'counter', 0);
    setup.commit();
    
    const reader = mgr.begin();
    assert.equal(mgr.read(reader, 'counter'), 0);
    
    // Many other transactions increment counter
    for (let i = 1; i <= 100; i++) {
      const tx = mgr.begin();
      mgr.write(tx, 'counter', i);
      tx.commit();
    }
    
    // Reader still sees 0
    assert.equal(mgr.read(reader, 'counter'), 0);
    reader.commit();
    
    // New reader sees 100
    const newReader = mgr.begin();
    assert.equal(mgr.read(newReader, 'counter'), 100);
    newReader.commit();
  });
});
