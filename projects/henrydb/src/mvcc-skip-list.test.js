// mvcc-skip-list.test.js — Tests for MVCC Skip List
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCSkipList } from './mvcc-skip-list.js';

describe('MVCCSkipList — Basic Operations', () => {
  it('put and get within same transaction', () => {
    const sl = new MVCCSkipList();
    const tx = sl.begin();
    sl.put(tx, 'a', 1);
    sl.put(tx, 'b', 2);
    sl.put(tx, 'c', 3);
    assert.equal(sl.get(tx, 'a'), 1);
    assert.equal(sl.get(tx, 'b'), 2);
    assert.equal(sl.get(tx, 'c'), 3);
    assert.equal(sl.get(tx, 'd'), undefined);
    sl.commit(tx);
  });

  it('committed data visible to new transactions', () => {
    const sl = new MVCCSkipList();
    const tx1 = sl.begin();
    sl.put(tx1, 'key', 'value');
    sl.commit(tx1);

    const tx2 = sl.begin();
    assert.equal(sl.get(tx2, 'key'), 'value');
    sl.commit(tx2);
  });

  it('uncommitted data invisible to other transactions', () => {
    const sl = new MVCCSkipList();
    const tx1 = sl.begin();
    sl.put(tx1, 'secret', 'hidden');

    const tx2 = sl.begin();
    assert.equal(sl.get(tx2, 'secret'), undefined);
    
    sl.commit(tx1);
    // tx2's snapshot was taken before tx1 committed
    assert.equal(sl.get(tx2, 'secret'), undefined);
    sl.commit(tx2);
  });

  it('delete creates tombstone', () => {
    const sl = new MVCCSkipList();
    const tx1 = sl.begin();
    sl.put(tx1, 'x', 100);
    sl.commit(tx1);

    const tx2 = sl.begin();
    assert.equal(sl.get(tx2, 'x'), 100);
    sl.delete(tx2, 'x');
    assert.equal(sl.get(tx2, 'x'), undefined);
    sl.commit(tx2);

    const tx3 = sl.begin();
    assert.equal(sl.get(tx3, 'x'), undefined);
    sl.commit(tx3);
  });

  it('update creates new version', () => {
    const sl = new MVCCSkipList();
    const tx1 = sl.begin();
    sl.put(tx1, 'counter', 1);
    sl.commit(tx1);

    const tx2 = sl.begin();
    sl.put(tx2, 'counter', 2);
    sl.commit(tx2);

    const tx3 = sl.begin();
    assert.equal(sl.get(tx3, 'counter'), 2);
    sl.commit(tx3);
  });

  it('rollback undoes writes', () => {
    const sl = new MVCCSkipList();
    const tx1 = sl.begin();
    sl.put(tx1, 'temp', 'data');
    sl.rollback(tx1);

    const tx2 = sl.begin();
    assert.equal(sl.get(tx2, 'temp'), undefined);
    sl.commit(tx2);
  });
});

describe('MVCCSkipList — Snapshot Isolation', () => {
  it('readers see consistent snapshot', () => {
    const sl = new MVCCSkipList();
    
    // Write initial data
    const setup = sl.begin();
    sl.put(setup, 'balance', 1000);
    sl.commit(setup);

    // Reader starts
    const reader = sl.begin();
    assert.equal(sl.get(reader, 'balance'), 1000);

    // Writer updates
    const writer = sl.begin();
    sl.put(writer, 'balance', 500);
    sl.commit(writer);

    // Reader still sees old value (snapshot isolation)
    assert.equal(sl.get(reader, 'balance'), 1000);
    sl.commit(reader);

    // New reader sees updated value
    const reader2 = sl.begin();
    assert.equal(sl.get(reader2, 'balance'), 500);
    sl.commit(reader2);
  });

  it('out-of-order commits handled correctly', () => {
    const sl = new MVCCSkipList();

    const tx1 = sl.begin(); // txId=1
    sl.put(tx1, 'a', 'from_tx1');

    const tx2 = sl.begin(); // txId=2
    sl.put(tx2, 'b', 'from_tx2');

    const tx3 = sl.begin(); // txId=3, snapshot: tx1 and tx2 active

    // tx2 commits before tx1 (out of order!)
    sl.commit(tx2);

    // tx3 should NOT see tx2's write (was active in snapshot)
    assert.equal(sl.get(tx3, 'b'), undefined);

    sl.commit(tx1);
    // tx3 still shouldn't see tx1's write
    assert.equal(sl.get(tx3, 'a'), undefined);

    sl.commit(tx3);
  });
});

describe('MVCCSkipList — Ordered Scan', () => {
  it('scan returns keys in order', () => {
    const sl = new MVCCSkipList();
    const tx = sl.begin();
    sl.put(tx, 'cherry', 3);
    sl.put(tx, 'apple', 1);
    sl.put(tx, 'banana', 2);
    sl.commit(tx);

    const reader = sl.begin();
    const entries = [...sl.scan(reader)];
    assert.deepEqual(entries, [
      { key: 'apple', value: 1 },
      { key: 'banana', value: 2 },
      { key: 'cherry', value: 3 },
    ]);
    sl.commit(reader);
  });

  it('scan with range', () => {
    const sl = new MVCCSkipList();
    const tx = sl.begin();
    for (let i = 0; i < 100; i++) {
      sl.put(tx, String(i).padStart(3, '0'), i);
    }
    sl.commit(tx);

    const reader = sl.begin();
    const entries = [...sl.scan(reader, '020', '030')];
    assert.equal(entries.length, 11); // 020..030 inclusive
    assert.equal(entries[0].key, '020');
    assert.equal(entries[10].key, '030');
    sl.commit(reader);
  });

  it('scan skips deleted keys', () => {
    const sl = new MVCCSkipList();
    const tx1 = sl.begin();
    sl.put(tx1, 'a', 1);
    sl.put(tx1, 'b', 2);
    sl.put(tx1, 'c', 3);
    sl.commit(tx1);

    const tx2 = sl.begin();
    sl.delete(tx2, 'b');
    sl.commit(tx2);

    const reader = sl.begin();
    const entries = [...sl.scan(reader)];
    assert.equal(entries.length, 2);
    assert.equal(entries[0].key, 'a');
    assert.equal(entries[1].key, 'c');
    sl.commit(reader);
  });

  it('scan only shows committed data to reader', () => {
    const sl = new MVCCSkipList();
    const tx1 = sl.begin();
    sl.put(tx1, 'visible', 1);
    sl.commit(tx1);

    const reader = sl.begin();

    const tx2 = sl.begin();
    sl.put(tx2, 'invisible', 2);
    sl.commit(tx2);

    const entries = [...sl.scan(reader)];
    assert.equal(entries.length, 1);
    assert.equal(entries[0].key, 'visible');
    sl.commit(reader);
  });
});

describe('MVCCSkipList — GC and Stats', () => {
  it('gc removes old versions', () => {
    const sl = new MVCCSkipList();
    
    // Create 10 versions of the same key
    for (let i = 0; i < 10; i++) {
      const tx = sl.begin();
      sl.put(tx, 'counter', i);
      sl.commit(tx);
    }

    const stats1 = sl.getStats();
    assert.equal(stats1.totalVersions, 10);

    const cleaned = sl.gc();
    const stats2 = sl.getStats();
    
    // Should keep at most 1-2 versions (latest committed)
    assert.ok(cleaned > 0, `Should clean some versions, cleaned: ${cleaned}`);
    assert.ok(stats2.totalVersions < 10);
  });

  it('getStats reports correct values', () => {
    const sl = new MVCCSkipList();
    const tx = sl.begin();
    sl.put(tx, 'a', 1);
    sl.put(tx, 'b', 2);
    sl.delete(tx, 'c'); // tombstone
    sl.commit(tx);

    const stats = sl.getStats();
    assert.equal(stats.nodes, 3);
    assert.equal(stats.tombstones, 1);
    assert.ok(stats.level >= 1);
  });
});

describe('MVCCSkipList — Performance', () => {
  const N = 10_000;

  it('benchmark: 10K inserts + lookups', () => {
    const sl = new MVCCSkipList();
    
    const t0 = performance.now();
    const tx = sl.begin();
    for (let i = 0; i < N; i++) {
      sl.put(tx, `key-${String(i).padStart(5, '0')}`, i);
    }
    sl.commit(tx);
    const insertMs = performance.now() - t0;

    const t1 = performance.now();
    const reader = sl.begin();
    for (let i = 0; i < N; i++) {
      const v = sl.get(reader, `key-${String(i).padStart(5, '0')}`);
      assert.equal(v, i);
    }
    sl.commit(reader);
    const lookupMs = performance.now() - t1;

    console.log(`    MVCC Skip List: ${N} inserts in ${insertMs.toFixed(1)}ms, ${N} lookups in ${lookupMs.toFixed(1)}ms`);
  });

  it('benchmark: concurrent transactions', () => {
    const sl = new MVCCSkipList();
    const NUM_TX = 100;
    const OPS_PER_TX = 100;

    const t0 = performance.now();
    const txns = [];
    for (let t = 0; t < NUM_TX; t++) {
      const tx = sl.begin();
      for (let i = 0; i < OPS_PER_TX; i++) {
        sl.put(tx, `tx${t}-key${i}`, t * OPS_PER_TX + i);
      }
      txns.push(tx);
    }
    // Commit all
    for (const tx of txns) sl.commit(tx);
    const elapsed = performance.now() - t0;

    console.log(`    ${NUM_TX} concurrent txns × ${OPS_PER_TX} ops = ${NUM_TX * OPS_PER_TX} total in ${elapsed.toFixed(1)}ms`);
    
    // Verify all data visible
    const reader = sl.begin();
    assert.equal(sl.count(reader), NUM_TX * OPS_PER_TX);
    sl.commit(reader);
  });

  it('benchmark: scan performance', () => {
    const sl = new MVCCSkipList();
    const tx = sl.begin();
    for (let i = 0; i < N; i++) {
      sl.put(tx, `scan-${String(i).padStart(5, '0')}`, i);
    }
    sl.commit(tx);

    const reader = sl.begin();
    const t0 = performance.now();
    let count = 0;
    for (const _ of sl.scan(reader)) count++;
    const elapsed = performance.now() - t0;
    sl.commit(reader);

    console.log(`    Scan ${count} keys in ${elapsed.toFixed(1)}ms (${(count / elapsed * 1000) | 0} keys/sec)`);
    assert.equal(count, N);
  });
});
