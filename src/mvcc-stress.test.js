// mvcc-stress.test.js — Randomized MVCC stress testing
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCManager } from './mvcc.js';

function seeded(seed) {
  let s = seed;
  return () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff; };
}

function randomInt(rng, min, max) { return Math.floor(rng() * (max - min + 1)) + min; }

describe('MVCC Stress Tests', () => {
  it('100 concurrent transactions, random reads/writes, no crashes', () => {
    const mgr = new MVCCManager();
    const rng = seeded(42);
    const NUM_KEYS = 20;
    const NUM_TXNS = 100;
    const OPS_PER_TXN = 10;
    
    // Setup initial data
    const setup = mgr.begin();
    for (let k = 0; k < NUM_KEYS; k++) {
      mgr.write(setup, `k${k}`, 0);
    }
    setup.commit();
    
    // Run random transactions (some overlap)
    const activeTxns = [];
    let committed = 0, rolledBack = 0, conflicts = 0;
    
    for (let i = 0; i < NUM_TXNS; i++) {
      // Start new transaction
      const tx = mgr.begin();
      activeTxns.push(tx);
      
      // Random operations
      for (let op = 0; op < OPS_PER_TXN; op++) {
        const key = `k${randomInt(rng, 0, NUM_KEYS - 1)}`;
        const action = rng();
        
        if (action < 0.5) {
          // Read
          const val = mgr.read(tx, key);
          // Value should be a number or undefined
          assert.ok(val === undefined || typeof val === 'number', 
            `Bad value: ${val} for key ${key} in tx ${tx.txId}`);
        } else {
          // Write
          try {
            mgr.write(tx, key, randomInt(rng, 0, 1000));
          } catch (e) {
            if (e.message.includes('conflict')) {
              conflicts++;
            } else {
              throw e; // unexpected error
            }
          }
        }
      }
      
      // Randomly commit or rollback
      if (rng() < 0.8) {
        tx.commit();
        committed++;
      } else {
        tx.rollback();
        rolledBack++;
      }
    }
    
    assert.ok(committed > 0);
    assert.ok(committed + rolledBack === NUM_TXNS);
  });

  it('snapshot isolation invariant: reads never change within a transaction', () => {
    const mgr = new MVCCManager();
    const rng = seeded(99);
    
    // Setup
    const setup = mgr.begin();
    for (let i = 0; i < 10; i++) mgr.write(setup, `key${i}`, i);
    setup.commit();
    
    // Reader takes snapshot
    const reader = mgr.begin();
    const firstRead = {};
    for (let i = 0; i < 10; i++) {
      firstRead[`key${i}`] = mgr.read(reader, `key${i}`);
    }
    
    // Background writers modify all keys
    for (let round = 0; round < 50; round++) {
      const writer = mgr.begin();
      const key = `key${randomInt(rng, 0, 9)}`;
      try {
        mgr.write(writer, key, randomInt(rng, 100, 999));
        writer.commit();
      } catch (e) {
        writer.rollback();
      }
    }
    
    // Reader should still see exact same values
    for (let i = 0; i < 10; i++) {
      const val = mgr.read(reader, `key${i}`);
      assert.equal(val, firstRead[`key${i}`], 
        `Snapshot violated: key${i} was ${firstRead[`key${i}`]}, now ${val}`);
    }
    reader.commit();
  });

  it('write-write conflicts are correctly detected', () => {
    const mgr = new MVCCManager();
    
    const setup = mgr.begin();
    mgr.write(setup, 'x', 0);
    setup.commit();
    
    let conflictsDetected = 0;
    for (let i = 0; i < 20; i++) {
      const tx1 = mgr.begin();
      const tx2 = mgr.begin();
      
      mgr.write(tx1, 'x', i * 10);
      
      try {
        mgr.write(tx2, 'x', i * 20);
        // No conflict — should not happen (tx1 is uncommitted)
      } catch (e) {
        if (e.message.includes('conflict')) conflictsDetected++;
      }
      
      tx1.commit();
      tx2.rollback();
    }
    
    assert.equal(conflictsDetected, 20, 'All 20 should be conflicts');
  });

  it('GC preserves versions needed by active transactions', () => {
    const mgr = new MVCCManager();
    
    // Create initial data
    const setup = mgr.begin();
    mgr.write(setup, 'x', 0);
    setup.commit();
    
    // Long-running reader
    const reader = mgr.begin();
    assert.equal(mgr.read(reader, 'x'), 0);
    
    // Many writes
    for (let i = 1; i <= 50; i++) {
      const tx = mgr.begin();
      mgr.write(tx, 'x', i);
      tx.commit();
    }
    
    // GC should NOT remove versions needed by reader
    mgr.gc();
    
    // Reader must still see 0
    assert.equal(mgr.read(reader, 'x'), 0);
    reader.commit();
    
    // After reader commits, GC can clean up
    mgr.gc();
    const stats = mgr.getStats();
    assert.ok(stats.totalVersions < 50, `Expected cleanup, got ${stats.totalVersions} versions`);
  });

  it('1000-transaction stress test with GC', () => {
    const mgr = new MVCCManager();
    const rng = seeded(777);
    
    let writes = 0, reads = 0, conflicts = 0;
    
    for (let i = 0; i < 1000; i++) {
      const tx = mgr.begin();
      const key = `k${randomInt(rng, 0, 50)}`;
      
      try {
        if (rng() < 0.6) {
          mgr.write(tx, key, randomInt(rng, 0, 999));
          writes++;
        } else {
          mgr.read(tx, key);
          reads++;
        }
        tx.commit();
      } catch (e) {
        if (e.message.includes('conflict')) conflicts++;
        tx.rollback();
      }
      
      // Periodic GC
      if (i % 100 === 0) mgr.gc();
    }
    
    // Final GC + vacuum
    mgr.gc();
    const stats = mgr.getStats();
    
    assert.ok(writes > 0);
    assert.ok(reads > 0);
    assert.ok(stats.keys <= 51); // at most 51 distinct keys
  });
});
