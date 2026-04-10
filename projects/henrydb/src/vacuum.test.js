// vacuum.test.js — Tests for VACUUM and garbage collection
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCManager, MVCCTransaction } from './mvcc.js';

describe('MVCC Garbage Collection', () => {
  let mvcc;

  beforeEach(() => {
    mvcc = new MVCCManager();
  });

  describe('gc()', () => {
    it('removes old versions with no active transactions', () => {
      // Write 3 versions of key 'x'
      const tx1 = mvcc.begin();
      mvcc.write(tx1.txId, 'x', 'v1');
      mvcc.commit(tx1.txId);
      
      const tx2 = mvcc.begin();
      mvcc.write(tx2.txId, 'x', 'v2');
      mvcc.commit(tx2.txId);
      
      const tx3 = mvcc.begin();
      mvcc.write(tx3.txId, 'x', 'v3');
      mvcc.commit(tx3.txId);
      
      const stats = mvcc.getStats();
      assert.strictEqual(stats.totalVersions, 3);
      
      const result = mvcc.gc();
      assert.ok(result.cleaned >= 1, 'Should clean at least 1 old version');
      
      const afterStats = mvcc.getStats();
      assert.ok(afterStats.totalVersions <= 2, 'Should have at most 2 versions after GC');
    });

    it('preserves versions visible to active transactions', () => {
      const tx1 = mvcc.begin();
      mvcc.write(tx1.txId, 'x', 'v1');
      mvcc.commit(tx1.txId);
      
      // Start a long-running reader
      const reader = mvcc.begin();
      
      // Write more versions while reader is active
      const tx2 = mvcc.begin();
      mvcc.write(tx2.txId, 'x', 'v2');
      mvcc.commit(tx2.txId);
      
      const tx3 = mvcc.begin();
      mvcc.write(tx3.txId, 'x', 'v3');
      mvcc.commit(tx3.txId);
      
      // GC should preserve versions visible to the reader
      mvcc.gc();
      
      // Reader should still be able to read the value it could see
      const value = mvcc.read(reader.txId, 'x');
      assert.strictEqual(value, 'v1', 'Reader should see v1 from its snapshot');
      
      mvcc.commit(reader.txId);
    });

    it('handles keys with single version', () => {
      const tx = mvcc.begin();
      mvcc.write(tx.txId, 'single', 'only');
      mvcc.commit(tx.txId);
      
      const result = mvcc.gc();
      assert.strictEqual(result.cleaned, 0);
    });

    it('handles empty version map', () => {
      const result = mvcc.gc();
      assert.strictEqual(result.cleaned, 0);
    });
  });

  describe('vacuum()', () => {
    it('removes all old versions', () => {
      const tx1 = mvcc.begin();
      mvcc.write(tx1.txId, 'a', 'a1');
      mvcc.write(tx1.txId, 'b', 'b1');
      mvcc.commit(tx1.txId);
      
      const tx2 = mvcc.begin();
      mvcc.write(tx2.txId, 'a', 'a2');
      mvcc.write(tx2.txId, 'b', 'b2');
      mvcc.commit(tx2.txId);
      
      const tx3 = mvcc.begin();
      mvcc.write(tx3.txId, 'a', 'a3');
      mvcc.commit(tx3.txId);
      
      const before = mvcc.getStats();
      assert.strictEqual(before.totalVersions, 5); // a:3 + b:2
      
      const result = mvcc.vacuum();
      assert.strictEqual(result.cleaned, 3); // removed a1, a2, b1
      
      const after = mvcc.getStats();
      assert.strictEqual(after.totalVersions, 2); // a3, b2
    });

    it('removes deleted key markers', () => {
      const tx1 = mvcc.begin();
      mvcc.write(tx1.txId, 'x', 'value');
      mvcc.commit(tx1.txId);
      
      const tx2 = mvcc.begin();
      mvcc.delete(tx2.txId, 'x');
      mvcc.commit(tx2.txId);
      
      const before = mvcc.getStats();
      assert.strictEqual(before.keys, 1);
      
      const result = mvcc.vacuum();
      assert.ok(result.keysRemoved >= 1);
      
      const after = mvcc.getStats();
      assert.strictEqual(after.keys, 0);
    });

    it('throws when transactions are active', () => {
      const tx = mvcc.begin();
      assert.throws(() => mvcc.vacuum(), /active/i);
      mvcc.commit(tx.txId);
    });

    it('is idempotent (running twice)', () => {
      const tx = mvcc.begin();
      mvcc.write(tx.txId, 'x', 'v1');
      mvcc.commit(tx.txId);
      
      const tx2 = mvcc.begin();
      mvcc.write(tx2.txId, 'x', 'v2');
      mvcc.commit(tx2.txId);
      
      mvcc.vacuum();
      const result = mvcc.vacuum();
      assert.strictEqual(result.cleaned, 0);
    });
  });

  describe('Integration', () => {
    it('handles many updates then vacuum', () => {
      for (let i = 0; i < 100; i++) {
        const tx = mvcc.begin();
        mvcc.write(tx.txId, 'counter', i);
        mvcc.commit(tx.txId);
      }
      
      const before = mvcc.getStats();
      assert.strictEqual(before.totalVersions, 100);
      
      mvcc.vacuum();
      
      const after = mvcc.getStats();
      assert.strictEqual(after.totalVersions, 1);
      
      // Latest value should still be readable
      const reader = mvcc.begin();
      assert.strictEqual(mvcc.read(reader.txId, 'counter'), 99);
      mvcc.commit(reader.txId);
    });

    it('gc during concurrent reads and writes', () => {
      // Initial data
      const init = mvcc.begin();
      for (let i = 0; i < 10; i++) mvcc.write(init.txId, `k${i}`, `v0-${i}`);
      mvcc.commit(init.txId);
      
      // Start a long reader
      const reader = mvcc.begin();
      
      // Many updates
      for (let round = 1; round <= 5; round++) {
        const tx = mvcc.begin();
        for (let i = 0; i < 10; i++) mvcc.write(tx.txId, `k${i}`, `v${round}-${i}`);
        mvcc.commit(tx.txId);
      }
      
      // GC should clean some but preserve reader's snapshot
      const result = mvcc.gc();
      assert.ok(result.cleaned >= 0);
      
      // Reader should still see original values
      for (let i = 0; i < 10; i++) {
        assert.strictEqual(mvcc.read(reader.txId, `k${i}`), `v0-${i}`);
      }
      
      mvcc.commit(reader.txId);
      
      // After reader commits, vacuum should clean everything
      mvcc.vacuum();
      const stats = mvcc.getStats();
      assert.strictEqual(stats.totalVersions, 10); // 1 version per key
    });
  });
});
