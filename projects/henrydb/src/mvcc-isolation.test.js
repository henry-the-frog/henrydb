// mvcc-isolation.test.js — Comprehensive MVCC snapshot isolation tests
// Validates correct behavior for concurrent transaction scenarios.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCManager } from './mvcc.js';

describe('MVCC Snapshot Isolation', () => {
  describe('Dirty Read Prevention', () => {
    it('uncommitted writes are not visible to other transactions', () => {
      const mgr = new MVCCManager();
      const t1 = mgr.begin();
      const t2 = mgr.begin();
      
      mgr.write(t1, 'x', 42);
      
      assert.strictEqual(mgr.read(t2, 'x'), undefined,
        'T2 should not see T1 uncommitted write');
    });

    it('own writes are visible within same transaction', () => {
      const mgr = new MVCCManager();
      const t1 = mgr.begin();
      
      mgr.write(t1, 'x', 42);
      assert.strictEqual(mgr.read(t1, 'x'), 42,
        'T1 should see its own write');
    });
  });

  describe('Repeatable Read', () => {
    it('committed-after-snapshot writes are not visible (REPEATABLE READ)', () => {
      const mgr = new MVCCManager();
      const t1 = mgr.begin();
      const t2 = mgr.begin(); // T2 snapshot taken here
      
      mgr.write(t1, 'x', 'hello');
      t1.commit(); // T1 commits AFTER T2 began
      
      // T2 should NOT see T1's commit (snapshot isolation)
      assert.strictEqual(mgr.read(t2, 'x'), undefined,
        'T2 should not see T1 commit that happened after T2 began');
      t2.commit();
    });

    it('committed-before-snapshot writes ARE visible', () => {
      const mgr = new MVCCManager();
      const t1 = mgr.begin();
      mgr.write(t1, 'x', 'hello');
      t1.commit();
      
      const t2 = mgr.begin(); // T2 snapshot taken AFTER T1 committed
      assert.strictEqual(mgr.read(t2, 'x'), 'hello',
        'T2 should see T1 commit that happened before T2 began');
      t2.commit();
    });

    it('read returns same value throughout transaction', () => {
      const mgr = new MVCCManager();
      
      // Setup initial value
      const setup = mgr.begin();
      mgr.write(setup, 'x', 'initial');
      setup.commit();
      
      // T1 reads, T2 modifies, T1 reads again — should get same value
      const t1 = mgr.begin();
      assert.strictEqual(mgr.read(t1, 'x'), 'initial');
      
      const t2 = mgr.begin();
      mgr.write(t2, 'x', 'modified');
      t2.commit();
      
      assert.strictEqual(mgr.read(t1, 'x'), 'initial',
        'T1 should still see initial value after T2 committed');
      t1.commit();
    });
  });

  describe('Write-Write Conflict Detection', () => {
    it('detects concurrent writes to same key (both uncommitted)', () => {
      const mgr = new MVCCManager();
      
      const t1 = mgr.begin();
      const t2 = mgr.begin();
      
      mgr.write(t1, 'x', 1);
      
      assert.throws(() => mgr.write(t2, 'x', 2),
        /conflict/i,
        'Should detect write-write conflict');
    });

    it('detects write after concurrent commit', () => {
      const mgr = new MVCCManager();
      const setup = mgr.begin();
      mgr.write(setup, 'x', 0);
      setup.commit();
      
      const t1 = mgr.begin();
      const t2 = mgr.begin();
      
      mgr.write(t1, 'x', 1);
      t1.commit();
      
      assert.throws(() => mgr.write(t2, 'x', 2),
        /conflict/i,
        'Should detect write after concurrent commit');
    });

    it('allows writes to different keys', () => {
      const mgr = new MVCCManager();
      const t1 = mgr.begin();
      const t2 = mgr.begin();
      
      mgr.write(t1, 'x', 1);
      mgr.write(t2, 'y', 2); // Different key — no conflict
      
      t1.commit();
      t2.commit();
      
      const check = mgr.begin();
      assert.strictEqual(mgr.read(check, 'x'), 1);
      assert.strictEqual(mgr.read(check, 'y'), 2);
      check.commit();
    });

    it('prevents lost update', () => {
      const mgr = new MVCCManager();
      const setup = mgr.begin();
      mgr.write(setup, 'counter', 0);
      setup.commit();
      
      const t1 = mgr.begin();
      const t2 = mgr.begin();
      
      const v1 = mgr.read(t1, 'counter');
      const v2 = mgr.read(t2, 'counter');
      
      mgr.write(t1, 'counter', v1 + 1);
      t1.commit();
      
      // T2 should fail due to write-write conflict
      assert.throws(() => {
        mgr.write(t2, 'counter', v2 + 1);
      }, /conflict/i, 'Should prevent lost update');
    });
  });

  describe('Read Committed Isolation', () => {
    it('sees committed data after refresh', () => {
      const mgr = new MVCCManager();
      
      const t1 = mgr.begin();
      const t2 = mgr.begin({ isolationLevel: 'READ COMMITTED' });
      
      mgr.write(t1, 'x', 'hello');
      
      // Before commit: not visible
      assert.strictEqual(mgr.read(t2, 'x'), undefined);
      
      t1.commit();
      t2.refreshSnapshot();
      
      // After commit + refresh: visible in READ COMMITTED
      assert.strictEqual(mgr.read(t2, 'x'), 'hello',
        'READ COMMITTED should see committed data after refresh');
      t2.commit();
    });
  });

  describe('Multiple Versions', () => {
    it('different transactions see different versions', () => {
      const mgr = new MVCCManager();
      
      // Write v1
      const t1 = mgr.begin();
      mgr.write(t1, 'x', 'v1');
      t1.commit();
      
      // T2 starts, should see v1
      const t2 = mgr.begin();
      
      // Write v2
      const t3 = mgr.begin();
      mgr.write(t3, 'x', 'v2');
      t3.commit();
      
      // T4 starts, should see v2
      const t4 = mgr.begin();
      
      assert.strictEqual(mgr.read(t2, 'x'), 'v1',
        'T2 should still see v1');
      assert.strictEqual(mgr.read(t4, 'x'), 'v2',
        'T4 should see v2');
      
      t2.commit();
      t4.commit();
    });
  });

  describe('Rollback', () => {
    it('rolled back writes are not visible to new transactions', () => {
      const mgr = new MVCCManager();
      
      const t1 = mgr.begin();
      mgr.write(t1, 'x', 42);
      t1.rollback();
      
      const t2 = mgr.begin();
      assert.strictEqual(mgr.read(t2, 'x'), undefined,
        'Rolled back writes should not be visible');
      t2.commit();
    });
  });
});
