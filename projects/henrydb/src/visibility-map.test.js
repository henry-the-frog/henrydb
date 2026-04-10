// visibility-map.test.js — Tests for visibility map
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { VisibilityMap, TableVisibilityMap } from './visibility-map.js';

describe('VisibilityMap', () => {
  let vm;

  beforeEach(() => {
    vm = new VisibilityMap();
  });

  it('defaults to not all-visible', () => {
    assert.strictEqual(vm.isAllVisible(0), false);
    assert.strictEqual(vm.isAllVisible(100), false);
  });

  it('marks page as all-visible', () => {
    vm.setAllVisible(5);
    assert.strictEqual(vm.isAllVisible(5), true);
    assert.strictEqual(vm.isAllVisible(6), false);
  });

  it('clears individual page', () => {
    vm.setAllVisible(5);
    vm.clearPage(5);
    assert.strictEqual(vm.isAllVisible(5), false);
  });

  it('clears all pages', () => {
    vm.setAllVisible(1);
    vm.setAllVisible(2);
    vm.setAllVisible(3);
    vm.clearAll();
    assert.strictEqual(vm.visibleCount, 0);
  });

  it('tracks hit/miss statistics', () => {
    vm.setAllVisible(1);
    vm.isAllVisible(1); // hit
    vm.isAllVisible(2); // miss
    vm.isAllVisible(1); // hit
    
    const stats = vm.getStats();
    assert.strictEqual(stats.hits, 2);
    assert.strictEqual(stats.misses, 1);
    assert.ok(Math.abs(stats.hitRate - 2/3) < 0.01);
  });

  it('handles many pages', () => {
    for (let i = 0; i < 1000; i++) vm.setAllVisible(i);
    assert.strictEqual(vm.visibleCount, 1000);
    for (let i = 0; i < 1000; i++) {
      assert.strictEqual(vm.isAllVisible(i), true);
    }
  });

  it('invalidation pattern: set, modify, re-vacuum', () => {
    // VACUUM marks pages as visible
    vm.setAllVisible(1);
    vm.setAllVisible(2);
    vm.setAllVisible(3);
    assert.strictEqual(vm.visibleCount, 3);
    
    // UPDATE invalidates page 2
    vm.clearPage(2);
    assert.strictEqual(vm.visibleCount, 2);
    assert.strictEqual(vm.isAllVisible(2), false);
    
    // Re-VACUUM marks page 2 visible again
    vm.setAllVisible(2);
    assert.strictEqual(vm.visibleCount, 3);
  });
});

describe('TableVisibilityMap', () => {
  let tvm;

  beforeEach(() => {
    tvm = new TableVisibilityMap();
  });

  it('manages separate maps per table', () => {
    tvm.setAllVisible('users', 1);
    tvm.setAllVisible('orders', 1);
    
    assert.strictEqual(tvm.isAllVisible('users', 1), true);
    assert.strictEqual(tvm.isAllVisible('orders', 1), true);
    assert.strictEqual(tvm.isAllVisible('products', 1), false);
  });

  it('invalidation is per-table', () => {
    tvm.setAllVisible('users', 1);
    tvm.setAllVisible('orders', 1);
    
    tvm.onPageModified('users', 1);
    
    assert.strictEqual(tvm.isAllVisible('users', 1), false);
    assert.strictEqual(tvm.isAllVisible('orders', 1), true); // not affected
  });

  it('provides per-table stats', () => {
    tvm.setAllVisible('users', 1);
    tvm.isAllVisible('users', 1); // hit
    tvm.isAllVisible('users', 2); // miss
    
    const stats = tvm.getStats();
    assert.ok(stats.users);
    assert.strictEqual(stats.users.hits, 1);
    assert.strictEqual(stats.users.misses, 1);
    assert.strictEqual(stats.users.visiblePages, 1);
  });

  it('handles concurrent modifications across tables', () => {
    for (let i = 0; i < 100; i++) {
      tvm.setAllVisible('big_table', i);
    }
    
    // Simulate batch of updates
    for (let i = 0; i < 50; i += 2) {
      tvm.onPageModified('big_table', i);
    }
    
    const map = tvm.getMap('big_table');
    assert.strictEqual(map.visibleCount, 75); // 100 - 25 modified
  });
});
