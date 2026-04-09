// vacuum.test.js
import { describe, test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { VacuumManager } from './vacuum.js';

let vm;

describe('VacuumManager', () => {
  beforeEach(() => {
    vm = new VacuumManager({ threshold: 10, scaleFactor: 0.2 });
    vm.registerTable('users', 100);
    vm.registerTable('orders', 50);
  });

  afterEach(() => { vm.stopDaemon(); });

  test('tracks dead tuples on DELETE', () => {
    vm.recordDML('users', 'DELETE', 5);
    const tracker = vm.getTracker('users');
    assert.equal(tracker.deadTuples, 5);
    assert.equal(tracker.liveTuples, 95);
  });

  test('tracks dead tuples on UPDATE', () => {
    vm.recordDML('users', 'UPDATE', 3);
    const tracker = vm.getTracker('users');
    assert.equal(tracker.deadTuples, 3);
    assert.equal(tracker.liveTuples, 100); // UPDATE doesn't change live count
  });

  test('tracks inserts', () => {
    vm.recordDML('users', 'INSERT', 10);
    const tracker = vm.getTracker('users');
    assert.equal(tracker.liveTuples, 110);
    assert.equal(tracker.deadTuples, 0);
  });

  test('VACUUM reclaims dead tuples', () => {
    vm.recordDML('users', 'DELETE', 20);
    const result = vm.vacuum('users');
    assert.equal(result.deadTuplesReclaimed, 20);
    
    const tracker = vm.getTracker('users');
    assert.equal(tracker.deadTuples, 0);
    assert.equal(tracker.vacuumCount, 1);
  });

  test('VACUUM ALL', () => {
    vm.recordDML('users', 'DELETE', 10);
    vm.recordDML('orders', 'DELETE', 5);
    
    const results = vm.vacuumAll();
    assert.equal(results.length, 2);
    assert.equal(vm.getTracker('users').deadTuples, 0);
    assert.equal(vm.getTracker('orders').deadTuples, 0);
  });

  test('bloat ratio calculation', () => {
    vm.recordDML('users', 'DELETE', 50); // 50 dead / 100 total = 50% before
    const tracker = vm.getTracker('users');
    // live=50, dead=50, total=100, ratio=50/100=0.5
    assert.ok(tracker.getBloatRatio() > 0.4);
  });

  test('auto-vacuum threshold check', () => {
    // threshold = 10 + 0.2 * liveTuples
    // After 15 deletes: live=85, threshold = 10 + 0.2*85 = 27, dead=15 < 27
    vm.recordDML('users', 'DELETE', 15);
    assert.equal(vm.checkAutoVacuum().length, 0);
    
    // After 20 more: live=65, threshold = 10 + 0.2*65 = 23, dead=35 > 23
    vm.recordDML('users', 'DELETE', 20);
    const candidates = vm.checkAutoVacuum();
    assert.equal(candidates.length, 1);
    assert.equal(candidates[0].table, 'users');
  });

  test('runAutoVacuum vacuums candidates', () => {
    vm.recordDML('users', 'DELETE', 50);
    const results = vm.runAutoVacuum();
    assert.equal(results.length, 1);
    assert.equal(vm.getTracker('users').deadTuples, 0);
  });

  test('bloat report sorted by ratio', () => {
    vm.recordDML('users', 'DELETE', 10);
    vm.recordDML('orders', 'DELETE', 30);
    
    const report = vm.getBloatReport();
    assert.equal(report.length, 2);
    // orders should have higher bloat ratio (30/50 > 10/100)
    assert.equal(report[0].table, 'orders');
  });

  test('stats tracking', () => {
    vm.recordDML('users', 'DELETE', 20);
    vm.vacuum('users');
    
    vm.recordDML('users', 'DELETE', 50);
    vm.runAutoVacuum();
    
    const stats = vm.getStats();
    assert.equal(stats.manualVacuums, 1);
    assert.equal(stats.autoVacuums, 1);
    assert.equal(stats.totalReclaimed, 70);
  });

  test('vacuum resets DML counters', () => {
    vm.recordDML('users', 'INSERT', 10);
    vm.recordDML('users', 'UPDATE', 5);
    vm.recordDML('users', 'DELETE', 3);
    
    vm.vacuum('users');
    
    const tracker = vm.getTracker('users');
    assert.equal(tracker.insertsSinceLastVacuum, 0);
    assert.equal(tracker.updatesSinceLastVacuum, 0);
    assert.equal(tracker.deletesSinceLastVacuum, 0);
  });

  test('case-insensitive table names', () => {
    vm.recordDML('USERS', 'DELETE', 5);
    const tracker = vm.getTracker('users');
    assert.equal(tracker.deadTuples, 5);
  });

  test('vacuum non-existent table throws', () => {
    assert.throws(() => vm.vacuum('nonexistent'), /not registered/);
  });
});
