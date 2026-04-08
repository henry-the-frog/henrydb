// deadlock-detector.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DeadlockDetector } from './deadlock-detector.js';

describe('DeadlockDetector', () => {
  it('no deadlock with no cycles', () => {
    const dd = new DeadlockDetector();
    dd.registerTxn('T1');
    dd.registerTxn('T2');
    dd.registerTxn('T3');
    dd.addWait('T1', 'T2'); // T1 waits for T2
    dd.addWait('T2', 'T3'); // T2 waits for T3
    
    const cycles = dd.detectDeadlocks();
    assert.equal(cycles.length, 0);
  });

  it('detects simple 2-way deadlock', () => {
    const dd = new DeadlockDetector();
    dd.registerTxn('T1');
    dd.registerTxn('T2');
    dd.addWait('T1', 'T2'); // T1 waits for T2
    dd.addWait('T2', 'T1'); // T2 waits for T1 → deadlock!
    
    const cycles = dd.detectDeadlocks();
    assert.ok(cycles.length > 0);
    assert.ok(cycles[0].includes('T1'));
    assert.ok(cycles[0].includes('T2'));
  });

  it('detects 3-way deadlock', () => {
    const dd = new DeadlockDetector();
    dd.registerTxn('A');
    dd.registerTxn('B');
    dd.registerTxn('C');
    dd.addWait('A', 'B');
    dd.addWait('B', 'C');
    dd.addWait('C', 'A'); // A→B→C→A cycle
    
    const cycles = dd.detectDeadlocks();
    assert.ok(cycles.length > 0);
  });

  it('resolves deadlock by aborting youngest', () => {
    const dd = new DeadlockDetector({ victimPolicy: 'youngest' });
    dd.registerTxn('T1', { startTime: 100 });
    dd.registerTxn('T2', { startTime: 200 }); // Youngest
    dd.addWait('T1', 'T2');
    dd.addWait('T2', 'T1');
    
    const victims = dd.resolveDeadlocks();
    assert.equal(victims.length, 1);
    assert.equal(victims[0], 'T2'); // Youngest aborted
  });

  it('resolves deadlock by aborting cheapest', () => {
    const dd = new DeadlockDetector({ victimPolicy: 'cheapest' });
    dd.registerTxn('T1', { cost: 100 });
    dd.registerTxn('T2', { cost: 10 }); // Cheapest
    dd.addWait('T1', 'T2');
    dd.addWait('T2', 'T1');
    
    const victims = dd.resolveDeadlocks();
    assert.equal(victims.length, 1);
    assert.equal(victims[0], 'T2'); // Cheapest aborted
  });

  it('remove wait edge breaks deadlock', () => {
    const dd = new DeadlockDetector();
    dd.registerTxn('T1');
    dd.registerTxn('T2');
    dd.addWait('T1', 'T2');
    dd.addWait('T2', 'T1');
    
    assert.ok(dd.detectDeadlocks().length > 0);
    
    dd.removeWait('T1', 'T2'); // Lock granted
    assert.equal(dd.detectDeadlocks().length, 0);
  });

  it('removeTxn cleans up', () => {
    const dd = new DeadlockDetector();
    dd.registerTxn('T1');
    dd.registerTxn('T2');
    dd.addWait('T1', 'T2');
    dd.addWait('T2', 'T1');
    
    dd.removeTxn('T1');
    assert.equal(dd.detectDeadlocks().length, 0);
    assert.equal(dd.getStats().activeTransactions, 1);
  });

  it('stats tracking', () => {
    const dd = new DeadlockDetector();
    dd.registerTxn('T1');
    dd.registerTxn('T2');
    dd.addWait('T1', 'T2');
    dd.addWait('T2', 'T1');
    
    dd.resolveDeadlocks();
    const stats = dd.getStats();
    assert.equal(stats.checks, 1);
    assert.equal(stats.deadlocksDetected, 1);
    assert.equal(stats.victimsChosen, 1);
  });

  it('no false positives after resolution', () => {
    const dd = new DeadlockDetector();
    dd.registerTxn('T1');
    dd.registerTxn('T2');
    dd.registerTxn('T3');
    dd.addWait('T1', 'T2');
    dd.addWait('T2', 'T1'); // Deadlock
    dd.addWait('T3', 'T1'); // T3 waits for T1, no deadlock with T3
    
    const victims = dd.resolveDeadlocks();
    assert.equal(victims.length, 1);
    
    // After resolution, no more deadlocks
    const cycles = dd.detectDeadlocks();
    assert.equal(cycles.length, 0);
  });
});
