// deadlock-detector.test.js
import { describe, test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { WaitForGraph, DeadlockDetector } from './deadlock-detector.js';

describe('WaitForGraph', () => {
  let graph;

  beforeEach(() => { graph = new WaitForGraph(); });

  test('no cycles in empty graph', () => {
    assert.deepEqual(graph.detectCycles(), []);
  });

  test('no cycle: A→B', () => {
    graph.addEdge('A', 'B');
    assert.deepEqual(graph.detectCycles(), []);
  });

  test('simple cycle: A→B→A', () => {
    graph.addEdge('A', 'B');
    graph.addEdge('B', 'A');
    const cycles = graph.detectCycles();
    assert.ok(cycles.length > 0);
  });

  test('3-way cycle: A→B→C→A', () => {
    graph.addEdge('A', 'B');
    graph.addEdge('B', 'C');
    graph.addEdge('C', 'A');
    const cycles = graph.detectCycles();
    assert.ok(cycles.length > 0);
    assert.ok(cycles[0].length >= 3);
  });

  test('no cycle: A→B, C→D', () => {
    graph.addEdge('A', 'B');
    graph.addEdge('C', 'D');
    assert.deepEqual(graph.detectCycles(), []);
  });

  test('remove edge breaks cycle', () => {
    graph.addEdge('A', 'B');
    graph.addEdge('B', 'A');
    graph.removeEdge('A', 'B');
    assert.deepEqual(graph.detectCycles(), []);
  });

  test('remove transaction removes all edges', () => {
    graph.addEdge('A', 'B');
    graph.addEdge('B', 'A');
    graph.addEdge('A', 'C');
    graph.removeTransaction('A');
    assert.equal(graph.size, 0);
  });

  test('getEdges returns all edges', () => {
    graph.addEdge('A', 'B', 'lock_1');
    graph.addEdge('B', 'C', 'lock_2');
    const edges = graph.getEdges();
    assert.equal(edges.length, 2);
    assert.ok(edges.some(e => e.waiter === 'A' && e.holder === 'B'));
  });
});

describe('DeadlockDetector', () => {
  let dd;

  beforeEach(() => { dd = new DeadlockDetector(); });
  afterEach(() => { dd.stopMonitoring(); });

  test('no deadlock with simple waits', () => {
    dd.registerTransaction('tx1');
    dd.registerTransaction('tx2');
    dd.recordWait('tx1', 'tx2', 'row_1');
    
    const results = dd.check();
    assert.equal(results.length, 0);
  });

  test('detects simple deadlock', () => {
    dd.registerTransaction('tx1', { statementsExecuted: 5, rowsModified: 100 });
    dd.registerTransaction('tx2', { statementsExecuted: 1, rowsModified: 10 });
    dd.recordWait('tx1', 'tx2', 'row_1');
    dd.recordWait('tx2', 'tx1', 'row_2');
    
    const results = dd.check();
    assert.equal(results.length, 1);
    assert.ok(results[0].cycle.includes('tx1'));
    assert.ok(results[0].cycle.includes('tx2'));
  });

  test('selects lowest-cost victim', () => {
    dd.registerTransaction('tx1', { statementsExecuted: 10, rowsModified: 500 }); // High cost
    dd.registerTransaction('tx2', { statementsExecuted: 1, rowsModified: 5 });    // Low cost
    dd.recordWait('tx1', 'tx2', 'row_1');
    dd.recordWait('tx2', 'tx1', 'row_2');
    
    const results = dd.check();
    assert.equal(results[0].victim, 'tx2'); // Lowest cost
  });

  test('3-way deadlock detection', () => {
    dd.registerTransaction('tx1', { statementsExecuted: 1 });
    dd.registerTransaction('tx2', { statementsExecuted: 2 });
    dd.registerTransaction('tx3', { statementsExecuted: 3 });
    
    dd.recordWait('tx1', 'tx2', 'row_a');
    dd.recordWait('tx2', 'tx3', 'row_b');
    dd.recordWait('tx3', 'tx1', 'row_c');
    
    const results = dd.check();
    assert.ok(results.length > 0);
    assert.ok(results[0].victim === 'tx1'); // Lowest statement count
  });

  test('resolving wait breaks potential deadlock', () => {
    dd.registerTransaction('tx1');
    dd.registerTransaction('tx2');
    dd.recordWait('tx1', 'tx2');
    dd.recordWait('tx2', 'tx1');
    
    assert.ok(dd.check().length > 0);
    
    dd.resolveWait('tx1', 'tx2'); // tx1 got the lock
    assert.equal(dd.check().length, 0);
  });

  test('removing transaction clears from graph', () => {
    dd.registerTransaction('tx1');
    dd.registerTransaction('tx2');
    dd.recordWait('tx1', 'tx2');
    dd.recordWait('tx2', 'tx1');
    
    dd.removeTransaction('tx1');
    assert.equal(dd.check().length, 0);
  });

  test('priority affects victim selection', () => {
    dd.registerTransaction('tx1', { statementsExecuted: 1, rowsModified: 5, priority: 10 }); // High priority
    dd.registerTransaction('tx2', { statementsExecuted: 1, rowsModified: 5, priority: 0 });  // Low priority
    dd.recordWait('tx1', 'tx2');
    dd.recordWait('tx2', 'tx1');
    
    const results = dd.check();
    assert.equal(results[0].victim, 'tx2'); // Lower priority chosen as victim
  });

  test('stats tracking', () => {
    dd.registerTransaction('tx1');
    dd.registerTransaction('tx2');
    dd.recordWait('tx1', 'tx2');
    dd.recordWait('tx2', 'tx1');
    
    dd.check();
    
    const stats = dd.getStats();
    assert.equal(stats.checksPerformed, 1);
    assert.equal(stats.deadlocksDetected, 1);
    assert.equal(stats.victimsSelected, 1);
  });
});
