// scheduler.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DAG, WorkerPool } from './scheduler.js';

describe('DAG', () => {
  it('topological sort respects dependencies', () => {
    const dag = new DAG();
    dag.addNode('A');
    dag.addNode('B', null, ['A']);
    dag.addNode('C', null, ['A']);
    dag.addNode('D', null, ['B', 'C']);
    
    const sorted = dag.topologicalSort().map(n => n.id);
    assert.ok(sorted.indexOf('A') < sorted.indexOf('B'));
    assert.ok(sorted.indexOf('A') < sorted.indexOf('C'));
    assert.ok(sorted.indexOf('B') < sorted.indexOf('D'));
    assert.ok(sorted.indexOf('C') < sorted.indexOf('D'));
  });

  it('detects cycles', () => {
    const dag = new DAG();
    dag.addNode('A', null, ['C']);
    dag.addNode('B', null, ['A']);
    dag.addNode('C', null, ['B']);
    
    assert.throws(() => dag.topologicalSort(), /Cycle detected/);
  });

  it('parallel levels groups independent tasks', () => {
    const dag = new DAG();
    dag.addNode('scan_t1');
    dag.addNode('scan_t2');
    dag.addNode('join', null, ['scan_t1', 'scan_t2']);
    dag.addNode('project', null, ['join']);
    
    const levels = dag.parallelLevels();
    assert.equal(levels.length, 3);
    assert.equal(levels[0].length, 2); // scan_t1 and scan_t2 are parallel
    assert.equal(levels[1].length, 1); // join
    assert.equal(levels[2].length, 1); // project
  });

  it('query plan execution order', () => {
    const dag = new DAG();
    dag.addNode('scan_orders', { table: 'orders' });
    dag.addNode('scan_products', { table: 'products' });
    dag.addNode('filter_orders', { op: 'WHERE' }, ['scan_orders']);
    dag.addNode('hash_join', { op: 'JOIN' }, ['filter_orders', 'scan_products']);
    dag.addNode('aggregate', { op: 'GROUP BY' }, ['hash_join']);
    dag.addNode('sort', { op: 'ORDER BY' }, ['aggregate']);
    
    const sorted = dag.topologicalSort();
    assert.equal(sorted.length, 6);
    // Scans come before join
    const scanIdx = sorted.findIndex(n => n.id === 'scan_products');
    const joinIdx = sorted.findIndex(n => n.id === 'hash_join');
    assert.ok(scanIdx < joinIdx);
  });

  it('handles single node', () => {
    const dag = new DAG();
    dag.addNode('only');
    assert.equal(dag.topologicalSort().length, 1);
  });

  it('handles linear chain', () => {
    const dag = new DAG();
    dag.addNode('A');
    dag.addNode('B', null, ['A']);
    dag.addNode('C', null, ['B']);
    dag.addNode('D', null, ['C']);
    
    const sorted = dag.topologicalSort().map(n => n.id);
    assert.deepEqual(sorted, ['A', 'B', 'C', 'D']);
  });
});

describe('WorkerPool', () => {
  it('executes tasks and collects results', () => {
    const pool = new WorkerPool(2);
    pool.submit('t1', () => 42);
    pool.submit('t2', () => 'hello');
    pool.submit('t3', () => [1, 2, 3]);
    
    pool.executeAll();
    
    assert.equal(pool.getResult('t1').result, 42);
    assert.equal(pool.getResult('t2').result, 'hello');
    assert.deepEqual(pool.getResult('t3').result, [1, 2, 3]);
  });

  it('handles errors gracefully', () => {
    const pool = new WorkerPool(2);
    pool.submit('good', () => 'ok');
    pool.submit('bad', () => { throw new Error('oops'); });
    
    pool.executeAll();
    
    assert.equal(pool.getResult('good').status, 'ok');
    assert.equal(pool.getResult('bad').status, 'error');
    assert.equal(pool.getResult('bad').error, 'oops');
  });

  it('tracks completion count', () => {
    const pool = new WorkerPool(4);
    for (let i = 0; i < 10; i++) pool.submit(`t${i}`, () => i);
    pool.executeAll();
    assert.equal(pool.completedCount, 10);
  });
});
