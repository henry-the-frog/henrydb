// graph-ts-cdc.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GraphDB, TimeSeriesEngine, CDC } from './graph-ts-cdc.js';

describe('GraphDB', () => {
  it('add nodes and edges', () => {
    const g = new GraphDB();
    g.addNode('A'); g.addNode('B'); g.addNode('C');
    g.addEdge('A', 'B'); g.addEdge('A', 'C'); g.addEdge('B', 'C');
    assert.equal(g.nodeCount, 3);
    assert.equal(g.edgeCount, 3);
  });
  it('BFS', () => {
    const g = new GraphDB();
    g.addNode('A'); g.addNode('B'); g.addNode('C'); g.addNode('D');
    g.addEdge('A', 'B'); g.addEdge('A', 'C'); g.addEdge('B', 'D');
    assert.deepEqual(g.bfs('A'), ['A', 'B', 'C', 'D']);
  });
  it('DFS', () => {
    const g = new GraphDB();
    g.addNode('A'); g.addNode('B'); g.addNode('C');
    g.addEdge('A', 'B'); g.addEdge('A', 'C');
    const order = g.dfs('A');
    assert.equal(order[0], 'A');
    assert.equal(order.length, 3);
  });
  it('shortest path', () => {
    const g = new GraphDB();
    ['A','B','C','D','E'].forEach(n => g.addNode(n));
    g.addEdge('A', 'B'); g.addEdge('B', 'C'); g.addEdge('A', 'D'); g.addEdge('D', 'E'); g.addEdge('E', 'C');
    assert.deepEqual(g.shortestPath('A', 'C'), ['A', 'B', 'C']);
  });
  it('no path returns null', () => {
    const g = new GraphDB();
    g.addNode('A'); g.addNode('B');
    assert.equal(g.shortestPath('A', 'B'), null);
  });
});

describe('TimeSeriesEngine', () => {
  it('write and query', () => {
    const ts = new TimeSeriesEngine();
    ts.write('cpu', 50, 1000); ts.write('cpu', 60, 2000); ts.write('cpu', 70, 3000);
    const data = ts.query('cpu', 1000, 2000);
    assert.equal(data.length, 2);
  });
  it('downsample', () => {
    const ts = new TimeSeriesEngine();
    for (let i = 0; i < 100; i++) ts.write('temp', 20 + Math.random() * 10, i * 100);
    const ds = ts.downsample('temp', 0, 10000, 1000, 'avg');
    assert.equal(ds.length, 10);
    assert.ok(ds[0].v > 15 && ds[0].v < 35);
  });
  it('metrics list', () => {
    const ts = new TimeSeriesEngine();
    ts.write('cpu', 1); ts.write('mem', 2);
    assert.deepEqual(ts.metrics.sort(), ['cpu', 'mem']);
  });
});

describe('CDC', () => {
  it('capture events', () => {
    const cdc = new CDC();
    cdc.capture('users', 'INSERT', 1, null, { name: 'Alice' });
    cdc.capture('users', 'UPDATE', 1, { name: 'Alice' }, { name: 'Bob' });
    assert.equal(cdc.logSize, 2);
  });
  it('getChanges from sequence', () => {
    const cdc = new CDC();
    cdc.capture('users', 'INSERT', 1, null, { a: 1 });
    cdc.capture('users', 'INSERT', 2, null, { a: 2 });
    const changes = cdc.getChanges(1);
    assert.equal(changes.length, 1);
    assert.equal(changes[0].key, 2);
  });
  it('subscribe', () => {
    const cdc = new CDC();
    const events = [];
    cdc.subscribe(e => events.push(e));
    cdc.capture('orders', 'INSERT', 1, null, { total: 100 });
    assert.equal(events.length, 1);
  });
  it('filter by table', () => {
    const cdc = new CDC();
    cdc.capture('users', 'INSERT', 1, null, {});
    cdc.capture('orders', 'INSERT', 1, null, {});
    assert.equal(cdc.getChangesForTable('users').length, 1);
  });
});
