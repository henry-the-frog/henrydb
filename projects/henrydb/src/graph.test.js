// graph.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Graph } from './graph.js';

describe('Graph', () => {
  it('BFS', () => {
    const g = new Graph(false);
    g.addEdge(1, 2); g.addEdge(1, 3); g.addEdge(2, 4); g.addEdge(3, 4);
    const order = g.bfs(1);
    assert.equal(order[0], 1);
    assert.ok(order.includes(4));
  });

  it('DFS', () => {
    const g = new Graph(false);
    g.addEdge(1, 2); g.addEdge(1, 3); g.addEdge(2, 4);
    const order = g.dfs(1);
    assert.equal(order[0], 1);
    assert.equal(order.length, 4);
  });

  it('topological sort', () => {
    const g = new Graph(true);
    g.addEdge('a', 'b'); g.addEdge('a', 'c'); g.addEdge('b', 'd'); g.addEdge('c', 'd');
    const order = g.topologicalSort();
    assert.ok(order.indexOf('a') < order.indexOf('b'));
    assert.ok(order.indexOf('a') < order.indexOf('c'));
    assert.ok(order.indexOf('b') < order.indexOf('d'));
  });

  it('cycle detection', () => {
    const g = new Graph(true);
    g.addEdge(1, 2); g.addEdge(2, 3); g.addEdge(3, 1);
    assert.equal(g.hasCycle(), true);
    
    const g2 = new Graph(true);
    g2.addEdge(1, 2); g2.addEdge(2, 3);
    assert.equal(g2.hasCycle(), false);
  });

  it('Dijkstra shortest path', () => {
    const g = new Graph(true);
    g.addEdge('A', 'B', 1); g.addEdge('A', 'C', 4);
    g.addEdge('B', 'C', 2); g.addEdge('B', 'D', 5);
    g.addEdge('C', 'D', 1);
    
    const { dist } = g.dijkstra('A');
    assert.equal(dist.A, 0);
    assert.equal(dist.B, 1);
    assert.equal(dist.C, 3);
    assert.equal(dist.D, 4);
  });

  it('use case: query plan DAG', () => {
    const plan = new Graph(true);
    plan.addEdge('scan_users', 'filter_age');
    plan.addEdge('scan_orders', 'join');
    plan.addEdge('filter_age', 'join');
    plan.addEdge('join', 'project');
    plan.addEdge('project', 'sort');
    
    const execOrder = plan.topologicalSort();
    assert.ok(execOrder.indexOf('scan_users') < execOrder.indexOf('join'));
    assert.ok(execOrder.indexOf('join') < execOrder.indexOf('sort'));
  });
});
