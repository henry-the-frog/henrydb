// graph.test.js — Graph query engine tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GraphDB } from './graph.js';

describe('GraphDB', () => {
  function createSocialGraph() {
    const g = new GraphDB();
    g.addNode('alice', { name: 'Alice', age: 30 });
    g.addNode('bob', { name: 'Bob', age: 25 });
    g.addNode('charlie', { name: 'Charlie', age: 35 });
    g.addNode('diana', { name: 'Diana', age: 28 });
    
    g.addEdge('alice', 'bob', 'FRIEND');
    g.addEdge('alice', 'charlie', 'FRIEND');
    g.addEdge('bob', 'diana', 'FRIEND');
    g.addEdge('charlie', 'diana', 'COLLEAGUE');
    
    return g;
  }

  it('stores nodes and edges', () => {
    const g = createSocialGraph();
    assert.equal(g.nodeCount, 4);
    assert.equal(g.edgeCount, 4);
  });

  it('retrieves node properties', () => {
    const g = createSocialGraph();
    assert.equal(g.getNode('alice').name, 'Alice');
    assert.equal(g.getNode('alice').age, 30);
  });

  it('finds neighbors', () => {
    const g = createSocialGraph();
    const friends = g.neighbors('alice');
    assert.equal(friends.length, 2);
    assert.ok(friends.some(f => f.node === 'bob'));
    assert.ok(friends.some(f => f.node === 'charlie'));
  });

  it('filters neighbors by edge type', () => {
    const g = createSocialGraph();
    const colleagues = g.neighbors('charlie', 'COLLEAGUE');
    assert.equal(colleagues.length, 1);
    assert.equal(colleagues[0].node, 'diana');
  });

  it('BFS traversal', () => {
    const g = createSocialGraph();
    const result = g.bfs('alice');
    
    assert.equal(result.length, 4);
    assert.equal(result[0].id, 'alice');
    assert.equal(result[0].depth, 0);
    // Bob and Charlie at depth 1, Diana at depth 2
  });

  it('BFS with max depth', () => {
    const g = createSocialGraph();
    const result = g.bfs('alice', 1);
    
    assert.equal(result.length, 3); // Alice, Bob, Charlie (not Diana)
  });

  it('DFS traversal', () => {
    const g = createSocialGraph();
    const result = g.dfs('alice');
    assert.equal(result.length, 4);
    assert.equal(result[0].id, 'alice');
  });

  it('shortest path', () => {
    const g = createSocialGraph();
    const path = g.shortestPath('alice', 'diana');
    
    assert.ok(path);
    assert.equal(path[0], 'alice');
    assert.equal(path[path.length - 1], 'diana');
    assert.ok(path.length <= 3); // alice → bob → diana or alice → charlie → diana
  });

  it('no path returns null', () => {
    const g = new GraphDB();
    g.addNode('a');
    g.addNode('b');
    // No edges
    assert.equal(g.shortestPath('a', 'b'), null);
  });

  it('all paths', () => {
    const g = createSocialGraph();
    const paths = g.allPaths('alice', 'diana');
    
    assert.ok(paths.length >= 2); // Through Bob and through Charlie
  });

  it('find nodes by property', () => {
    const g = createSocialGraph();
    const young = g.findNodes(props => props.age < 30);
    assert.equal(young.length, 2); // Bob (25) and Diana (28)
  });

  it('find edges by type', () => {
    const g = createSocialGraph();
    const friends = g.findEdges('FRIEND');
    assert.equal(friends.length, 3);
  });

  it('knowledge graph use case', () => {
    const g = new GraphDB();
    g.addNode('js', { type: 'language', name: 'JavaScript' });
    g.addNode('py', { type: 'language', name: 'Python' });
    g.addNode('node', { type: 'runtime', name: 'Node.js' });
    g.addNode('react', { type: 'framework', name: 'React' });
    
    g.addEdge('node', 'js', 'USES');
    g.addEdge('react', 'js', 'USES');
    
    // What uses JavaScript?
    const users = g.findNodes((_, id) => {
      return g.neighbors(id, 'USES').some(n => n.node === 'js');
    });
    assert.equal(users.length, 2); // Node.js and React
  });
});
