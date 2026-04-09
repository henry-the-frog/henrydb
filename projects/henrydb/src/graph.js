// graph.js — Adjacency list graph with classic algorithms
// Supports directed/undirected, weighted edges.
// Used in: query plan optimization, dependency resolution, network analysis.

export class Graph {
  constructor(directed = true) {
    this._adj = new Map();
    this._directed = directed;
  }

  get nodeCount() { return this._adj.size; }
  get isDirected() { return this._directed; }

  addNode(node) {
    if (!this._adj.has(node)) this._adj.set(node, []);
  }

  addEdge(from, to, weight = 1) {
    this.addNode(from);
    this.addNode(to);
    this._adj.get(from).push({ to, weight });
    if (!this._directed) this._adj.get(to).push({ to: from, weight });
  }

  neighbors(node) { return (this._adj.get(node) || []).map(e => e.to); }

  /** BFS from source. Returns visited order. */
  bfs(start) {
    const visited = new Set();
    const order = [];
    const queue = [start];
    visited.add(start);
    while (queue.length > 0) {
      const node = queue.shift();
      order.push(node);
      for (const { to } of (this._adj.get(node) || [])) {
        if (!visited.has(to)) { visited.add(to); queue.push(to); }
      }
    }
    return order;
  }

  /** DFS from source. Returns visited order. */
  dfs(start) {
    const visited = new Set();
    const order = [];
    const stack = [start];
    while (stack.length > 0) {
      const node = stack.pop();
      if (visited.has(node)) continue;
      visited.add(node);
      order.push(node);
      for (const { to } of (this._adj.get(node) || []).reverse()) {
        if (!visited.has(to)) stack.push(to);
      }
    }
    return order;
  }

  /** Topological sort (Kahn's algorithm). Returns null if cycle. */
  topologicalSort() {
    if (!this._directed) return null;
    const inDegree = new Map();
    for (const [node] of this._adj) inDegree.set(node, 0);
    for (const [, edges] of this._adj) {
      for (const { to } of edges) inDegree.set(to, (inDegree.get(to) || 0) + 1);
    }
    
    const queue = [];
    for (const [node, deg] of inDegree) if (deg === 0) queue.push(node);
    
    const order = [];
    while (queue.length > 0) {
      const node = queue.shift();
      order.push(node);
      for (const { to } of (this._adj.get(node) || [])) {
        inDegree.set(to, inDegree.get(to) - 1);
        if (inDegree.get(to) === 0) queue.push(to);
      }
    }
    
    return order.length === this._adj.size ? order : null; // null = cycle
  }

  /** Shortest path using Dijkstra's algorithm. */
  dijkstra(start) {
    const dist = new Map();
    const prev = new Map();
    const visited = new Set();
    
    for (const [node] of this._adj) dist.set(node, Infinity);
    dist.set(start, 0);
    
    while (true) {
      // Find unvisited node with minimum distance
      let minNode = null, minDist = Infinity;
      for (const [node, d] of dist) {
        if (!visited.has(node) && d < minDist) { minNode = node; minDist = d; }
      }
      if (minNode === null) break;
      
      visited.add(minNode);
      for (const { to, weight } of (this._adj.get(minNode) || [])) {
        const alt = dist.get(minNode) + weight;
        if (alt < dist.get(to)) {
          dist.set(to, alt);
          prev.set(to, minNode);
        }
      }
    }
    
    return { dist: Object.fromEntries(dist), prev: Object.fromEntries(prev) };
  }

  /** Has cycle? */
  hasCycle() {
    if (!this._directed) return this._adj.size > 0 && this.topologicalSort() === null;
    return this.topologicalSort() === null;
  }
}
