// graph.js — Graph query engine for HenryDB
// Stores vertices and edges, supports BFS/DFS traversal, shortest path.

/**
 * Graph database module for HenryDB.
 * Stores nodes and edges as a property graph.
 */
export class GraphDB {
  constructor() {
    this._nodes = new Map(); // id → { properties }
    this._edges = new Map(); // id → { from, to, type, properties }
    this._adjacency = new Map(); // nodeId → [{ edgeId, target }]
    this._reverseAdj = new Map(); // nodeId → [{ edgeId, source }]
    this._nextEdgeId = 0;
  }

  /**
   * Add a node with properties.
   */
  addNode(id, properties = {}) {
    this._nodes.set(id, properties);
    if (!this._adjacency.has(id)) this._adjacency.set(id, []);
    if (!this._reverseAdj.has(id)) this._reverseAdj.set(id, []);
  }

  /**
   * Add a directed edge.
   */
  addEdge(from, to, type = 'RELATED', properties = {}) {
    const edgeId = this._nextEdgeId++;
    this._edges.set(edgeId, { from, to, type, properties });
    
    if (!this._adjacency.has(from)) this._adjacency.set(from, []);
    this._adjacency.get(from).push({ edgeId, target: to });
    
    if (!this._reverseAdj.has(to)) this._reverseAdj.set(to, []);
    this._reverseAdj.get(to).push({ edgeId, source: from });
    
    return edgeId;
  }

  /**
   * Get node properties.
   */
  getNode(id) {
    return this._nodes.get(id);
  }

  /**
   * Get outgoing neighbors of a node.
   */
  neighbors(nodeId, edgeType = null) {
    const adj = this._adjacency.get(nodeId) || [];
    if (edgeType) {
      return adj
        .filter(a => this._edges.get(a.edgeId).type === edgeType)
        .map(a => ({ node: a.target, edge: this._edges.get(a.edgeId) }));
    }
    return adj.map(a => ({ node: a.target, edge: this._edges.get(a.edgeId) }));
  }

  /**
   * BFS traversal from a start node.
   * Returns nodes in BFS order with depth.
   */
  bfs(startId, maxDepth = Infinity) {
    const visited = new Set();
    const queue = [{ id: startId, depth: 0 }];
    const result = [];
    
    while (queue.length > 0) {
      const { id, depth } = queue.shift();
      if (visited.has(id) || depth > maxDepth) continue;
      visited.add(id);
      result.push({ id, depth, properties: this._nodes.get(id) });
      
      for (const { target } of (this._adjacency.get(id) || [])) {
        if (!visited.has(target)) {
          queue.push({ id: target, depth: depth + 1 });
        }
      }
    }
    return result;
  }

  /**
   * DFS traversal from a start node.
   */
  dfs(startId, maxDepth = Infinity) {
    const visited = new Set();
    const result = [];
    
    const visit = (id, depth) => {
      if (visited.has(id) || depth > maxDepth) return;
      visited.add(id);
      result.push({ id, depth, properties: this._nodes.get(id) });
      
      for (const { target } of (this._adjacency.get(id) || [])) {
        visit(target, depth + 1);
      }
    };
    
    visit(startId, 0);
    return result;
  }

  /**
   * Shortest path using BFS (unweighted).
   */
  shortestPath(from, to) {
    const visited = new Set();
    const queue = [{ id: from, path: [from] }];
    
    while (queue.length > 0) {
      const { id, path } = queue.shift();
      if (id === to) return path;
      if (visited.has(id)) continue;
      visited.add(id);
      
      for (const { target } of (this._adjacency.get(id) || [])) {
        if (!visited.has(target)) {
          queue.push({ id: target, path: [...path, target] });
        }
      }
    }
    return null; // No path found
  }

  /**
   * Find all paths between two nodes (up to maxDepth).
   */
  allPaths(from, to, maxDepth = 10) {
    const paths = [];
    
    const dfs = (current, path, visited) => {
      if (current === to) { paths.push([...path]); return; }
      if (path.length > maxDepth || visited.has(current)) return;
      visited.add(current);
      
      for (const { target } of (this._adjacency.get(current) || [])) {
        dfs(target, [...path, target], new Set(visited));
      }
    };
    
    dfs(from, [from], new Set());
    return paths;
  }

  /**
   * Pattern matching: find nodes matching a property predicate.
   */
  findNodes(predicate) {
    const results = [];
    for (const [id, props] of this._nodes) {
      if (predicate(props, id)) results.push({ id, properties: props });
    }
    return results;
  }

  /**
   * Find edges by type.
   */
  findEdges(type) {
    const results = [];
    for (const [id, edge] of this._edges) {
      if (edge.type === type) results.push({ id, ...edge });
    }
    return results;
  }

  get nodeCount() { return this._nodes.size; }
  get edgeCount() { return this._edges.size; }
}
