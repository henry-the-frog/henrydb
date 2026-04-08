// graph-db.js — Graph database primitives: nodes, edges, BFS, DFS, shortest path
export class GraphDB {
  constructor() { this._nodes = new Map(); this._edges = []; this._adj = new Map(); }
  
  addNode(id, props = {}) { this._nodes.set(id, props); if (!this._adj.has(id)) this._adj.set(id, []); }
  
  addEdge(from, to, props = {}) {
    this._edges.push({ from, to, ...props });
    if (!this._adj.has(from)) this._adj.set(from, []);
    this._adj.get(from).push({ to, ...props });
  }
  
  getNode(id) { return this._nodes.get(id); }
  
  neighbors(id) { return (this._adj.get(id) || []).map(e => e.to); }
  
  bfs(start) {
    const visited = new Set([start]);
    const queue = [start];
    const order = [];
    while (queue.length > 0) {
      const node = queue.shift();
      order.push(node);
      for (const neighbor of this.neighbors(node)) {
        if (!visited.has(neighbor)) { visited.add(neighbor); queue.push(neighbor); }
      }
    }
    return order;
  }
  
  dfs(start) {
    const visited = new Set();
    const order = [];
    const stack = [start];
    while (stack.length > 0) {
      const node = stack.pop();
      if (visited.has(node)) continue;
      visited.add(node);
      order.push(node);
      for (const neighbor of this.neighbors(node).reverse()) {
        if (!visited.has(neighbor)) stack.push(neighbor);
      }
    }
    return order;
  }
  
  shortestPath(start, end) {
    const prev = new Map();
    const visited = new Set([start]);
    const queue = [start];
    while (queue.length > 0) {
      const node = queue.shift();
      if (node === end) {
        const path = [];
        let curr = end;
        while (curr !== undefined) { path.unshift(curr); curr = prev.get(curr); }
        return path;
      }
      for (const neighbor of this.neighbors(node)) {
        if (!visited.has(neighbor)) { visited.add(neighbor); prev.set(neighbor, node); queue.push(neighbor); }
      }
    }
    return null;
  }
  
  get nodeCount() { return this._nodes.size; }
  get edgeCount() { return this._edges.length; }
}

// time-series.js — Time series engine
export class TimeSeriesEngine {
  constructor(retentionMs = Infinity) { this._series = new Map(); this.retentionMs = retentionMs; }
  
  write(metric, value, timestamp = Date.now()) {
    if (!this._series.has(metric)) this._series.set(metric, []);
    this._series.get(metric).push({ t: timestamp, v: value });
  }
  
  query(metric, startTime, endTime) {
    const series = this._series.get(metric) || [];
    return series.filter(p => p.t >= startTime && p.t <= endTime);
  }
  
  downsample(metric, startTime, endTime, intervalMs, agg = 'avg') {
    const points = this.query(metric, startTime, endTime);
    const buckets = new Map();
    for (const p of points) {
      const bucket = Math.floor(p.t / intervalMs) * intervalMs;
      if (!buckets.has(bucket)) buckets.set(bucket, []);
      buckets.get(bucket).push(p.v);
    }
    return [...buckets.entries()].map(([t, vals]) => {
      let v;
      switch (agg) {
        case 'avg': v = vals.reduce((a, b) => a + b, 0) / vals.length; break;
        case 'sum': v = vals.reduce((a, b) => a + b, 0); break;
        case 'min': v = Math.min(...vals); break;
        case 'max': v = Math.max(...vals); break;
        case 'count': v = vals.length; break;
      }
      return { t, v };
    }).sort((a, b) => a.t - b.t);
  }
  
  enforceRetention() {
    const cutoff = Date.now() - this.retentionMs;
    for (const [metric, series] of this._series) {
      this._series.set(metric, series.filter(p => p.t >= cutoff));
    }
  }
  
  get metrics() { return [...this._series.keys()]; }
}

// cdc.js — Change Data Capture
export class CDC {
  constructor() { this._log = []; this._subscribers = []; this._seq = 0; }
  
  capture(table, op, key, before, after) {
    const event = { seq: ++this._seq, table, op, key, before, after, timestamp: Date.now() };
    this._log.push(event);
    for (const sub of this._subscribers) sub(event);
    return event;
  }
  
  subscribe(callback) { this._subscribers.push(callback); return this._subscribers.length - 1; }
  unsubscribe(idx) { this._subscribers[idx] = () => {}; }
  
  getChanges(fromSeq = 0, limit = 100) {
    return this._log.filter(e => e.seq > fromSeq).slice(0, limit);
  }
  
  getChangesForTable(table, fromSeq = 0) {
    return this._log.filter(e => e.table === table && e.seq > fromSeq);
  }
  
  get logSize() { return this._log.length; }
}
