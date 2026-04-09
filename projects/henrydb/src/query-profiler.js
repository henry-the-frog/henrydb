// query-profiler.js — Per-node query profiling with timing, I/O, and memory tracking
// Instruments query plan nodes to collect actual execution statistics.
// Integrates with EXPLAIN ANALYZE for real performance analysis.

/**
 * ProfileNode — wraps a plan node to track execution statistics.
 */
class ProfileNode {
  constructor(operation, details = {}) {
    this.operation = operation;
    this.details = details;
    this.children = [];
    this.stats = {
      startTime: 0,
      endTime: 0,
      totalTimeMs: 0,
      selfTimeMs: 0,       // Time in this node, excluding children
      rowsEstimated: details.estimatedRows || 0,
      rowsActual: 0,
      loops: 0,
      rowsPerLoop: 0,
      // I/O counters
      pagesRead: 0,
      pagesWritten: 0,
      indexLookups: 0,
      // Memory
      peakMemoryBytes: 0,
      sortMemoryBytes: 0,
      hashMemoryBytes: 0,
      // Spill to disk
      spilledPages: 0,
    };
  }

  start() {
    this.stats.startTime = performance.now();
    this.stats.loops++;
  }

  end() {
    this.stats.endTime = performance.now();
    const elapsed = this.stats.endTime - this.stats.startTime;
    this.stats.totalTimeMs += elapsed;
  }

  addRow() {
    this.stats.rowsActual++;
  }

  addRows(count) {
    this.stats.rowsActual += count;
  }

  addPageRead(count = 1) {
    this.stats.pagesRead += count;
  }

  addPageWrite(count = 1) {
    this.stats.pagesWritten += count;
  }

  addIndexLookup(count = 1) {
    this.stats.indexLookups += count;
  }

  setMemory(bytes) {
    if (bytes > this.stats.peakMemoryBytes) {
      this.stats.peakMemoryBytes = bytes;
    }
  }

  finalize() {
    // Calculate self time (total - children)
    const childrenTime = this.children.reduce((sum, c) => sum + c.stats.totalTimeMs, 0);
    this.stats.selfTimeMs = Math.max(0, this.stats.totalTimeMs - childrenTime);
    this.stats.rowsPerLoop = this.stats.loops > 0 ? this.stats.rowsActual / this.stats.loops : 0;

    for (const child of this.children) {
      child.finalize();
    }
  }

  toJSON() {
    return {
      operation: this.operation,
      details: this.details,
      stats: {
        ...this.stats,
        totalTimeMs: +this.stats.totalTimeMs.toFixed(3),
        selfTimeMs: +this.stats.selfTimeMs.toFixed(3),
        rowsPerLoop: +this.stats.rowsPerLoop.toFixed(1),
      },
      children: this.children.map(c => c.toJSON()),
    };
  }
}

/**
 * QueryProfiler — instruments and profiles query execution.
 */
export class QueryProfiler {
  constructor() {
    this.root = null;
    this._nodeStack = [];
    this._allNodes = [];
  }

  /**
   * Create and push a new profile node.
   */
  beginNode(operation, details = {}) {
    const node = new ProfileNode(operation, details);
    
    if (this._nodeStack.length > 0) {
      this._nodeStack[this._nodeStack.length - 1].children.push(node);
    } else {
      this.root = node;
    }

    this._nodeStack.push(node);
    this._allNodes.push(node);
    node.start();
    return node;
  }

  /**
   * End the current profile node.
   */
  endNode() {
    const node = this._nodeStack.pop();
    if (node) node.end();
    return node;
  }

  /**
   * Get the current (innermost) node.
   */
  currentNode() {
    return this._nodeStack.length > 0 ? this._nodeStack[this._nodeStack.length - 1] : null;
  }

  /**
   * Finalize and generate the profile report.
   */
  getProfile() {
    if (this.root) {
      this.root.finalize();
    }

    return {
      plan: this.root ? this.root.toJSON() : null,
      summary: this._getSummary(),
    };
  }

  /**
   * Generate a formatted text report.
   */
  formatReport(options = {}) {
    if (!this.root) return 'No profile data';

    this.root.finalize();
    const lines = [];
    this._formatNode(this.root, 0, lines, options);
    
    lines.push('');
    lines.push('--- Summary ---');
    const summary = this._getSummary();
    lines.push(`Total time: ${summary.totalTimeMs.toFixed(3)} ms`);
    lines.push(`Total rows: ${summary.totalRows}`);
    lines.push(`Total pages read: ${summary.totalPagesRead}`);
    lines.push(`Total index lookups: ${summary.totalIndexLookups}`);
    if (summary.peakMemoryBytes > 0) {
      lines.push(`Peak memory: ${(summary.peakMemoryBytes / 1024).toFixed(1)} KB`);
    }
    lines.push(`Nodes: ${summary.nodeCount}`);
    
    if (options.showHotPath) {
      lines.push('');
      lines.push('--- Hot Path ---');
      const hotPath = this._findHotPath(this.root);
      for (const node of hotPath) {
        lines.push(`  → ${node.operation} (${node.stats.selfTimeMs.toFixed(3)} ms, ${node.stats.rowsActual} rows)`);
      }
    }

    return lines.join('\n');
  }

  _formatNode(node, depth, lines, options) {
    const indent = '  '.repeat(depth);
    const timeStr = `${node.stats.totalTimeMs.toFixed(3)} ms`;
    const selfStr = `${node.stats.selfTimeMs.toFixed(3)} ms self`;
    const rowStr = `rows=${node.stats.rowsActual}`;
    const loopStr = node.stats.loops > 1 ? ` loops=${node.stats.loops}` : '';
    
    let extra = '';
    if (node.stats.pagesRead > 0) extra += ` pages_read=${node.stats.pagesRead}`;
    if (node.stats.indexLookups > 0) extra += ` idx_lookups=${node.stats.indexLookups}`;
    if (node.stats.peakMemoryBytes > 0) extra += ` mem=${(node.stats.peakMemoryBytes / 1024).toFixed(1)}KB`;

    const detailStr = Object.entries(node.details)
      .filter(([k]) => k !== 'estimatedRows')
      .map(([k, v]) => `${k}=${v}`)
      .join(' ');

    lines.push(`${indent}→ ${node.operation} (${timeStr}, ${selfStr}, ${rowStr}${loopStr}${extra})${detailStr ? ' [' + detailStr + ']' : ''}`);

    if (options.showEstimates && node.details.estimatedRows) {
      const ratio = node.stats.rowsActual / (node.details.estimatedRows || 1);
      lines.push(`${indent}  estimated=${node.details.estimatedRows} ratio=${ratio.toFixed(2)}x`);
    }

    for (const child of node.children) {
      this._formatNode(child, depth + 1, lines, options);
    }
  }

  _getSummary() {
    let totalPagesRead = 0;
    let totalPagesWritten = 0;
    let totalIndexLookups = 0;
    let totalRows = 0;
    let peakMemory = 0;

    for (const node of this._allNodes) {
      totalPagesRead += node.stats.pagesRead;
      totalPagesWritten += node.stats.pagesWritten;
      totalIndexLookups += node.stats.indexLookups;
      totalRows += node.stats.rowsActual;
      if (node.stats.peakMemoryBytes > peakMemory) {
        peakMemory = node.stats.peakMemoryBytes;
      }
    }

    return {
      totalTimeMs: this.root ? this.root.stats.totalTimeMs : 0,
      totalRows,
      totalPagesRead,
      totalPagesWritten,
      totalIndexLookups,
      peakMemoryBytes: peakMemory,
      nodeCount: this._allNodes.length,
    };
  }

  _findHotPath(node) {
    const path = [node];
    if (node.children.length === 0) return path;

    // Follow the child with the highest self time
    let hottest = node.children[0];
    for (const child of node.children) {
      if (child.stats.selfTimeMs > hottest.stats.selfTimeMs) {
        hottest = child;
      }
    }

    return [...path, ...this._findHotPath(hottest)];
  }
}

/**
 * Simulate profiled query execution for testing.
 * In a real DB, the executor would call profiler methods during execution.
 */
export function profileQuery(db, sql) {
  const profiler = new QueryProfiler();

  // Simulate a plan with timing
  const start = performance.now();
  const result = db.execute(sql);
  const elapsed = performance.now() - start;

  // Create a simple profile from the result
  const upper = sql.trim().toUpperCase();
  let operation = 'SCAN';
  if (upper.includes('JOIN')) operation = 'JOIN';
  if (upper.includes('GROUP BY')) operation = 'AGGREGATE';
  if (upper.includes('ORDER BY')) operation = 'SORT';

  const node = profiler.beginNode(operation, {
    table: extractFirstTable(sql),
    estimatedRows: 100,
  });

  if (result && result.rows) {
    node.addRows(result.rows.length);
    // Estimate pages read (assume 100 rows per page)
    node.addPageRead(Math.ceil(result.rows.length / 100) || 1);
  }

  // Simulate memory usage
  const memEstimate = result?.rows ? result.rows.length * 100 : 0;
  node.setMemory(memEstimate);

  profiler.endNode();

  return {
    result,
    profile: profiler.getProfile(),
    report: profiler.formatReport({ showEstimates: true, showHotPath: true }),
  };
}

function extractFirstTable(sql) {
  const match = sql.match(/\bFROM\s+(\w+)/i);
  return match ? match[1] : 'unknown';
}
