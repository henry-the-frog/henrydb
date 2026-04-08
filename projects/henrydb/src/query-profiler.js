// query-profiler.js — Per-operator timing and row counts for EXPLAIN ANALYZE

export class QueryProfiler {
  constructor() { this._operators = []; }

  /** Wrap an operator function with timing */
  profileOp(name, fn) {
    const start = process.hrtime.bigint();
    const result = fn();
    const elapsed = Number(process.hrtime.bigint() - start) / 1e6; // ms
    const rows = Array.isArray(result) ? result.length : 0;
    this._operators.push({ name, elapsedMs: Math.round(elapsed * 100) / 100, rows });
    return result;
  }

  /** Get profiling results */
  getProfile() {
    const totalMs = this._operators.reduce((s, o) => s + o.elapsedMs, 0);
    return {
      operators: this._operators,
      totalMs: Math.round(totalMs * 100) / 100,
    };
  }

  /** Format as text table */
  toText() {
    const profile = this.getProfile();
    const lines = ['Operator                  Time(ms)  Rows', '-'.repeat(50)];
    for (const op of profile.operators) {
      lines.push(`${op.name.padEnd(26)}${String(op.elapsedMs).padStart(8)}  ${op.rows}`);
    }
    lines.push('-'.repeat(50));
    lines.push(`TOTAL                     ${String(profile.totalMs).padStart(8)}`);
    return lines.join('\n');
  }

  reset() { this._operators = []; }
}

/** Index Advisor — suggest indexes based on query workload */
export class IndexAdvisor {
  constructor() { this._queries = []; }

  /** Record a query with its predicates and accessed columns */
  recordQuery(query) {
    this._queries.push(query);
  }

  /** Analyze workload and suggest indexes */
  suggest() {
    const columnAccess = new Map(); // table.column → { eq: n, range: n, join: n }
    
    for (const q of this._queries) {
      if (q.predicates) {
        for (const pred of q.predicates) {
          const key = `${q.table}.${pred.column}`;
          if (!columnAccess.has(key)) columnAccess.set(key, { eq: 0, range: 0, join: 0, total: 0 });
          const stats = columnAccess.get(key);
          stats.total++;
          if (pred.type === 'EQ') stats.eq++;
          else if (pred.type === 'RANGE' || pred.type === 'GT' || pred.type === 'LT') stats.range++;
          else if (pred.type === 'JOIN') stats.join++;
        }
      }
    }

    const suggestions = [];
    for (const [key, stats] of columnAccess) {
      const [table, column] = key.split('.');
      if (stats.total >= 2) { // Column accessed at least twice
        const type = stats.range > stats.eq ? 'B-TREE' : stats.eq > 0 ? 'HASH' : 'B-TREE';
        suggestions.push({
          table,
          column,
          indexType: type,
          benefit: stats.total,
          reason: stats.range > 0 ? 'range queries' : stats.eq > 0 ? 'equality lookups' : 'join key',
        });
      }
    }

    return suggestions.sort((a, b) => b.benefit - a.benefit);
  }
}
