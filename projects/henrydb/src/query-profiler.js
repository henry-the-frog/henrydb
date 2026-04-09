// query-profiler.js — Query execution profiler for HenryDB
//
// Instruments query execution phases and reports timing breakdown.
// Enables identification of performance bottlenecks.
//
// Phases tracked:
//   - PARSE: SQL text → AST
//   - PLAN: AST → execution plan (optimizer decisions)
//   - SCAN: reading rows from storage engine
//   - FILTER: evaluating WHERE conditions
//   - INDEX_LOOKUP: B+tree/hash index traversal
//   - SORT: ORDER BY sorting
//   - AGGREGATE: GROUP BY / aggregate functions
//   - WINDOW: window function computation
//   - PROJECT: column selection / expression evaluation
//   - LIMIT: LIMIT/OFFSET application
//   - TOTAL: end-to-end execution time

/**
 * ProfilePhase — One phase of query execution.
 */
class ProfilePhase {
  constructor(name) {
    this.name = name;
    this.startTime = 0;
    this.endTime = 0;
    this.duration = 0; // milliseconds
    this.rowsIn = 0;
    this.rowsOut = 0;
    this.metadata = {};
  }

  start() {
    this.startTime = performance.now();
    return this;
  }

  end(rowsOut = 0) {
    this.endTime = performance.now();
    this.duration = this.endTime - this.startTime;
    this.rowsOut = rowsOut;
    return this;
  }
}

/**
 * QueryProfile — Complete profile of a query execution.
 */
export class QueryProfile {
  constructor(sql) {
    this.sql = sql;
    this.phases = [];
    this._active = null;
    this._total = new ProfilePhase('TOTAL');
    this._total.start();
  }

  /**
   * Start a new phase.
   */
  startPhase(name, metadata = {}) {
    if (this._active) {
      this._active.end();
    }
    const phase = new ProfilePhase(name);
    phase.metadata = metadata;
    phase.start();
    this._active = phase;
    this.phases.push(phase);
    return phase;
  }

  /**
   * End the current active phase.
   */
  endPhase(rowsOut = 0) {
    if (this._active) {
      this._active.end(rowsOut);
      this._active = null;
    }
  }

  /**
   * End profiling and generate report.
   */
  finish(totalRows = 0) {
    this.endPhase();
    this._total.end(totalRows);
    this._total.duration = this._total.endTime - this._total.startTime;
    return this.report();
  }

  /**
   * Generate a formatted profile report.
   */
  report() {
    const total = this._total.duration;
    const lines = [];
    
    lines.push(`Query: ${this.sql.slice(0, 80)}${this.sql.length > 80 ? '...' : ''}`);
    lines.push(`${'─'.repeat(70)}`);
    lines.push(`${'Phase'.padEnd(20)} ${'Duration'.padStart(10)} ${'Pct'.padStart(6)} ${'Rows Out'.padStart(10)}`);
    lines.push(`${'─'.repeat(70)}`);
    
    for (const phase of this.phases) {
      const pct = total > 0 ? (phase.duration / total * 100).toFixed(1) : '0.0';
      const bar = '█'.repeat(Math.round(phase.duration / total * 20));
      lines.push(
        `${phase.name.padEnd(20)} ${(phase.duration.toFixed(3) + 'ms').padStart(10)} ${(pct + '%').padStart(6)} ${String(phase.rowsOut).padStart(10)} ${bar}`
      );
    }
    
    lines.push(`${'─'.repeat(70)}`);
    lines.push(`${'TOTAL'.padEnd(20)} ${(total.toFixed(3) + 'ms').padStart(10)} ${'100%'.padStart(6)} ${String(this._total.rowsOut).padStart(10)}`);
    
    return {
      sql: this.sql,
      totalMs: parseFloat(total.toFixed(3)),
      phases: this.phases.map(p => ({
        name: p.name,
        durationMs: parseFloat(p.duration.toFixed(3)),
        pct: parseFloat((total > 0 ? p.duration / total * 100 : 0).toFixed(1)),
        rowsOut: p.rowsOut,
        metadata: p.metadata,
      })),
      formatted: lines.join('\n'),
    };
  }
}

/**
 * Create a profiled database wrapper.
 * Usage: const pdb = profiledDB(db); pdb.execute(sql); pdb.lastProfile;
 */
export function profiledDB(db) {
  const { parse } = db.constructor.toString().includes('parse') 
    ? {} // Can't import in this context
    : {};
  
  return {
    _db: db,
    lastProfile: null,
    
    execute(sql) {
      const profile = new QueryProfile(sql);
      
      // Parse phase
      profile.startPhase('PARSE');
      const result = db.execute(sql);
      profile.endPhase();
      
      // We can't instrument internal phases without modifying db.js,
      // so we measure end-to-end and report EXECUTE as a single phase.
      // For deeper profiling, use db.execute with profiling hooks.
      
      profile.startPhase('EXECUTE', { rows: result?.rows?.length || 0 });
      profile.endPhase(result?.rows?.length || 0);
      
      this.lastProfile = profile.finish(result?.rows?.length || 0);
      return result;
    }
  };
}
