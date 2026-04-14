// planner.js — Cost-based query optimizer for HenryDB
// Features: column histograms, selectivity estimation, DP join reordering, cost model

const PAGE_SIZE = 4096;
const TUPLE_OVERHEAD = 16; // header per tuple
const IO_COST = 1.0;      // sequential page read
const RANDOM_IO_COST = 4.0; // random page read (index lookup)
const CPU_TUPLE_COST = 0.01;
const CPU_OPERATOR_COST = 0.0025;
const HASH_BUILD_COST = 0.02;

// ===== Column Statistics with Histograms =====

export class ColumnStats {
  constructor(columnName, values) {
    this.column = columnName;
    this.rowCount = values.length;
    this.nullCount = values.filter(v => v == null).length;
    this.nonNullValues = values.filter(v => v != null);
    this.ndv = new Set(this.nonNullValues).size; // number of distinct values
    
    if (this.nonNullValues.length === 0) {
      this.min = null;
      this.max = null;
      this.histogram = [];
      this.avgWidth = 0;
      return;
    }

    // Sort for histogram building
    const sorted = [...this.nonNullValues].sort((a, b) => {
      if (typeof a === 'string' && typeof b === 'string') return a.localeCompare(b);
      return a < b ? -1 : a > b ? 1 : 0;
    });
    
    this.min = sorted[0];
    this.max = sorted[sorted.length - 1];
    this.avgWidth = this.nonNullValues.reduce((s, v) => s + estimateSize(v), 0) / this.nonNullValues.length;
    
    // Build equi-depth histogram (10 buckets by default)
    this.histogram = this._buildHistogram(sorted, 10);
    
    // Most common values (MCV) — top 10 most frequent values
    this.mcv = this._buildMCV(this.nonNullValues, 10);
  }

  toJSON() {
    return {
      column: this.column,
      rowCount: this.rowCount,
      nullCount: this.nullCount,
      ndv: this.ndv,
      min: this.min,
      max: this.max,
      avgWidth: this.avgWidth,
      histogram: this.histogram,
      mcv: this.mcv,
    };
  }

  static fromJSON(data) {
    const stats = Object.create(ColumnStats.prototype);
    stats.column = data.column;
    stats.rowCount = data.rowCount;
    stats.nullCount = data.nullCount;
    stats.nonNullValues = []; // Not persisted (too large)
    stats.ndv = data.ndv;
    stats.min = data.min;
    stats.max = data.max;
    stats.avgWidth = data.avgWidth;
    stats.histogram = data.histogram;
    stats.mcv = data.mcv;
    return stats;
  }

  _buildHistogram(sorted, numBuckets) {
    if (sorted.length <= numBuckets) {
      // Fewer values than buckets — one value per bucket
      return sorted.map((v, i) => ({
        low: v,
        high: v,
        count: 1,
        ndv: 1,
      }));
    }

    const bucketSize = Math.ceil(sorted.length / numBuckets);
    const buckets = [];
    
    for (let i = 0; i < numBuckets; i++) {
      const start = i * bucketSize;
      const end = Math.min(start + bucketSize, sorted.length);
      if (start >= sorted.length) break;
      
      const bucketValues = sorted.slice(start, end);
      buckets.push({
        low: bucketValues[0],
        high: bucketValues[bucketValues.length - 1],
        count: bucketValues.length,
        ndv: new Set(bucketValues).size,
      });
    }
    
    return buckets;
  }

  _buildMCV(values, topN) {
    const freq = new Map();
    for (const v of values) {
      freq.set(v, (freq.get(v) || 0) + 1);
    }
    
    return [...freq.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, topN)
      .map(([value, count]) => ({ value, count, frequency: count / values.length }));
  }

  // Estimate selectivity for equality: value = constant
  selectivityEq(value) {
    if (this.nonNullValues.length === 0) return 0;
    
    // Check MCV first
    const mcvEntry = this.mcv.find(m => m.value === value);
    if (mcvEntry) return mcvEntry.frequency;
    
    // Fall back to uniform assumption within NDV
    if (this.ndv === 0) return 0;
    return 1 / this.ndv;
  }

  // Estimate selectivity for range: column < value
  selectivityLT(value) {
    if (this.nonNullValues.length === 0) return 0;
    if (value <= this.min) return 0;
    if (value > this.max) return 1;
    
    // Use histogram
    let matchingRows = 0;
    for (const bucket of this.histogram) {
      if (value > bucket.high) {
        matchingRows += bucket.count;
      } else if (value > bucket.low) {
        // Interpolate within bucket (linear assumption)
        const range = bucket.high - bucket.low || 1;
        const fraction = (value - bucket.low) / range;
        matchingRows += bucket.count * fraction;
      }
    }
    
    return matchingRows / this.nonNullValues.length;
  }

  // Estimate selectivity for range: column > value
  selectivityGT(value) {
    return 1 - this.selectivityLT(value) - this.selectivityEq(value);
  }

  // Estimate selectivity for BETWEEN low AND high
  selectivityBetween(low, high) {
    return this.selectivityLT(high) - this.selectivityLT(low) + this.selectivityEq(high);
  }

  // Estimate selectivity for LIKE pattern
  selectivityLike(pattern) {
    if (pattern.startsWith('%')) return 0.5; // Leading wildcard — poor selectivity
    // Prefix match — estimate based on prefix length
    const prefix = pattern.split('%')[0].split('_')[0];
    if (prefix.length === 0) return 0.5;
    return Math.max(0.01, 1 / Math.pow(26, Math.min(prefix.length, 3)));
  }

  // NULL selectivity
  selectivityNull() {
    if (this.rowCount === 0) return 0;
    return this.nullCount / this.rowCount;
  }
}

// ===== Table Statistics =====

export class TableStats {
  constructor(tableName, table) {
    this.table = tableName;
    this.rowCount = 0;
    this.pageCount = table.heap.pageCount || 1;
    this.avgRowWidth = 0;
    this.columns = new Map(); // columnName → ColumnStats
    this.indexedColumns = [...table.indexes.keys()];
    this.expressionIndexes = [];
    // Collect expression index metadata
    if (table.indexMeta) {
      for (const [key, meta] of table.indexMeta) {
        if (meta.expressions && meta.expressions.some(e => e !== null)) {
          this.expressionIndexes.push({
            key,
            name: meta.name,
            expressions: meta.expressions,
            columns: meta.columns,
            unique: meta.unique,
          });
        }
      }
    }
    
    // Collect all rows and column values
    const columnValues = new Map(); // columnName → values[]
    for (const col of table.schema) {
      columnValues.set(col.name, []);
    }
    
    let totalWidth = 0;
    for (const { values } of table.heap.scan()) {
      this.rowCount++;
      for (let i = 0; i < table.schema.length; i++) {
        columnValues.get(table.schema[i].name).push(values[i]);
      }
      totalWidth += values.reduce((s, v) => s + estimateSize(v), 0) + TUPLE_OVERHEAD;
    }
    
    this.avgRowWidth = this.rowCount > 0 ? totalWidth / this.rowCount : 0;
    this.tuplesPerPage = this.avgRowWidth > 0 ? Math.floor(PAGE_SIZE / this.avgRowWidth) : 100;
    
    // Build per-column statistics
    for (const [colName, values] of columnValues) {
      this.columns.set(colName, new ColumnStats(colName, values));
    }
  }

  toJSON() {
    const columns = {};
    for (const [name, stats] of this.columns) {
      columns[name] = stats.toJSON();
    }
    return {
      table: this.table,
      rowCount: this.rowCount,
      pageCount: this.pageCount,
      avgRowWidth: this.avgRowWidth,
      tuplesPerPage: this.tuplesPerPage,
      indexedColumns: this.indexedColumns,
      columns,
    };
  }

  static fromJSON(data) {
    const stats = Object.create(TableStats.prototype);
    stats.table = data.table;
    stats.rowCount = data.rowCount;
    stats.pageCount = data.pageCount;
    stats.avgRowWidth = data.avgRowWidth;
    stats.tuplesPerPage = data.tuplesPerPage;
    stats.indexedColumns = data.indexedColumns || [];
    stats.columns = new Map();
    for (const [name, colData] of Object.entries(data.columns)) {
      stats.columns.set(name, ColumnStats.fromJSON(colData));
    }
    return stats;
  }
}

function estimateSize(v) {
  if (v == null) return 1;
  if (typeof v === 'number') return 8;
  if (typeof v === 'string') return v.length * 2 + 4;
  if (typeof v === 'boolean') return 1;
  return 8;
}

// ===== Cost-Based Query Optimizer =====

export class QueryPlanner {
  constructor(database) {
    this.db = database;
    this.statsCache = new Map(); // tableName → TableStats
  }

  // Gather (or refresh) statistics for a table
  analyzeTable(tableName) {
    const table = this.db.tables.get(tableName);
    if (!table) throw new Error(`Table ${tableName} not found`);
    const stats = new TableStats(tableName, table);
    this.statsCache.set(tableName, stats);
    return stats;
  }

  getStats(tableName) {
    return this.statsCache.get(tableName) || this.analyzeTable(tableName);
  }

  // Main entry: plan a SELECT query
  plan(ast) {
    if (ast.type !== 'SELECT') return { strategy: 'direct' };

    const tableName = ast.from.table;
    const tableStats = this.getStats(tableName);
    const joins = ast.joins || [];

    // Single-table plan
    const accessPlan = this._planAccess(tableName, tableStats, ast.where);

    if (joins.length === 0) {
      return {
        table: tableName,
        ...accessPlan,
        joins: [],
      };
    }

    // Multi-table: DP join reordering
    const tables = [{ name: tableName, alias: ast.from.alias || tableName, stats: tableStats }];
    for (const join of joins) {
      const jStats = this.getStats(join.table);
      tables.push({ name: join.table, alias: join.alias || join.table, stats: jStats, joinSpec: join });
    }

    const joinPlan = this._dpJoinReorder(tables, joins, ast.where);
    return {
      table: tableName,
      ...accessPlan,
      joins: joinPlan.steps,
      totalCost: joinPlan.totalCost,
      joinOrder: joinPlan.order,
    };
  }

  // Access path selection for a single table
  _planAccess(tableName, stats, where) {
    const tableScanCost = stats.pageCount * IO_COST + stats.rowCount * CPU_TUPLE_COST;
    
    let bestPlan = {
      scanType: 'TABLE_SCAN',
      estimatedCost: tableScanCost,
      estimatedRows: stats.rowCount,
    };

    // Apply WHERE selectivity
    if (where) {
      const selectivity = this._estimateSelectivity(where, stats);
      bestPlan.estimatedRows = Math.max(1, Math.round(stats.rowCount * selectivity));
    }

    // Check index scans
    if (where) {
      const indexPlans = this._planIndexScans(where, tableName, stats);
      for (const ip of indexPlans) {
        if (ip.cost < bestPlan.estimatedCost) {
          bestPlan = {
            scanType: ip.type,
            indexColumn: ip.column,
            estimatedCost: ip.cost,
            estimatedRows: ip.estimatedRows,
            residualFilter: ip.residual,
          };
        }
      }
    }

    return bestPlan;
  }

  // Generate candidate index scan plans
  _planIndexScans(expr, tableName, stats) {
    const plans = [];

    if (expr.type === 'COMPARE' && expr.left?.type === 'column_ref') {
      const col = expr.left.name;
      const colStats = stats.columns.get(col);
      if (!colStats) return plans;

      if (stats.indexedColumns.includes(col)) {
        // B+tree fan-out ~100-200, so height = log_100(N)
        const fanOut = 100;
        const treeHeight = Math.max(1, Math.ceil(Math.log(stats.rowCount + 1) / Math.log(fanOut)));
        
        if (expr.op === 'EQ') {
          const selectivity = colStats.selectivityEq(expr.right?.value);
          const matchingRows = Math.max(1, Math.round(stats.rowCount * selectivity));
          const cost = treeHeight * RANDOM_IO_COST + matchingRows * CPU_TUPLE_COST;
          plans.push({
            type: 'INDEX_SCAN',
            column: col,
            cost,
            estimatedRows: matchingRows,
          });
        } else if (['LT', 'GT', 'LE', 'GE'].includes(expr.op)) {
          let selectivity;
          const val = expr.right?.value;
          if (expr.op === 'LT' || expr.op === 'LE') selectivity = colStats.selectivityLT(val);
          else selectivity = colStats.selectivityGT(val);
          if (expr.op === 'LE' || expr.op === 'GE') selectivity += colStats.selectivityEq(val);
          
          const matchingRows = Math.max(1, Math.round(stats.rowCount * selectivity));
          const pagesRead = Math.max(1, Math.ceil(matchingRows / stats.tuplesPerPage));
          const cost = treeHeight * RANDOM_IO_COST + pagesRead * IO_COST + matchingRows * CPU_TUPLE_COST;
          plans.push({
            type: 'INDEX_RANGE_SCAN',
            column: col,
            cost,
            estimatedRows: matchingRows,
          });
        }
      }
    }

    // Check expression indexes: match WHERE expr = value against expression indexes
    if (expr.type === 'COMPARE' && stats.expressionIndexes) {
      for (const exprIdx of stats.expressionIndexes) {
        if (exprIdx.expressions.length === 1 && exprIdx.expressions[0]) {
          const idxExpr = exprIdx.expressions[0];
          if (this._exprMatchesIndex(expr.left, idxExpr)) {
            const fanOut = 100;
            const treeHeight = Math.max(1, Math.ceil(Math.log(stats.rowCount + 1) / Math.log(fanOut)));
            
            if (expr.op === 'EQ') {
              // Assume uniform distribution for expression indexes (we don't have per-expression stats)
              const ndv = Math.max(1, stats.rowCount / 10);
              const selectivity = 1 / ndv;
              const matchingRows = Math.max(1, Math.round(stats.rowCount * selectivity));
              const cost = treeHeight * RANDOM_IO_COST + matchingRows * CPU_TUPLE_COST;
              plans.push({
                type: 'INDEX_SCAN',
                column: exprIdx.key,
                cost,
                estimatedRows: matchingRows,
                expressionIndex: true,
              });
            } else if (['LT', 'GT', 'LE', 'GE'].includes(expr.op)) {
              const selectivity = 0.33; // Default range selectivity for expression indexes
              const matchingRows = Math.max(1, Math.round(stats.rowCount * selectivity));
              const pagesRead = Math.max(1, Math.ceil(matchingRows / stats.tuplesPerPage));
              const cost = treeHeight * RANDOM_IO_COST + pagesRead * IO_COST + matchingRows * CPU_TUPLE_COST;
              plans.push({
                type: 'INDEX_RANGE_SCAN',
                column: exprIdx.key,
                cost,
                estimatedRows: matchingRows,
                expressionIndex: true,
              });
            }
          }
        }
      }
    }

    // For AND expressions, check if either side can use an index
    if (expr.type === 'AND') {
      plans.push(...this._planIndexScans(expr.left, tableName, stats));
      plans.push(...this._planIndexScans(expr.right, tableName, stats));
    }

    return plans;
  }

  // Check if a WHERE expression matches an expression index definition
  _exprMatchesIndex(whereExpr, indexExpr) {
    if (!whereExpr || !indexExpr) return false;
    // Deep structural comparison of AST nodes
    if (whereExpr.type !== indexExpr.type) return false;
    
    switch (whereExpr.type) {
      case 'function_call':
        return (whereExpr.func || whereExpr.name || '').toUpperCase() === (indexExpr.func || indexExpr.name || '').toUpperCase() &&
          whereExpr.args?.length === indexExpr.args?.length &&
          (whereExpr.args || []).every((arg, i) => this._exprMatchesIndex(arg, indexExpr.args[i]));
      case 'column_ref':
        return (whereExpr.column || whereExpr.name) === (indexExpr.column || indexExpr.name) &&
          (whereExpr.table || null) === (indexExpr.table || null);
      case 'BINARY':
      case 'arith':
        return whereExpr.op === indexExpr.op &&
          this._exprMatchesIndex(whereExpr.left, indexExpr.left) &&
          this._exprMatchesIndex(whereExpr.right, indexExpr.right);
      case 'literal':
        return whereExpr.value === indexExpr.value;
      default:
        return JSON.stringify(whereExpr) === JSON.stringify(indexExpr);
    }
  }

  // Selectivity estimation using histograms
  _estimateSelectivity(expr, stats) {
    if (!expr) return 1.0;

    switch (expr.type) {
      case 'COMPARE': {
        const col = expr.left?.type === 'column_ref' ? expr.left.name : null;
        const val = expr.right?.value;
        const colStats = col ? stats.columns.get(col) : null;

        if (colStats && val !== undefined) {
          switch (expr.op) {
            case 'EQ': return colStats.selectivityEq(val);
            case 'NE': return 1 - colStats.selectivityEq(val);
            case 'LT': return colStats.selectivityLT(val);
            case 'GT': return colStats.selectivityGT(val);
            case 'LE': return colStats.selectivityLT(val) + colStats.selectivityEq(val);
            case 'GE': return colStats.selectivityGT(val) + colStats.selectivityEq(val);
          }
        }
        // Fallback magic numbers
        if (expr.op === 'EQ') return 0.1;
        if (['LT', 'GT', 'LE', 'GE'].includes(expr.op)) return 0.3;
        if (expr.op === 'NE') return 0.9;
        return 0.5;
      }

      case 'AND': {
        const left = this._estimateSelectivity(expr.left, stats);
        const right = this._estimateSelectivity(expr.right, stats);
        return left * right; // Independence assumption
      }

      case 'OR': {
        const left = this._estimateSelectivity(expr.left, stats);
        const right = this._estimateSelectivity(expr.right, stats);
        return Math.min(1.0, left + right - left * right); // Inclusion-exclusion
      }

      case 'NOT':
        return 1 - this._estimateSelectivity(expr.expr, stats);

      case 'IS_NULL': {
        const col = expr.left?.type === 'column_ref' ? expr.left.name : null;
        const colStats = col ? stats.columns.get(col) : null;
        return colStats ? colStats.selectivityNull() : 0.01;
      }

      case 'LIKE': {
        const col = expr.left?.type === 'column_ref' ? expr.left.name : null;
        const colStats = col ? stats.columns.get(col) : null;
        if (colStats && expr.pattern?.value) return colStats.selectivityLike(expr.pattern.value);
        return 0.5;
      }

      case 'BETWEEN': {
        const col = expr.left?.type === 'column_ref' ? expr.left.name : null;
        const colStats = col ? stats.columns.get(col) : null;
        if (colStats) return colStats.selectivityBetween(expr.low?.value, expr.high?.value);
        return 0.25;
      }

      case 'IN_LIST': {
        const col = expr.left?.type === 'column_ref' ? expr.left.name : null;
        const colStats = col ? stats.columns.get(col) : null;
        const numValues = expr.values?.length || 1;
        if (colStats) return Math.min(1.0, numValues * colStats.selectivityEq(null));
        return Math.min(1.0, numValues * 0.1);
      }

      default: return 1.0;
    }
  }

  // ===== DP Join Reordering =====
  // Dynamic programming approach: enumerate all join orders for up to ~8 tables
  // Uses bitmask to represent table subsets

  _dpJoinReorder(tables, joins, where) {
    const n = tables.length;
    
    // For small join counts, just use the given order
    if (n <= 2) {
      return this._simpleJoinPlan(tables, joins);
    }

    // Build join graph: which tables can join with which?
    const joinEdges = this._buildJoinGraph(tables, joins);

    // DP table: dp[bitmask] = { cost, plan, rows }
    const dp = new Map();

    // Base cases: single tables
    for (let i = 0; i < n; i++) {
      const mask = 1 << i;
      const t = tables[i];
      const accessCost = t.stats.pageCount * IO_COST + t.stats.rowCount * CPU_TUPLE_COST;
      dp.set(mask, {
        cost: accessCost,
        rows: t.stats.rowCount,
        plan: [{ type: 'SCAN', table: t.name, rows: t.stats.rowCount }],
        tables: new Set([i]),
      });
    }

    // Fill DP table for subsets of increasing size
    for (let size = 2; size <= n; size++) {
      for (const mask of this._subsetsOfSize(n, size)) {
        let bestCost = Infinity;
        let bestPlan = null;

        // Try all ways to split mask into two non-empty subsets
        for (const [left, right] of this._splitSubsets(mask)) {
          if (!dp.has(left) || !dp.has(right)) continue;

          // Check if there's a join condition between left and right
          const edge = this._findJoinEdge(joinEdges, left, right, tables);
          if (!edge && size > 2) continue; // No join predicate — skip unless it's the first pair

          const leftPlan = dp.get(left);
          const rightPlan = dp.get(right);

          // Try nested loop join
          const nlCost = leftPlan.cost + leftPlan.rows * rightPlan.cost + 
                         leftPlan.rows * rightPlan.rows * CPU_TUPLE_COST;
          
          // Estimate join output rows
          const joinRows = this._estimateJoinRows(leftPlan, rightPlan, edge, tables);

          if (nlCost < bestCost) {
            bestCost = nlCost;
            bestPlan = {
              cost: nlCost,
              rows: joinRows,
              plan: [...leftPlan.plan, ...rightPlan.plan, {
                type: 'NESTED_LOOP_JOIN',
                left: left, right: right,
                rows: joinRows,
                edge,
              }],
              tables: new Set([...leftPlan.tables, ...rightPlan.tables]),
            };
          }

          // Try hash join (if right side is small enough to hash)
          if (edge) {
            const hjCost = leftPlan.cost + rightPlan.cost +
                          rightPlan.rows * HASH_BUILD_COST +
                          leftPlan.rows * CPU_TUPLE_COST;
            
            if (hjCost < bestCost) {
              bestCost = hjCost;
              bestPlan = {
                cost: hjCost,
                rows: joinRows,
                plan: [...leftPlan.plan, ...rightPlan.plan, {
                  type: 'HASH_JOIN',
                  left: left, right: right,
                  buildSide: right,
                  rows: joinRows,
                  edge,
                }],
                tables: new Set([...leftPlan.tables, ...rightPlan.tables]),
              };
            }

            // Try merge join
            const sortLeft = leftPlan.rows * Math.log2(leftPlan.rows + 1) * CPU_TUPLE_COST;
            const sortRight = rightPlan.rows * Math.log2(rightPlan.rows + 1) * CPU_TUPLE_COST;
            const mjCost = leftPlan.cost + rightPlan.cost + sortLeft + sortRight +
                          (leftPlan.rows + rightPlan.rows) * CPU_TUPLE_COST;

            if (mjCost < bestCost) {
              bestCost = mjCost;
              bestPlan = {
                cost: mjCost,
                rows: joinRows,
                plan: [...leftPlan.plan, ...rightPlan.plan, {
                  type: 'MERGE_JOIN',
                  left: left, right: right,
                  rows: joinRows,
                  edge,
                }],
                tables: new Set([...leftPlan.tables, ...rightPlan.tables]),
              };
            }
          }
        }

        if (bestPlan) dp.set(mask, bestPlan);
      }
    }

    // Full table set
    const fullMask = (1 << n) - 1;
    const optimal = dp.get(fullMask);
    
    if (!optimal) return this._simpleJoinPlan(tables, joins);

    // Convert DP plan to output format
    return {
      totalCost: optimal.cost,
      order: optimal.plan.filter(p => p.type !== 'SCAN').map(p => ({
        type: p.type,
        estimatedRows: p.rows,
      })),
      steps: optimal.plan.filter(p => p.type !== 'SCAN').map(p => ({
        type: p.type,
        cost: 0, // Already in totalCost
        estimatedRows: p.rows,
        buildSide: p.buildSide ? tables[Math.log2(p.buildSide)]?.name : undefined,
      })),
    };
  }

  _buildJoinGraph(tables, joins) {
    const edges = [];
    for (let i = 0; i < joins.length; i++) {
      const join = joins[i];
      if (!join.on) continue;
      
      // Find which tables are referenced in the ON clause
      const cols = this._extractJoinColumns(join.on);
      const leftTableIdx = 0; // In SQL, left side of JOIN is the accumulated result
      const rightTableIdx = tables.findIndex(t => t.name === join.table || t.alias === join.table);
      
      if (rightTableIdx >= 0) {
        edges.push({
          left: 1 << leftTableIdx,
          right: 1 << rightTableIdx,
          on: join.on,
          columns: cols,
        });
      }
    }
    return edges;
  }

  _extractJoinColumns(expr) {
    if (!expr) return [];
    if (expr.type === 'COMPARE' && expr.op === 'EQ') {
      return [
        expr.left?.type === 'column_ref' ? expr.left.name : null,
        expr.right?.type === 'column_ref' ? expr.right.name : null,
      ].filter(Boolean);
    }
    if (expr.type === 'AND') {
      return [...this._extractJoinColumns(expr.left), ...this._extractJoinColumns(expr.right)];
    }
    return [];
  }

  _findJoinEdge(edges, leftMask, rightMask, tables) {
    for (const edge of edges) {
      if ((edge.left & leftMask) && (edge.right & rightMask)) return edge;
      if ((edge.right & leftMask) && (edge.left & rightMask)) return edge;
    }
    return null;
  }

  _estimateJoinRows(leftPlan, rightPlan, edge, tables) {
    if (!edge) {
      // Cross join
      return leftPlan.rows * rightPlan.rows;
    }
    
    // For equijoin, estimate using NDV
    // |A ⋈ B| ≈ |A| * |B| / max(ndv(A.col), ndv(B.col))
    const cols = edge.columns;
    let maxNdv = 1;
    
    for (const t of tables) {
      for (const col of cols) {
        const bareCol = col.includes('.') ? col.split('.').pop() : col;
        const colStats = t.stats.columns.get(bareCol);
        if (colStats) maxNdv = Math.max(maxNdv, colStats.ndv);
      }
    }
    
    return Math.max(1, Math.round(leftPlan.rows * rightPlan.rows / maxNdv));
  }

  // Generate all bitmasks with exactly `size` bits set from `n` bits
  *_subsetsOfSize(n, size) {
    const generate = function*(mask, start, remaining) {
      if (remaining === 0) { yield mask; return; }
      for (let i = start; i < n; i++) {
        yield* generate(mask | (1 << i), i + 1, remaining - 1);
      }
    };
    yield* generate(0, 0, size);
  }

  // Split a bitmask into two non-empty complementary subsets
  *_splitSubsets(mask) {
    // Enumerate all non-empty proper subsets of mask
    let sub = (mask - 1) & mask;
    while (sub > 0) {
      const complement = mask & ~sub;
      if (complement > 0 && sub < complement) {
        yield [sub, complement];
      }
      sub = (sub - 1) & mask;
    }
  }

  _simpleJoinPlan(tables, joins) {
    const steps = [];
    let totalCost = 0;
    let currentRows = tables[0].stats.rowCount;

    for (let i = 0; i < joins.length; i++) {
      const rightStats = tables[i + 1]?.stats;
      if (!rightStats) continue;

      const nlCost = currentRows * rightStats.rowCount;
      const hjCost = currentRows + rightStats.rowCount + rightStats.rowCount;

      if (hjCost < nlCost && rightStats.rowCount > 10) {
        steps.push({
          type: 'HASH_JOIN',
          table: joins[i].table,
          cost: hjCost,
          buildSide: joins[i].table,
          estimatedRows: Math.min(currentRows, rightStats.rowCount),
        });
        totalCost += hjCost;
        currentRows = Math.min(currentRows, rightStats.rowCount);
      } else {
        // Also consider merge join
        const sortLeftCost = currentRows * Math.log2(currentRows + 1) * CPU_TUPLE_COST;
        const sortRightCost = rightStats.rowCount * Math.log2(rightStats.rowCount + 1) * CPU_TUPLE_COST;
        const mergeCost = sortLeftCost + sortRightCost + (currentRows + rightStats.rowCount) * CPU_TUPLE_COST;

        if (mergeCost < nlCost) {
          steps.push({
            type: 'MERGE_JOIN',
            table: joins[i].table,
            cost: mergeCost,
            estimatedRows: Math.min(currentRows, rightStats.rowCount),
          });
          totalCost += mergeCost;
        } else {
          steps.push({
            type: 'NESTED_LOOP_JOIN',
            table: joins[i].table,
            cost: nlCost,
            estimatedRows: Math.min(currentRows, rightStats.rowCount),
          });
          totalCost += nlCost;
        }
        currentRows = Math.min(currentRows, rightStats.rowCount);
      }
    }

    return { totalCost, steps, order: steps };
  }

  // Resolve join columns: determine which column belongs to left vs right
  _resolveJoinColumns(joinOn, leftRows, rightRows) {
    const col1 = joinOn.left?.name;
    const col2 = joinOn.right?.name;
    
    if (!col1 || !col2 || leftRows.length === 0 || rightRows.length === 0) {
      return { leftCol: col1, rightCol: col2 };
    }

    const leftHas1 = col1 in leftRows[0];
    const leftHas2 = col2 in leftRows[0];
    const rightHas1 = col1 in rightRows[0];
    const rightHas2 = col2 in rightRows[0];

    // Prefer assignment where each column maps to its unique table
    if (leftHas1 && rightHas2 && !leftHas2) return { leftCol: col1, rightCol: col2 };
    if (leftHas2 && rightHas1 && !leftHas1) return { leftCol: col2, rightCol: col1 };
    if (leftHas1 && rightHas2) return { leftCol: col1, rightCol: col2 };
    if (leftHas2 && rightHas1) return { leftCol: col2, rightCol: col1 };

    // Fallback: col1 is left, col2 is right
    return { leftCol: col1, rightCol: col2 };
  }
  executeHashJoin(leftRows, rightTable, joinOn, schema, alias) {
    const hashMap = new Map();
    const rightRows = [];
    for (const { values } of rightTable.heap.scan()) {
      const row = {};
      for (let i = 0; i < schema.length; i++) {
        row[schema[i].name] = values[i];
        row[`${alias}.${schema[i].name}`] = values[i];
      }
      rightRows.push(row);
    }

    const { leftCol, rightCol } = this._resolveJoinColumns(joinOn, leftRows, rightRows);

    for (const row of rightRows) {
      const key = row[rightCol];
      if (!hashMap.has(key)) hashMap.set(key, []);
      hashMap.get(key).push(row);
    }

    const result = [];
    for (const leftRow of leftRows) {
      const key = leftRow[leftCol];
      const matches = hashMap.get(key) || [];
      for (const rightRow of matches) {
        result.push({ ...leftRow, ...rightRow });
      }
    }
    return result;
  }

  // Execute a merge join: sort both inputs on join key, merge in O(n+m)
  executeMergeJoin(leftRows, rightTable, joinOn, schema, alias) {
    // Build right rows
    const rightRows = [];
    for (const { values } of rightTable.heap.scan()) {
      const row = {};
      for (let i = 0; i < schema.length; i++) {
        row[schema[i].name] = values[i];
        row[`${alias}.${schema[i].name}`] = values[i];
      }
      rightRows.push(row);
    }

    const { leftCol, rightCol } = this._resolveJoinColumns(joinOn, leftRows, rightRows);

    // Sort both inputs on join key
    const sortedLeft = [...leftRows].sort((a, b) => {
      const av = a[leftCol], bv = b[leftCol];
      return av < bv ? -1 : av > bv ? 1 : 0;
    });
    const sortedRight = [...rightRows].sort((a, b) => {
      const av = a[rightCol], bv = b[rightCol];
      return av < bv ? -1 : av > bv ? 1 : 0;
    });

    // Merge: scan both in lockstep
    const result = [];
    let li = 0, ri = 0;

    while (li < sortedLeft.length && ri < sortedRight.length) {
      const lv = sortedLeft[li][leftCol];
      const rv = sortedRight[ri][rightCol];

      if (lv < rv) {
        li++;
      } else if (lv > rv) {
        ri++;
      } else {
        // Equal: find all matching rows on both sides
        const matchStart = ri;
        while (ri < sortedRight.length && sortedRight[ri][rightCol] === lv) ri++;
        const rightMatches = sortedRight.slice(matchStart, ri);

        // For each left row with this key, join with all right matches
        while (li < sortedLeft.length && sortedLeft[li][leftCol] === lv) {
          for (const rightRow of rightMatches) {
            result.push({ ...sortedLeft[li], ...rightRow });
          }
          li++;
        }
      }
    }

    return result;
  }
}

// ===== EXPLAIN output =====
export function formatPlan(plan) {
  const lines = [];
  lines.push(`Scan: ${plan.scanType} on ${plan.table}`);
  if (plan.indexColumn) lines.push(`  Index: ${plan.indexColumn}`);
  lines.push(`  Estimated rows: ${plan.estimatedRows}`);
  lines.push(`  Estimated cost: ${plan.estimatedCost.toFixed(2)}`);

  for (const join of plan.joins || []) {
    lines.push(`Join: ${join.type} with ${join.table || '?'}`);
    if (join.buildSide) lines.push(`  Build side: ${join.buildSide}`);
    lines.push(`  Estimated rows: ${join.estimatedRows}`);
    if (join.cost) lines.push(`  Cost: ${join.cost.toFixed(2)}`);
  }

  if (plan.totalCost) lines.push(`Total cost: ${plan.totalCost.toFixed(2)}`);
  if (plan.joinOrder) {
    lines.push(`Join order: ${plan.joinOrder.map(j => j.type).join(' → ')}`);
  }

  return lines.join('\n');
}
