// compiled-query.js — Integrates cost-based planner with pipeline JIT compilation
// When a query is planned, this module generates optimized compiled code
// that executes the planner's chosen strategy without Volcano overhead.

import { QueryPlanner } from './planner.js';
import { compilePipelineJIT, JITCompiledIterator, compilePipeline, CompiledIterator } from './pipeline-compiler.js';
import { SeqScan, Filter, Project, Limit, HashJoin, Sort, HashAggregate } from './volcano.js';
import { sqliteCompare } from './type-affinity.js';

/**
 * CompiledQueryEngine — takes a Database, plans queries, and compiles execution.
 * Bridges the planner's strategic decisions with the pipeline compiler's codegen.
 */
export class CompiledQueryEngine {
  constructor(database, { compileThreshold = 5000 } = {}) {
    this.db = database;
    this.planner = new QueryPlanner(database);
    this._compileThreshold = compileThreshold;
    this.stats = { queriesCompiled: 0, queriesInterpreted: 0, totalCompileTimeMs: 0 };
  }

  /**
   * Execute a SELECT using planned + compiled execution.
   * Falls back to standard execution if compilation isn't beneficial.
   */
  executeSelect(ast, _skipAggCheck = false) {
    const startTime = Date.now();
    
    // Check for aggregates/GROUP BY — use specialized path
    if (!_skipAggCheck) {
      const hasAggregates = ast.columns?.some(c => c.type === 'aggregate' || c.func);
      const hasGroupBy = ast.groupBy && ast.groupBy.length > 0;
      if (hasAggregates || hasGroupBy) {
        return this.executeSelectWithAggregation(ast);
      }
    }
    
    // 1. Plan the query
    const plan = this.planner.plan(ast);
    
    // 2. Decide: compile or interpret?
    // Compile if: table has enough rows (overhead worthwhile) and no subqueries
    const tableStats = this.planner.getStats(ast.from?.table);
    const tableRows = tableStats?.rowCount || 0;
    // Only compile queries on tables with enough rows to justify the overhead.
    // Benchmarking (Apr 21): compiled engine 3x slower than interpreter at 1K rows.
    // Crossover point is ~5K rows where compilation overhead is amortized.
    const threshold = this._compileThreshold ?? 5000;
    const shouldCompile = tableRows >= threshold && !ast.where?.subquery;
    
    if (!shouldCompile) {
      this.stats.queriesInterpreted++;
      return null; // Signal caller to use standard execution
    }

    // 3. Generate compiled execution based on plan
    try {
      const compiled = this._compileFromPlan(ast, plan);
      if (compiled) {
        this.stats.queriesCompiled++;
        this.stats.totalCompileTimeMs += Date.now() - startTime;
        return compiled;
      }
    } catch (e) {
      // Compilation failed — fall back
    }

    this.stats.queriesInterpreted++;
    return null;
  }

  /**
   * Compile a query from a planner's output.
   * Returns { rows: [...] } or null.
   */
  _compileFromPlan(ast, plan) {
    const tableName = ast.from?.table;
    if (!tableName) return null;

    const table = this.db.tables.get(tableName);
    if (!table) return null;

    const joins = ast.joins || [];

    // Single-table query: compile scan + filter + project
    if (joins.length === 0) {
      return this._compileSingleTable(ast, table, plan);
    }

    // Multi-table: compile join strategy from plan
    return this._compileJoinQuery(ast, table, plan, joins);
  }

  /**
   * Compile a single-table query into a tight loop.
   */
  _compileSingleTable(ast, table, plan) {
    const schema = table.schema;
    const columns = schema.map(c => c.name);

    // Build compiled scan function
    const scanRows = [];
    
    if (plan.scanType === 'INDEX_SCAN' && plan.indexColumn) {
      // Use index scan
      const idx = table.indexes.get(plan.indexColumn);
      if (idx && ast.where) {
        const value = this._extractCompareValue(ast.where);
        if (value !== undefined) {
          const rids = idx.search(value);
          for (const rid of rids) {
            const { values } = table.heap.get(rid);
            const row = {};
            for (let i = 0; i < columns.length; i++) row[columns[i]] = values[i];
            scanRows.push(row);
          }
          return { rows: this._applyProjectAndLimit(scanRows, ast) };
        }
      }
    }

    // Sequential scan with compiled filter
    const filterFn = ast.where ? this._compileFilter(ast.where, schema) : null;
    if (ast.where && !filterFn) {
      // Cannot compile the WHERE clause — throw to signal fallback to interpreter.
      // This prevents silently returning all rows when the filter can't compile.
      throw new Error(`Compiled engine: unsupported WHERE expression type. Use interpreter fallback.`);
    }
    const limitVal = ast.limit?.value;
    let count = 0;

    for (const { values } of table.heap.scan()) {
      if (limitVal && count >= limitVal) break;
      
      const row = {};
      for (let i = 0; i < columns.length; i++) row[columns[i]] = values[i];
      
      if (filterFn && !filterFn(row)) continue;
      scanRows.push(row);
      count++;
    }

    return { rows: this._applyProjectAndLimit(scanRows, ast) };
  }

  /**
   * Compile a multi-table join query using the planner's chosen strategy.
   */
  _compileJoinQuery(ast, leftTable, plan, joins) {
    const leftSchema = leftTable.schema;
    const leftColumns = leftSchema.map(c => c.name);
    const leftAlias = ast.from.alias || ast.from.table;

    // Build left side rows
    let leftRows = [];
    for (const { values } of leftTable.heap.scan()) {
      const row = {};
      for (let i = 0; i < leftColumns.length; i++) {
        row[leftColumns[i]] = values[i];
        row[`${leftAlias}.${leftColumns[i]}`] = values[i];
      }
      leftRows.push(row);
    }

    // Apply WHERE filter to left table (pre-join pushdown)
    if (ast.where) {
      const preFilter = this._compileFilter(ast.where, leftSchema, true);
      if (preFilter) {
        leftRows = leftRows.filter(preFilter);
      }
    }

    // Execute joins in order, using plan's chosen strategy
    for (let i = 0; i < joins.length; i++) {
      const join = joins[i];
      const rightTableName = join.table;
      const rightTable = this.db.tables.get(rightTableName);
      if (!rightTable) continue;

      const rightSchema = rightTable.schema;
      const rightColumns = rightSchema.map(c => c.name);
      const rightAlias = join.alias || rightTableName;

      // Determine join strategy from plan
      const joinStep = plan.joins?.[i];
      const strategy = joinStep?.type || 'HASH_JOIN';

      // Build right side rows
      const rightRows = [];
      for (const { values } of rightTable.heap.scan()) {
        const row = {};
        for (let j = 0; j < rightColumns.length; j++) {
          row[rightColumns[j]] = values[j];
          row[`${rightAlias}.${rightColumns[j]}`] = values[j];
        }
        rightRows.push(row);
      }

      // Extract join columns from ON clause
      const joinCols = this._extractJoinColumns(join.on);

      switch (strategy) {
        case 'HASH_JOIN':
          leftRows = this._compiledHashJoin(leftRows, rightRows, joinCols, join.joinType);
          break;
        case 'MERGE_JOIN':
          leftRows = this._compiledMergeJoin(leftRows, rightRows, joinCols, join.joinType);
          break;
        case 'NESTED_LOOP_JOIN':
        default:
          leftRows = this._compiledNestedLoopJoin(leftRows, rightRows, join.on, join.joinType);
          break;
      }
    }

    // Apply final WHERE filter (post-join conditions)
    if (ast.where) {
      const postFilter = this._compileFilter(ast.where, null, false);
      if (!postFilter) {
        throw new Error(`Compiled engine: unsupported WHERE expression in join query. Use interpreter fallback.`);
      }
      leftRows = leftRows.filter(postFilter);
    }

    return { rows: this._applyProjectAndLimit(leftRows, ast) };
  }

  /**
   * Compiled hash join — builds hash table on right, probes with left.
   * Generated as tight code with minimal overhead.
   */
  _compiledHashJoin(leftRows, rightRows, joinCols, joinType) {
    if (!joinCols || joinCols.length < 2) {
      // Fall back to nested loop for cross joins
      return this._compiledCrossJoin(leftRows, rightRows);
    }

    const [leftCol, rightCol] = joinCols;
    
    // Resolve which column belongs to which side
    const resolvedCols = this._resolveColumns(leftRows, rightRows, leftCol, rightCol);

    // Build hash table on right side (smaller assumed by planner)
    const hashTable = new Map();
    for (const row of rightRows) {
      const key = row[resolvedCols.right];
      if (!hashTable.has(key)) hashTable.set(key, []);
      hashTable.get(key).push(row);
    }

    const result = [];
    for (const leftRow of leftRows) {
      const key = leftRow[resolvedCols.left];
      const matches = hashTable.get(key);
      
      if (matches) {
        for (const rightRow of matches) {
          result.push({ ...leftRow, ...rightRow });
        }
      } else if (joinType === 'LEFT' || joinType === 'LEFT OUTER') {
        // Left join: include unmatched left rows with nulls
        result.push({ ...leftRow });
      }
    }

    return result;
  }

  /**
   * Compiled merge join — sort both sides, merge in O(n+m).
   */
  _compiledMergeJoin(leftRows, rightRows, joinCols, joinType) {
    if (!joinCols || joinCols.length < 2) {
      return this._compiledCrossJoin(leftRows, rightRows);
    }

    const [leftCol, rightCol] = joinCols;
    const resolved = this._resolveColumns(leftRows, rightRows, leftCol, rightCol);

    // Sort both sides
    const sortedLeft = [...leftRows].sort((a, b) => {
      const av = a[resolved.left], bv = b[resolved.left];
      return sqliteCompare(av, bv);
    });
    const sortedRight = [...rightRows].sort((a, b) => {
      const av = a[resolved.right], bv = b[resolved.right];
      return sqliteCompare(av, bv);
    });

    const result = [];
    let li = 0, ri = 0;

    while (li < sortedLeft.length && ri < sortedRight.length) {
      const lv = sortedLeft[li][resolved.left];
      const rv = sortedRight[ri][resolved.right];

      if (lv < rv) {
        if (joinType === 'LEFT' || joinType === 'LEFT OUTER') {
          result.push({ ...sortedLeft[li] });
        }
        li++;
      } else if (lv > rv) {
        ri++;
      } else {
        // Equal: find all matching rows on both sides
        const matchStart = ri;
        while (ri < sortedRight.length && sortedRight[ri][resolved.right] === lv) ri++;
        const rightMatches = sortedRight.slice(matchStart, ri);

        while (li < sortedLeft.length && sortedLeft[li][resolved.left] === lv) {
          for (const rightRow of rightMatches) {
            result.push({ ...sortedLeft[li], ...rightRow });
          }
          li++;
        }
      }
    }

    // Handle remaining left rows for LEFT JOIN
    if (joinType === 'LEFT' || joinType === 'LEFT OUTER') {
      while (li < sortedLeft.length) {
        result.push({ ...sortedLeft[li] });
        li++;
      }
    }

    return result;
  }

  /**
   * Compiled nested loop join with predicate.
   */
  _compiledNestedLoopJoin(leftRows, rightRows, onExpr, joinType) {
    const predicate = onExpr ? this._compileJoinPredicate(onExpr) : () => true;
    const result = [];

    for (const leftRow of leftRows) {
      let matched = false;
      for (const rightRow of rightRows) {
        const combined = { ...leftRow, ...rightRow };
        if (predicate(combined)) {
          result.push(combined);
          matched = true;
        }
      }
      if (!matched && (joinType === 'LEFT' || joinType === 'LEFT OUTER')) {
        result.push({ ...leftRow });
      }
    }

    return result;
  }

  _compiledCrossJoin(leftRows, rightRows) {
    const result = [];
    for (const l of leftRows) {
      for (const r of rightRows) {
        result.push({ ...l, ...r });
      }
    }
    return result;
  }

  /**
   * Resolve which join column belongs to left vs right side.
   */
  _resolveColumns(leftRows, rightRows, col1, col2) {
    if (leftRows.length === 0 || rightRows.length === 0) {
      return { left: col1, right: col2 };
    }

    const l0 = leftRows[0];
    const r0 = rightRows[0];

    // Try both assignments
    if (col1 in l0 && col2 in r0) return { left: col1, right: col2 };
    if (col2 in l0 && col1 in r0) return { left: col2, right: col1 };

    // Try with table qualification
    for (const key of Object.keys(l0)) {
      if (key.endsWith(`.${col1}`)) return { left: key, right: col2 };
      if (key.endsWith(`.${col2}`)) return { left: key, right: col1 };
    }

    return { left: col1, right: col2 };
  }

  /**
   * Extract join columns from an ON expression (e.g., a.id = b.customer_id)
   */
  _extractJoinColumns(onExpr) {
    if (!onExpr) return null;
    if (onExpr.type === 'COMPARE' && onExpr.op === 'EQ') {
      const left = onExpr.left?.type === 'column_ref' ? (onExpr.left.table ? `${onExpr.left.table}.${onExpr.left.name}` : onExpr.left.name) : null;
      const right = onExpr.right?.type === 'column_ref' ? (onExpr.right.table ? `${onExpr.right.table}.${onExpr.right.name}` : onExpr.right.name) : null;
      if (left && right) return [left, right];
    }
    // For AND of multiple conditions, return the first equijoin pair
    if (onExpr.type === 'AND') {
      return this._extractJoinColumns(onExpr.left) || this._extractJoinColumns(onExpr.right);
    }
    return null;
  }

  /**
   * Compile a WHERE clause into a JS predicate function.
   * @param {boolean} preJoin — if true, only compile single-table predicates
   */
  _compileFilter(expr, schema, preJoin = false) {
    if (!expr) return null;

    // For pre-join filters, only handle simple column comparisons
    if (preJoin && expr.type === 'COMPARE') {
      if (expr.left?.type === 'column_ref' && !expr.left.table &&
          expr.right?.type !== 'column_ref') {
        const col = expr.left.name;
        const val = expr.right?.value;
        if (val !== undefined) {
          switch (expr.op) {
            case 'EQ': return (row) => row[col] === val;
            case 'NE': return (row) => row[col] !== val;
            case 'LT': return (row) => row[col] < val;
            case 'GT': return (row) => row[col] > val;
            case 'LE': return (row) => row[col] <= val;
            case 'GE': return (row) => row[col] >= val;
          }
        }
      }
      return null; // Can't push this filter pre-join
    }

    // Post-join or standalone: compile the full expression
    return this._compileExpr(expr);
  }

  _compileExpr(expr) {
    if (!expr) return null;

    switch (expr.type) {
      case 'COMPARE': {
        const leftFn = this._compileValueExpr(expr.left);
        const rightFn = this._compileValueExpr(expr.right);
        switch (expr.op) {
          case 'EQ': return (row) => leftFn(row) === rightFn(row);
          case 'NE': return (row) => leftFn(row) !== rightFn(row);
          case 'LT': return (row) => leftFn(row) < rightFn(row);
          case 'GT': return (row) => leftFn(row) > rightFn(row);
          case 'LE': return (row) => leftFn(row) <= rightFn(row);
          case 'GE': return (row) => leftFn(row) >= rightFn(row);
        }
        return null;
      }
      case 'AND': {
        const left = this._compileExpr(expr.left);
        const right = this._compileExpr(expr.right);
        // Both sides must compile — can't silently drop conditions
        if (left && right) return (row) => left(row) && right(row);
        if (!left || !right) return null; // Fall back to interpreter
        return left || right;
      }
      case 'OR': {
        const left = this._compileExpr(expr.left);
        const right = this._compileExpr(expr.right);
        // Both sides must compile for OR — can't evaluate partial OR
        if (left && right) return (row) => left(row) || right(row);
        return null;
      }
      case 'NOT': {
        const inner = this._compileExpr(expr.expr);
        return inner ? (row) => !inner(row) : null;
      }
      case 'BETWEEN': {
        const valFn = this._compileValueExpr(expr.expr || expr.left);
        const lowFn = this._compileValueExpr(expr.low || expr.right?.low);
        const highFn = this._compileValueExpr(expr.high || expr.right?.high);
        return (row) => {
          const v = valFn(row), lo = lowFn(row), hi = highFn(row);
          return v >= lo && v <= hi;
        };
      }
      case 'NOT_BETWEEN': {
        const valFn = this._compileValueExpr(expr.expr || expr.left);
        const lowFn = this._compileValueExpr(expr.low || expr.right?.low);
        const highFn = this._compileValueExpr(expr.high || expr.right?.high);
        return (row) => {
          const v = valFn(row), lo = lowFn(row), hi = highFn(row);
          return v < lo || v > hi;
        };
      }
      case 'IS_NULL': {
        const valFn = this._compileValueExpr(expr.expr || expr.left);
        return (row) => valFn(row) == null;
      }
      case 'IS_NOT_NULL': {
        const valFn = this._compileValueExpr(expr.expr || expr.left);
        return (row) => valFn(row) != null;
      }
      case 'IN_LIST': {
        const valFn = this._compileValueExpr(expr.expr || expr.left);
        const listFns = (expr.list || expr.values || []).map(item => this._compileValueExpr(item));
        return (row) => {
          const v = valFn(row);
          return listFns.some(fn => fn(row) === v);
        };
      }
      case 'NOT_IN': {
        const valFn = this._compileValueExpr(expr.expr || expr.left);
        const listFns = (expr.list || expr.values || []).map(item => this._compileValueExpr(item));
        return (row) => {
          const v = valFn(row);
          if (v == null) return null; // SQL: NULL NOT IN (...) = NULL
          return !listFns.some(fn => fn(row) === v);
        };
      }
      case 'LIKE':
      case 'ILIKE': {
        const valFn = this._compileValueExpr(expr.left || expr.expr);
        const patternFn = this._compileValueExpr(expr.pattern || expr.right);
        const caseInsensitive = expr.type === 'ILIKE';
        return (row) => {
          const v = valFn(row);
          const p = patternFn(row);
          if (v == null || p == null) return false;
          const regex = String(p).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
            .replace(/%/g, '.*').replace(/_/g, '.');
          return new RegExp(`^${regex}$`, caseInsensitive ? 'i' : '').test(String(v));
        };
      }
      default:
        // Unknown expression type — return null to signal the compiled engine
        // cannot handle this query. Callers MUST fall back to interpreter.
        return null;
    }
  }

  _compileValueExpr(expr) {
    if (!expr) return () => null;
    if (expr.type === 'column_ref') {
      const name = expr.table ? `${expr.table}.${expr.name}` : expr.name;
      return (row) => row[name] !== undefined ? row[name] : row[expr.name];
    }
    if (expr.type === 'literal' || expr.value !== undefined) {
      const val = expr.value;
      return () => val;
    }
    return () => null;
  }

  _compileJoinPredicate(onExpr) {
    return this._compileExpr(onExpr) || (() => true);
  }

  _extractCompareValue(expr) {
    if (expr.type === 'COMPARE' && expr.op === 'EQ') {
      return expr.right?.value;
    }
    return undefined;
  }

  /**
   * Compiled aggregation — push GROUP BY + aggregates into the compiled loop.
   * Instead of materializing all rows then grouping, aggregate incrementally.
   */
  _compiledAggregate(rows, groupByColumns, aggregates) {
    const groups = new Map();

    for (const row of rows) {
      // Build group key
      const keyParts = groupByColumns.map(col => {
        const val = row[col] !== undefined ? row[col] : row[col.split('.').pop()];
        return val;
      });
      const key = keyParts.length === 1 ? keyParts[0] : keyParts.join('|');

      if (!groups.has(key)) {
        // Initialize group
        const group = { _key: keyParts, _count: 0 };
        for (const agg of aggregates) {
          switch (agg.fn) {
            case 'COUNT': group[`_agg_${agg.alias}`] = 0; break;
            case 'SUM': group[`_agg_${agg.alias}`] = 0; break;
            case 'MIN': group[`_agg_${agg.alias}`] = Infinity; break;
            case 'MAX': group[`_agg_${agg.alias}`] = -Infinity; break;
            case 'AVG': group[`_agg_${agg.alias}_sum`] = 0; group[`_agg_${agg.alias}_cnt`] = 0; break;
          }
        }
        groups.set(key, group);
      }

      const group = groups.get(key);
      group._count++;

      // Accumulate aggregates
      for (const agg of aggregates) {
        const val = agg.column === '*' ? 1 : (row[agg.column] !== undefined ? row[agg.column] : row[agg.column?.split('.').pop()]);
        const numVal = typeof val === 'number' ? val : (parseFloat(val) || 0);
        
        switch (agg.fn) {
          case 'COUNT':
            if (agg.column === '*' || val != null) group[`_agg_${agg.alias}`]++;
            break;
          case 'SUM':
            if (val != null) group[`_agg_${agg.alias}`] += numVal;
            break;
          case 'MIN':
            if (val != null && numVal < group[`_agg_${agg.alias}`]) group[`_agg_${agg.alias}`] = numVal;
            break;
          case 'MAX':
            if (val != null && numVal > group[`_agg_${agg.alias}`]) group[`_agg_${agg.alias}`] = numVal;
            break;
          case 'AVG':
            if (val != null) {
              group[`_agg_${agg.alias}_sum`] += numVal;
              group[`_agg_${agg.alias}_cnt`]++;
            }
            break;
        }
      }
    }

    // Build result rows from groups
    const result = [];
    for (const group of groups.values()) {
      const row = {};
      
      // Add group-by columns
      for (let i = 0; i < groupByColumns.length; i++) {
        const colName = groupByColumns[i].split('.').pop();
        row[colName] = group._key[i];
      }

      // Add aggregate results
      for (const agg of aggregates) {
        if (agg.fn === 'AVG') {
          const cnt = group[`_agg_${agg.alias}_cnt`];
          row[agg.alias] = cnt > 0 ? group[`_agg_${agg.alias}_sum`] / cnt : null;
        } else {
          let val = group[`_agg_${agg.alias}`];
          if (agg.fn === 'MIN' && val === Infinity) val = null;
          if (agg.fn === 'MAX' && val === -Infinity) val = null;
          row[agg.alias] = val;
        }
      }

      result.push(row);
    }

    return result;
  }

  /**
   * Execute a compiled query with aggregation.
   * Detects GROUP BY and aggregate functions in the AST,
   * runs the scan/join compiled, then aggregates the results.
   */
  executeSelectWithAggregation(ast) {
    // First, try to detect aggregation
    const aggInfo = this._extractAggregation(ast);
    if (!aggInfo) {
      // No aggregation — use standard compiled path
      return this.executeSelect(ast, true);
    }

    // Run the scan/join phase (get raw rows before aggregation)
    const startTime = Date.now();
    const tableStats = this.planner.getStats(ast.from?.table);
    const tableRows = tableStats?.rowCount || 0;
    if (tableRows < 50) return null;

    const plan = this.planner.plan(ast);

    let rawRows;
    // Strip aggregation from AST for the scan phase
    const scanAst = { ...ast, columns: [{ name: '*' }], groupBy: undefined, having: undefined, orderBy: undefined, limit: undefined };
    
    try {
      const result = this._compileFromPlan(scanAst, plan);
      if (!result) return null;
      rawRows = result.rows;
    } catch (e) {
      return null;
    }

    // Run compiled aggregation
    const aggregated = this._compiledAggregate(rawRows, aggInfo.groupBy, aggInfo.aggregates);

    // Apply HAVING filter
    if (aggInfo.having) {
      const havingFn = this._compileHavingFilter(aggInfo.having);
      if (!havingFn) {
        throw new Error(`Compiled engine: unsupported HAVING expression. Use interpreter fallback.`);
      }
      const filtered = aggregated.filter(havingFn);
      this.stats.queriesCompiled++;
      this.stats.totalCompileTimeMs += Date.now() - startTime;
      return { rows: ast.limit ? filtered.slice(0, typeof ast.limit === "number" ? ast.limit : ast.limit.value) : filtered };
    }

    // Apply ORDER BY
    if (ast.orderBy) {
      aggregated.sort((a, b) => {
        for (const ob of ast.orderBy) {
          const col = ob.column || ob.name;
          const dir = ob.direction === 'DESC' ? -1 : 1;
          const av = a[col], bv = b[col];
          // NULL handling: NULL is smallest (SQLite behavior)
          const aNull = av === null || av === undefined;
          const bNull = bv === null || bv === undefined;
          if (aNull && bNull) continue;
          if (aNull) return -dir; // null is smallest → first in ASC, last in DESC
          if (bNull) return dir;
          if (av < bv) return -dir;
          if (av > bv) return dir;
        }
        return 0;
      });
    }

    this.stats.queriesCompiled++;
    this.stats.totalCompileTimeMs += Date.now() - startTime;
    return { rows: ast.limit ? aggregated.slice(0, typeof ast.limit === "number" ? ast.limit : ast.limit.value) : aggregated };
  }

  /**
   * Extract aggregation info from AST.
   * Returns { groupBy: string[], aggregates: [{fn, column, alias}] } or null.
   */
  _extractAggregation(ast) {
    if (!ast.columns) return null;

    const aggregates = [];
    const groupBy = ast.groupBy || [];

    for (const col of ast.columns) {
      if (col.type === 'aggregate' || col.aggregate || col.fn || col.func) {
        const fn = (col.func || col.aggregate || col.fn || '').toUpperCase();
        const column = col.arg || col.args?.[0]?.name || col.column || col.args?.[0] || '*';
        const alias = col.alias || `${fn.toLowerCase()}_${column}`;
        aggregates.push({ fn, column, alias });
      }
    }

    if (aggregates.length === 0) return null;

    return {
      groupBy: groupBy.map(g => typeof g === 'string' ? g : (g.column || g.name || g)),
      aggregates,
      having: ast.having || null,
    };
  }

  _compileHavingFilter(havingExpr) {
    return this._compileExpr(havingExpr);
  }

  _applyProjectAndLimit(rows, ast) {
    // Apply ORDER BY first (before projection to access all columns)
    if (ast.orderBy) {
      rows.sort((a, b) => {
        for (const ob of ast.orderBy) {
          const col = ob.column || ob.name;
          const dir = ob.direction === 'DESC' ? -1 : 1;
          const av = a[col], bv = b[col];
          const aNull = av === null || av === undefined;
          const bNull = bv === null || bv === undefined;
          if (aNull && bNull) continue;
          if (aNull) return -dir;
          if (bNull) return dir;
          if (av < bv) return -dir;
          if (av > bv) return dir;
        }
        return 0;
      });
    }

    // Apply projection
    if (ast.columns && !ast.columns.some(c => c === '*' || c.name === '*' || c.type === 'star')) {
      rows = rows.map(row => {
        const out = {};
        for (const col of ast.columns) {
          const name = col.alias || col.name;
          const srcName = col.table ? `${col.table}.${col.name}` : col.name;
          out[name] = row[srcName] !== undefined ? row[srcName] : row[col.name];
        }
        return out;
      });
    }

    // Apply DISTINCT — hash-based deduplication using JSON key
    if (ast.distinct) {
      const seen = new Set();
      const deduped = [];
      for (const row of rows) {
        const key = JSON.stringify(row);
        if (!seen.has(key)) {
          seen.add(key);
          deduped.push(row);
        }
      }
      rows = deduped;
    }

    // Apply OFFSET
    if (ast.offset) {
      const offsetVal = typeof ast.offset === 'number' ? ast.offset : ast.offset.value;
      if (offsetVal) rows = rows.slice(offsetVal);
    }

    // Apply LIMIT
    if (ast.limit) {
      const limitVal = typeof ast.limit === 'number' ? ast.limit : ast.limit.value;
      if (limitVal) rows = rows.slice(0, limitVal);
    }

    return rows;
  }

  /**
   * EXPLAIN COMPILED — show what the compiled engine would do.
   */
  explainCompiled(ast) {
    const plan = this.planner.plan(ast);
    const joins = ast.joins || [];
    
    const lines = [];
    lines.push(`=== Compiled Query Plan ===`);
    lines.push(`Table: ${plan.table}`);
    lines.push(`Access: ${plan.scanType} (est. ${plan.estimatedRows} rows, cost ${plan.estimatedCost?.toFixed(1) || '?'})`);
    
    if (plan.indexColumn) {
      lines.push(`  Index: ${plan.indexColumn}`);
    }

    for (let i = 0; i < (plan.joins || []).length; i++) {
      const j = plan.joins[i];
      lines.push(`Join ${i + 1}: ${j.type} with ${joins[i]?.table || '?'}`);
      lines.push(`  Strategy: ${j.type} (est. ${j.estimatedRows} rows)`);
      if (j.buildSide) lines.push(`  Build side: ${j.buildSide}`);
    }

    if (plan.totalCost) lines.push(`Total cost: ${plan.totalCost.toFixed(1)}`);
    if (plan.joinOrder) {
      lines.push(`Join order: ${plan.joinOrder.map(j => j.type).join(' → ')}`);
    }

    return lines.join('\n');
  }

  getStats() {
    return { ...this.stats };
  }
}
