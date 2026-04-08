// compiled-query.js — Integrates cost-based planner with pipeline JIT compilation
// When a query is planned, this module generates optimized compiled code
// that executes the planner's chosen strategy without Volcano overhead.

import { QueryPlanner } from './planner.js';
import { compilePipelineJIT, JITCompiledIterator, compilePipeline, CompiledIterator } from './pipeline-compiler.js';
import { SeqScan, Filter, Project, Limit, HashJoin, Sort, HashAggregate } from './volcano.js';

/**
 * CompiledQueryEngine — takes a Database, plans queries, and compiles execution.
 * Bridges the planner's strategic decisions with the pipeline compiler's codegen.
 */
export class CompiledQueryEngine {
  constructor(database) {
    this.db = database;
    this.planner = new QueryPlanner(database);
    this.stats = { queriesCompiled: 0, queriesInterpreted: 0, totalCompileTimeMs: 0 };
  }

  /**
   * Execute a SELECT using planned + compiled execution.
   * Falls back to standard execution if compilation isn't beneficial.
   */
  executeSelect(ast) {
    const startTime = Date.now();
    
    // 1. Plan the query
    const plan = this.planner.plan(ast);
    
    // 2. Decide: compile or interpret?
    // Compile if: table has enough rows (overhead worthwhile) and no subqueries
    const tableStats = this.planner.getStats(ast.from?.table);
    const tableRows = tableStats?.rowCount || 0;
    const shouldCompile = tableRows >= 50 && !ast.where?.subquery;
    
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
      if (postFilter) {
        leftRows = leftRows.filter(postFilter);
      }
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
      return av < bv ? -1 : av > bv ? 1 : 0;
    });
    const sortedRight = [...rightRows].sort((a, b) => {
      const av = a[resolved.right], bv = b[resolved.right];
      return av < bv ? -1 : av > bv ? 1 : 0;
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
        if (left && right) return (row) => left(row) && right(row);
        return left || right;
      }
      case 'OR': {
        const left = this._compileExpr(expr.left);
        const right = this._compileExpr(expr.right);
        if (left && right) return (row) => left(row) || right(row);
        return null;
      }
      case 'NOT': {
        const inner = this._compileExpr(expr.expr);
        return inner ? (row) => !inner(row) : null;
      }
      default:
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

  _applyProjectAndLimit(rows, ast) {
    // Apply projection
    if (ast.columns && !ast.columns.some(c => c === '*' || c.name === '*')) {
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

    // Apply LIMIT
    if (ast.limit?.value) {
      rows = rows.slice(0, ast.limit.value);
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
