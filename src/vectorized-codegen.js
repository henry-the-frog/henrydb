// vectorized-codegen.js — Vectorized compiled execution
// Combines vectorized processing (column batches) with code generation.
// Instead of row-at-a-time: process 1024 rows at once in columnar format.
// This is the DuckDB/MonetDB approach adapted for JavaScript.

import { QueryPlanner } from './planner.js';

const BATCH_SIZE = 1024;

/**
 * VectorizedCodeGen — generates vectorized JavaScript functions.
 * Data is processed in column-oriented batches for cache efficiency.
 */
export class VectorizedCodeGen {
  constructor(database) {
    this.db = database;
    this.planner = new QueryPlanner(database);
    this.stats = { queriesCompiled: 0, totalCompileMs: 0, batchesProcessed: 0 };
  }

  /**
   * Compile and execute a SELECT query using vectorized processing.
   */
  execute(ast) {
    const startMs = Date.now();
    const tableName = ast.from?.table;
    if (!tableName) return null;

    const table = this.db.tables.get(tableName);
    if (!table) return null;

    const schema = table.schema.map(c => c.name);
    const colMap = {};
    schema.forEach((c, i) => colMap[c] = i);

    const joins = ast.joins || [];

    if (joins.length === 0) {
      const result = this._vectorizedScan(table, schema, colMap, ast);
      this.stats.queriesCompiled++;
      this.stats.totalCompileMs += Date.now() - startMs;
      return result;
    }

    const result = this._vectorizedJoin(table, schema, colMap, ast, joins);
    this.stats.queriesCompiled++;
    this.stats.totalCompileMs += Date.now() - startMs;
    return result;
  }

  /**
   * Vectorized scan: read table in batches, apply filter in bulk.
   */
  _vectorizedScan(table, schema, colMap, ast) {
    const limit = ast.limit?.value || Infinity;
    const filterFn = ast.where ? this._compileVectorFilter(ast.where, colMap) : null;
    const projCols = this._getProjection(ast, schema, colMap);

    // Read all rows into columnar format
    const columns = {};
    for (const name of schema) columns[name] = [];

    for (const { values } of table.heap.scan()) {
      for (let i = 0; i < schema.length; i++) {
        columns[schema[i]].push(values[i]);
      }
    }

    const totalRows = columns[schema[0]].length;

    // Process in batches
    const results = [];
    for (let batchStart = 0; batchStart < totalRows && results.length < limit; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE, totalRows);
      const batchLen = batchEnd - batchStart;
      this.stats.batchesProcessed++;

      // Create selection vector (which rows pass the filter)
      let selection;
      if (filterFn) {
        selection = new Uint32Array(batchLen);
        let selCount = 0;
        const batchCols = {};
        for (const name of schema) {
          batchCols[name] = columns[name];
        }
        
        for (let i = 0; i < batchLen; i++) {
          const rowIdx = batchStart + i;
          if (filterFn(batchCols, rowIdx)) {
            selection[selCount++] = rowIdx;
          }
        }
        selection = selection.subarray(0, selCount);
      } else {
        selection = new Uint32Array(batchLen);
        for (let i = 0; i < batchLen; i++) selection[i] = batchStart + i;
      }

      // Materialize selected rows
      for (let si = 0; si < selection.length && results.length < limit; si++) {
        const rowIdx = selection[si];
        const row = {};
        for (const { name, alias, colIdx } of projCols) {
          row[alias || name] = columns[schema[colIdx]][rowIdx];
        }
        results.push(row);
      }
    }

    return { rows: results };
  }

  /**
   * Vectorized hash join: build hash table in batches, probe in batches.
   */
  _vectorizedJoin(leftTable, leftSchema, leftColMap, ast, joins) {
    const limit = ast.limit?.value || Infinity;
    const leftAlias = ast.from.alias || ast.from.table;

    // Read left table into columnar format
    const leftColumns = {};
    for (const name of leftSchema) leftColumns[name] = [];
    for (const { values } of leftTable.heap.scan()) {
      for (let i = 0; i < leftSchema.length; i++) {
        leftColumns[leftSchema[i]].push(values[i]);
      }
    }
    const leftRows = leftColumns[leftSchema[0]].length;

    // Process each join
    let currentColumns = leftColumns;
    let currentSchema = [...leftSchema];
    let currentRows = leftRows;
    let currentColMap = { ...leftColMap };

    for (let ji = 0; ji < joins.length; ji++) {
      const join = joins[ji];
      const rightTableName = join.table;
      const rightTable = this.db.tables.get(rightTableName);
      if (!rightTable) continue;

      const rightSchema = rightTable.schema.map(c => c.name);
      const rightAlias = join.alias || rightTableName;

      // Read right table into columnar format
      const rightColumns = {};
      for (const name of rightSchema) rightColumns[name] = [];
      for (const { values } of rightTable.heap.scan()) {
        for (let i = 0; i < rightSchema.length; i++) {
          rightColumns[rightSchema[i]].push(values[i]);
        }
      }
      const rightRows = rightColumns[rightSchema[0]].length;

      // Extract join key columns
      const joinCols = this._extractJoinColumns(join.on);
      if (!joinCols) continue;

      const leftKeyCol = this._resolveJoinCol(joinCols[0], joinCols[1], currentColMap);
      const rightKeyCol = this._resolveJoinCol(joinCols[1], joinCols[0], 
        Object.fromEntries(rightSchema.map((c, i) => [c, i])));

      if (leftKeyCol === null || rightKeyCol === null) continue;

      // Build hash table on right side (vectorized: process in batches)
      const hashTable = new Map();
      for (let batchStart = 0; batchStart < rightRows; batchStart += BATCH_SIZE) {
        const batchEnd = Math.min(batchStart + BATCH_SIZE, rightRows);
        this.stats.batchesProcessed++;

        const keyCol = rightColumns[rightSchema[rightKeyCol]];
        for (let i = batchStart; i < batchEnd; i++) {
          const key = keyCol[i];
          if (!hashTable.has(key)) hashTable.set(key, []);
          hashTable.get(key).push(i); // Store row index, not row object
        }
      }

      // Probe in batches (vectorized)
      const newColumns = {};
      // Initialize output columns
      for (const name of currentSchema) newColumns[name] = [];
      for (const name of rightSchema) {
        const outName = currentSchema.includes(name) ? `${rightAlias}.${name}` : name;
        newColumns[outName] = [];
      }

      const leftKeyArr = currentColumns[currentSchema[leftKeyCol]];

      for (let batchStart = 0; batchStart < currentRows; batchStart += BATCH_SIZE) {
        const batchEnd = Math.min(batchStart + BATCH_SIZE, currentRows);
        this.stats.batchesProcessed++;

        for (let i = batchStart; i < batchEnd; i++) {
          const key = leftKeyArr[i];
          const matches = hashTable.get(key);

          if (matches) {
            for (const rightIdx of matches) {
              // Append to output columns
              for (const name of currentSchema) {
                newColumns[name].push(currentColumns[name][i]);
              }
              for (const name of rightSchema) {
                const outName = currentSchema.includes(name) ? `${rightAlias}.${name}` : name;
                newColumns[outName].push(rightColumns[name][rightIdx]);
              }
            }
          } else if (join.joinType === 'LEFT' || join.joinType === 'LEFT OUTER') {
            for (const name of currentSchema) {
              newColumns[name].push(currentColumns[name][i]);
            }
            for (const name of rightSchema) {
              const outName = currentSchema.includes(name) ? `${rightAlias}.${name}` : name;
              newColumns[outName].push(null);
            }
          }
        }
      }

      // Update current state for next join
      currentSchema = Object.keys(newColumns);
      currentColumns = newColumns;
      currentRows = newColumns[currentSchema[0]]?.length || 0;
      currentColMap = {};
      currentSchema.forEach((c, i) => currentColMap[c] = i);
    }

    // Materialize results with projection and limit
    const projCols = this._getJoinProjection(ast, currentSchema);
    const results = [];
    for (let i = 0; i < currentRows && results.length < limit; i++) {
      const row = {};
      for (const { name, alias } of projCols) {
        row[alias || name] = currentColumns[name][i];
      }
      results.push(row);
    }

    return { rows: results };
  }

  /**
   * Compile a vectorized filter function.
   * Takes (columns, rowIndex) → boolean.
   */
  _compileVectorFilter(expr, colMap) {
    if (!expr) return null;

    switch (expr.type) {
      case 'COMPARE': {
        const leftName = expr.left?.name;
        const rightVal = expr.right?.value;
        const leftIdx = colMap[leftName];

        if (leftIdx !== undefined && rightVal !== undefined) {
          const schema = Object.keys(colMap);
          const colName = schema[leftIdx];
          switch (expr.op) {
            case 'EQ': return (cols, idx) => cols[colName][idx] === rightVal;
            case 'NE': return (cols, idx) => cols[colName][idx] !== rightVal;
            case 'LT': return (cols, idx) => cols[colName][idx] < rightVal;
            case 'GT': return (cols, idx) => cols[colName][idx] > rightVal;
            case 'LE': return (cols, idx) => cols[colName][idx] <= rightVal;
            case 'GE': return (cols, idx) => cols[colName][idx] >= rightVal;
          }
        }
        return null;
      }
      case 'AND': {
        const left = this._compileVectorFilter(expr.left, colMap);
        const right = this._compileVectorFilter(expr.right, colMap);
        if (left && right) return (cols, idx) => left(cols, idx) && right(cols, idx);
        return left || right;
      }
      case 'OR': {
        const left = this._compileVectorFilter(expr.left, colMap);
        const right = this._compileVectorFilter(expr.right, colMap);
        if (left && right) return (cols, idx) => left(cols, idx) || right(cols, idx);
        return null;
      }
      default:
        return null;
    }
  }

  _getProjection(ast, schema, colMap) {
    if (!ast.columns || ast.columns.some(c => c === '*' || c.name === '*')) {
      return schema.map((name, i) => ({ name, alias: null, colIdx: i }));
    }
    return ast.columns.map(col => {
      const idx = colMap[col.name];
      return { name: col.name, alias: col.alias, colIdx: idx !== undefined ? idx : 0 };
    });
  }

  _getJoinProjection(ast, currentSchema) {
    if (!ast.columns || ast.columns.some(c => c === '*' || c.name === '*')) {
      return currentSchema.map(name => ({ name, alias: null }));
    }
    return ast.columns.map(col => {
      const name = col.table ? `${col.table}.${col.name}` : col.name;
      // Try exact match first, then search in schema
      if (currentSchema.includes(name)) return { name, alias: col.alias };
      if (currentSchema.includes(col.name)) return { name: col.name, alias: col.alias };
      return { name: col.name, alias: col.alias };
    });
  }

  _extractJoinColumns(onExpr) {
    if (!onExpr) return null;
    if (onExpr.type === 'COMPARE' && onExpr.op === 'EQ') {
      return [onExpr.left?.name, onExpr.right?.name];
    }
    if (onExpr.type === 'AND') {
      return this._extractJoinColumns(onExpr.left) || this._extractJoinColumns(onExpr.right);
    }
    return null;
  }

  _resolveJoinCol(col1, col2, colMap) {
    if (col1 in colMap) return colMap[col1];
    if (col2 in colMap) return colMap[col2];
    // Try without table prefix
    for (const key of Object.keys(colMap)) {
      if (key.endsWith(`.${col1}`)) return colMap[key];
      if (key.endsWith(`.${col2}`)) return colMap[key];
    }
    return null;
  }
}
