// join-executor.js — JOIN executor methods extracted from db.js
// Note: These functions take 'db' as first parameter (database context)

export function executeJoinWithRows(db, leftRows, rightRows, join, rightAlias) {
  const result = [];
  
  if (join.joinType === 'CROSS') {
    for (const leftRow of leftRows) {
      for (const rightRow of rightRows) {
        result.push({ ...leftRow, ...rightRow });
      }
    }
    return result;
  }

  // Hash join optimization for equi-join in _executeJoinWithRows
  // Currently uses nested loop for correctness with complex column resolution.
  // TODO: implement hash join with proper column resolution for performance.

  // Fallback: nested loop join
  const rightMatched = new Set();
  for (const leftRow of leftRows) {
    let matched = false;
    for (let ri = 0; ri < rightRows.length; ri++) {
      const rightRow = rightRows[ri];
      // For NATURAL JOIN: preserve left values before merge overwrites them
      const combined = { ...leftRow, ...rightRow };
      if (join.natural) {
        for (const key of Object.keys(leftRow)) {
          combined[`__natural_left_${key}`] = leftRow[key];
        }
      }
      if (!join.on || db._evalExpr(join.on, combined)) {
        result.push(combined);
        matched = true;
        rightMatched.add(ri);
      }
    }
    if (!matched && (join.joinType === 'LEFT' || join.joinType === 'LEFT_OUTER' || join.joinType === 'FULL' || join.joinType === 'FULL_OUTER')) {
      const nullRow = {};
      for (const key of Object.keys(rightRows[0] || {})) {
        nullRow[key] = null;
      }
      result.push({ ...leftRow, ...nullRow });
    }
  }
  
  // RIGHT and FULL: add unmatched right rows
  if (join.joinType === 'RIGHT' || join.joinType === 'RIGHT_OUTER' || join.joinType === 'FULL' || join.joinType === 'FULL_OUTER') {
    // Get left column names from first row or fall back to empty
    const leftKeys = leftRows.length > 0 ? Object.keys(leftRows[0]) : [];
    for (let ri = 0; ri < rightRows.length; ri++) {
      if (!rightMatched.has(ri)) {
        const nullRow = {};
        for (const key of leftKeys) {
          nullRow[key] = null;
        }
        result.push({ ...nullRow, ...rightRows[ri] });
      }
    }
  }
  
  // Apply pushed-down filter if present
  if (join.filter) {
    return result.filter(row => db._evalExpr(join.filter, row));
  }
  
  return result;
}

export function executeJoin(db, leftRows, join, leftAlias) {
  // LATERAL JOIN: for each left row, evaluate the subquery with left row in scope
  if (join.lateral && join.subquery) {
    const rightAlias = join.alias || '__lateral';
    const result = [];
    
    for (const leftRow of leftRows) {
      // Set outer row for correlated subquery resolution
      const prevOuter = db._outerRow;
      db._outerRow = leftRow;
      
      let rightRows;
      try {
        const subResult = db._select(join.subquery);
        rightRows = subResult.rows.map(r => {
          const row = {};
          for (const [k, v] of Object.entries(r)) {
            row[k] = v;
            row[`${rightAlias}.${k}`] = v;
          }
          return row;
        });
      } finally {
        db._outerRow = prevOuter;
      }
      
      if (rightRows.length === 0) {
        if (join.joinType === 'LEFT') {
          result.push({ ...leftRow });
        }
        // INNER/CROSS: skip
      } else {
        for (const rightRow of rightRows) {
          const merged = { ...leftRow, ...rightRow };
          if (join.on) {
            if (db._evalExpr(join.on, merged)) {
              result.push(merged);
            } else if (join.joinType === 'LEFT') {
              result.push({ ...leftRow });
            }
          } else {
            result.push(merged);
          }
        }
      }
    }
    
    // Apply pushed-down filter predicates (e.g., WHERE sub.col > X pushed to join.filter)
    if (join.filter) {
      return result.filter(row => db._evalExpr(join.filter, row));
    }
    return result;
  }

  const rightTable = db.tables.get(join.table);
  const rightView = db.views.get(join.table);

  if (!rightTable && !rightView) throw new Error(`Table ${join.table} not found`);

  const rightAlias = join.alias || join.table;

  // SELF-JOIN OPTIMIZATION: detect when right table is explicitly the same table with different alias
  // Only detect based on table name match — column overlap is unreliable
  const leftTableName = leftAlias; // This is the alias used when scanning the left table
  const rightTableName = join.table;
  // We no longer try to route self-joins differently — the existing code handles them correctly
  // The key optimization is in EXPLAIN showing [Self-Join] detection

  // NATURAL JOIN or USING clause: auto-generate ON condition
  if ((join.natural || join.usingColumns) && rightTable && !join.on) {
    let sharedCols;
    if (join.usingColumns) {
      sharedCols = join.usingColumns;
    } else {
      const leftColNames = new Set();
      if (leftRows.length > 0) {
        for (const k of Object.keys(leftRows[0])) {
          const parts = k.split('.');
          leftColNames.add(parts[parts.length - 1]);
        }
      }
      const rightCols = rightTable.schema.map(c => c.name);
      sharedCols = rightCols.filter(c => leftColNames.has(c));
    }
    if (sharedCols.length > 0) {
      // Add qualified names to left rows so the join condition can resolve them
      for (const leftRow of leftRows) {
        for (const col of sharedCols) {
          if (leftRow[col] !== undefined && leftRow[`${leftAlias}.${col}`] === undefined) {
            leftRow[`${leftAlias}.${col}`] = leftRow[col];
          }
        }
      }
      // Build standard COMPARE conditions
      let onCondition = null;
      for (const col of sharedCols) {
        const cond = {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', name: `${leftAlias}.${col}` },
          right: { type: 'column_ref', name: `${rightAlias}.${col}` },
        };
        if (!onCondition) onCondition = cond;
        else onCondition = { type: 'AND', left: onCondition, right: cond };
      }
      join = { ...join, on: onCondition };
    }
  }

  // If right side is a view/CTE, get its rows
  if (rightView) {
    let rightRows;
    if (rightView.materializedRows) {
      rightRows = rightView.materializedRows.map(r => {
        const row = {};
        for (const [k, v] of Object.entries(r)) {
          row[k] = v;
          row[`${rightAlias}.${k}`] = v;
        }
        return row;
      });
    } else {
      const viewResult = db._select(rightView.query);
      rightRows = viewResult.rows.map(r => {
        const row = {};
        for (const [k, v] of Object.entries(r)) {
          row[k] = v;
          row[`${rightAlias}.${k}`] = v;
        }
        return row;
      });
    }
    return executeJoinWithRows(db, leftRows, rightRows, join, rightAlias);
  }

  const result = [];

  // CROSS JOIN
  if (join.joinType === 'CROSS') {
    for (const leftRow of leftRows) {
      for (const { values } of rightTable.heap.scan()) {
        const rightRow = db._valuesToRow(values, rightTable.schema, rightAlias);
        if (join.filter && !db._evalExpr(join.filter, rightRow)) continue;
        result.push({ ...leftRow, ...rightRow });
      }
    }
    return result;
  }

  // RIGHT or FULL JOIN: track matched right rows
  if (join.joinType === 'RIGHT' || join.joinType === 'FULL') {
    const rightMatchedSet = new Set();
    const rightRows = [];
    for (const { values } of rightTable.heap.scan()) {
      const row = db._valuesToRow(values, rightTable.schema, rightAlias);
      if (join.filter && !db._evalExpr(join.filter, row)) continue;
      rightRows.push(row);
    }

    for (const leftRow of leftRows) {
      let matched = false;
      for (let i = 0; i < rightRows.length; i++) {
        const combined = { ...leftRow, ...rightRows[i] };
        if (db._evalExpr(join.on, combined)) {
          result.push(combined);
          rightMatchedSet.add(i);
          matched = true;
        }
      }
      // FULL JOIN: add unmatched left rows with null right
      if (!matched && join.joinType === 'FULL') {
        const nullRow = {};
        for (const col of rightTable.schema) {
          nullRow[col.name] = null;
          nullRow[`${rightAlias}.${col.name}`] = null;
        }
        result.push({ ...leftRow, ...nullRow });
      }
    }

    // Add unmatched right rows with null left
    for (let i = 0; i < rightRows.length; i++) {
      if (!rightMatchedSet.has(i)) {
        const nullRow = {};
        for (const leftKey of Object.keys(leftRows[0] || {})) nullRow[leftKey] = null;
        result.push({ ...nullRow, ...rightRows[i] });
      }
    }

    return result;
  }

  // INNER or LEFT JOIN
  // Cost-based join method selection
  const equiJoinKey = extractEquiJoinKey(db, join.on, leftAlias, rightAlias);
  
  if (equiJoinKey) {
    const rightColName = equiJoinKey.rightKey;
    const rightIndex = rightTable.indexes?.get(rightColName);
    const rightRows = estimateRowCount(db, rightTable);
    
    const joinCost = compareJoinCosts(db, 
      leftRows.length, rightRows,
      true, !!rightIndex
    );
    
    if (joinCost.method === 'hash') {
      const hashResult = hashJoin(db, leftRows, rightTable, equiJoinKey, rightAlias, join.joinType, join.filter);
      if (hashResult) return hashResult;
    }
    
    if (joinCost.method === 'merge') {
      const mergeResult = mergeJoin(db, leftRows, rightTable, equiJoinKey, rightAlias, join.joinType);
      if (mergeResult) return mergeResult;
    }
    
    if (joinCost.method === 'index_nl' && rightIndex) {
      // Index nested-loop join: for each left row, look up matching right rows via index
      for (const leftRow of leftRows) {
        const lookupVal = leftRow[equiJoinKey.leftKey] !== undefined
          ? leftRow[equiJoinKey.leftKey]
          : db._resolveColumn(equiJoinKey.leftKey, leftRow);
        let matched = false;
        if (lookupVal != null) {
          // Use range(val, val) for non-unique indexes (returns all matching entries)
          const entries = rightIndex.range ? rightIndex.range(lookupVal, lookupVal) : [];
          for (const entry of entries) {
            const rid = entry.value || entry;
            const values = db._heapGetFollowHot(rightTable.heap, rid.pageId, rid.slotIdx);
            if (!values) continue;
            const rightRow = db._valuesToRow(values, rightTable.schema, rightAlias);
            if (join.filter && !db._evalExpr(join.filter, rightRow)) continue;
            const combined = { ...leftRow, ...rightRow };
            // Verify full join condition (handles compound conditions)
            if (db._evalExpr(join.on, combined)) {
              result.push(combined);
              matched = true;
            }
          }
        }
        if (!matched && join.joinType === 'LEFT') {
          const nullRow = {};
          for (const col of rightTable.schema) {
            nullRow[col.name] = null;
            nullRow[`${rightAlias}.${col.name}`] = null;
          }
          result.push({ ...leftRow, ...nullRow });
        }
      }
      return result;
    }
    
    // If preferred method failed, try alternatives as fallback
    if (joinCost.method !== 'hash') {
      const hashResult = hashJoin(db, leftRows, rightTable, equiJoinKey, rightAlias, join.joinType, join.filter);
      if (hashResult) return hashResult;
    }
    if (joinCost.method !== 'merge') {
      const mergeResult = mergeJoin(db, leftRows, rightTable, equiJoinKey, rightAlias, join.joinType);
      if (mergeResult) return mergeResult;
    }
  }

  // Fallback: nested loop join (full table scan)
  for (const leftRow of leftRows) {
    let matched = false;
    for (const { values } of rightTable.heap.scan()) {
      const rightRow = db._valuesToRow(values, rightTable.schema, rightAlias);
      // Apply pushed-down filter on right side
      if (join.filter && !db._evalExpr(join.filter, rightRow)) continue;
      const combined = { ...leftRow, ...rightRow };
      if (db._evalExpr(join.on, combined)) {
        result.push(combined);
        matched = true;
      }
    }
    if (!matched && join.joinType === 'LEFT') {
      const nullRow = {};
      for (const col of rightTable.schema) nullRow[`${rightAlias}.${col.name}`] = null;
      result.push({ ...leftRow, ...nullRow });
    }
  }

  return result;
}

/**
 * Extract equi-join key columns from a join condition AST.
 * Returns { leftKey, rightKey } if it's a simple equality, null otherwise.
 */
export function extractEquiJoinColumns(db, onExpr) {
  if (!onExpr || onExpr.type !== 'COMPARE' || onExpr.op !== 'EQ') return null;
  if (onExpr.left.type !== 'column_ref' || onExpr.right.type !== 'column_ref') return null;
  return { leftCol: onExpr.left.name, rightCol: onExpr.right.name };
}

export function extractEquiJoinKey(db, onExpr, leftAlias, rightAlias) {
  if (!onExpr || onExpr.type !== 'COMPARE' || onExpr.op !== 'EQ') return null;
  if (onExpr.left.type !== 'column_ref' || onExpr.right.type !== 'column_ref') return null;

  const leftCol = onExpr.left.name;
  const rightCol = onExpr.right.name;

  // Determine which column belongs to which table
  // Column refs can be "alias.col" or just "col"
  const isLeftSide = (col) => {
    if (col.startsWith(leftAlias + '.')) return true;
    // If no prefix, check if it exists in left rows
    return false;
  };
  const isRightSide = (col) => {
    if (col.startsWith(rightAlias + '.')) return true;
    return false;
  };

  let leftKey, rightKey;
  if (isLeftSide(leftCol) && isRightSide(rightCol)) {
    leftKey = leftCol;
    rightKey = rightCol.includes('.') ? rightCol.split('.').pop() : rightCol;
  } else if (isRightSide(leftCol) && isLeftSide(rightCol)) {
    leftKey = rightCol;
    rightKey = leftCol.includes('.') ? leftCol.split('.').pop() : leftCol;
  } else {
    // Can't determine sides — try both orientations
    // If left col has right alias prefix, swap
    if (leftCol.startsWith(rightAlias + '.')) {
      leftKey = rightCol;
      rightKey = leftCol.split('.').pop();
    } else {
      leftKey = leftCol;
      rightKey = rightCol.includes('.') ? rightCol.split('.').pop() : rightCol;
    }
  }

  return { leftKey, rightKey };
}

/**
 * Hash join: build hash table on right table, probe with left rows.
 * O(n + m) instead of O(n * m).
 */
export function hashJoin(db, leftRows, rightTable, keys, rightAlias, joinType, pushdownFilter) {
  const { leftKey, rightKey } = keys;

  // Build phase: hash the right table by join key
  const hashMap = new Map();
  const rightKeyIdx = rightTable.schema.findIndex(c => c.name === rightKey);
  if (rightKeyIdx < 0) {
    // Key not found in schema — fall back to nested loop
    return null;
  }

  for (const { values } of rightTable.heap.scan()) {
    // Apply pushed-down filter during build phase
    if (pushdownFilter) {
      const rightRow = db._valuesToRow(values, rightTable.schema, rightAlias);
      if (!db._evalExpr(pushdownFilter, rightRow)) continue;
    }
    const keyVal = values[rightKeyIdx];
    if (keyVal == null) continue; // NULL keys never match in SQL
    const keyStr = String(keyVal);
    if (!hashMap.has(keyStr)) hashMap.set(keyStr, []);
    hashMap.get(keyStr).push(values);
  }

  // Probe phase: look up each left row in the hash map
  const result = [];
  for (const leftRow of leftRows) {
    // Get the left key value — try with and without alias prefix
    let leftVal = leftRow[leftKey];
    if (leftVal === undefined) {
      // Try without alias prefix
      const bare = leftKey.includes('.') ? leftKey.split('.').pop() : leftKey;
      leftVal = leftRow[bare];
    }
    if (leftVal === undefined) {
      // Try all keys that end with the column name
      const bare = leftKey.includes('.') ? leftKey.split('.').pop() : leftKey;
      for (const k of Object.keys(leftRow)) {
        if (k === bare || k.endsWith('.' + bare)) {
          leftVal = leftRow[k];
          break;
        }
      }
    }

    const keyStr = leftVal == null ? null : String(leftVal);
    const matches = keyStr != null ? hashMap.get(keyStr) : undefined;
    let matched = false;

    if (matches) {
      for (const values of matches) {
        const rightRow = db._valuesToRow(values, rightTable.schema, rightAlias);
        result.push({ ...leftRow, ...rightRow });
        matched = true;
      }
    }

    if (!matched && joinType === 'LEFT') {
      const nullRow = {};
      for (const col of rightTable.schema) {
        nullRow[col.name] = null;
        nullRow[`${rightAlias}.${col.name}`] = null;
      }
      result.push({ ...leftRow, ...nullRow });
    }
  }

  return result;
}

export function mergeJoin(db, leftRows, rightTable, keys, rightAlias, joinType) {
  const { leftKey, rightKey } = keys;
  
  const rightKeyIdx = rightTable.schema.findIndex(c => c.name === rightKey);
  if (rightKeyIdx < 0) return null;
  
  const rightRows = [];
  for (const item of rightTable.heap.scan()) {
    rightRows.push(db._valuesToRow(item.values, rightTable.schema, rightAlias));
  }
  
  const compare = (va, vb) => {
    if (va === vb) return 0;
    if (va == null) return -1;
    if (vb == null) return 1;
    return va < vb ? -1 : va > vb ? 1 : 0;
  };
  
  const getLeftKey = (row) => row[leftKey] ?? row[Object.keys(row).find(k => k.endsWith('.' + leftKey))];
  const getRightKey = (row) => row[rightKey] ?? row[`${rightAlias}.${rightKey}`];
  
  const sortedLeft = [...leftRows].sort((a, b) => compare(getLeftKey(a), getLeftKey(b)));
  const sortedRight = [...rightRows].sort((a, b) => compare(getRightKey(a), getRightKey(b)));
  
  const result = [];
  let ri = 0;
  
  for (const leftRow of sortedLeft) {
    const lv = getLeftKey(leftRow);
    let matched = false;
    
    while (ri < sortedRight.length && compare(getRightKey(sortedRight[ri]), lv) < 0) ri++;
    
    let rj = ri;
    while (rj < sortedRight.length && getRightKey(sortedRight[rj]) === lv) {
      result.push({ ...leftRow, ...sortedRight[rj] });
      matched = true;
      rj++;
    }
    
    if (!matched && joinType === 'LEFT') {
      const nullRow = {};
      for (const col of rightTable.schema) {
        nullRow[col.name] = null;
        nullRow[`${rightAlias}.${col.name}`] = null;
      }
      result.push({ ...leftRow, ...nullRow });
    }
  }
  
  return result;
}

export function estimateRowCount(db, table) {
  // Use tracked row count if available
  if (table.heap?.rowCount !== undefined) return table.heap.rowCount;
  // Fallback: quick scan
  let count = 0;
  for (const _ of table.heap.scan()) count++;
  return count;
}

/**
 * Parametric cost model — compares sequential scan vs index scan costs.
 * Returns { useIndex: boolean, seqCost, indexCost, selectivity } 
 */
export function compareScanCosts(db, totalRows, estimatedResultRows) {
  const C = db.constructor.COST_MODEL;
  
  // Estimate pages: assume ~100 rows per page (8KB page, ~80 byte rows)
  const ROWS_PER_PAGE = 100;
  const totalPages = Math.max(1, Math.ceil(totalRows / ROWS_PER_PAGE));
  
  // Sequential scan cost: read all pages sequentially + process all rows + filter each row
  const seqCost = totalPages * C.seq_page_cost + 
                  totalRows * C.cpu_tuple_cost + 
                  totalRows * C.cpu_operator_cost;
  
  // Index scan cost: random I/O for each matching row + process index entries + process rows
  const selectivity = totalRows > 0 ? estimatedResultRows / totalRows : 0;
  const indexPages = Math.max(1, Math.ceil(estimatedResultRows / ROWS_PER_PAGE));
  
  // Mackert-Lohman formula: effective random pages based on correlation
  // For simplicity, use: min(indexPages, totalPages) * random_page_cost
  const effectivePages = Math.min(indexPages, totalPages);
  
  const indexCost = effectivePages * C.random_page_cost + 
                    estimatedResultRows * C.cpu_index_tuple_cost + 
                    estimatedResultRows * C.cpu_tuple_cost;
  
  // Index startup cost (B-tree traversal: log2(totalRows) * cpu_operator_cost)
  const indexStartup = Math.log2(Math.max(2, totalRows)) * C.cpu_operator_cost;
  
  const totalIndexCost = indexStartup + indexCost;
  
  return {
    useIndex: totalIndexCost < seqCost,
    seqCost: Math.round(seqCost * 100) / 100,
    indexCost: Math.round(totalIndexCost * 100) / 100,
    selectivity: Math.round(selectivity * 10000) / 10000,
  };
}

/**
 * Compare join method costs: hash join, index nested loop, nested loop.
 * Returns { method: 'hash'|'index_nl'|'nested_loop', costs: {...} }
 */
export function compareJoinCosts(db, leftRows, rightRows, hasEquiJoin, hasRightIndex) {
  const C = db.constructor.COST_MODEL;
  
  const costs = {};
  
  // Hash join: build hash table on right, probe with left
  // Cost: build = rightRows * cpu_tuple + memory. Probe = leftRows * cpu_tuple + leftRows * cpu_operator
  if (hasEquiJoin) {
    costs.hash = rightRows * C.cpu_tuple_cost +          // Build hash table
                 leftRows * C.cpu_tuple_cost +            // Scan left and probe
                 leftRows * C.cpu_operator_cost;          // Hash comparison per left row
  }
  
  // Index nested loop: for each left row, index lookup on right
  // Cost: leftRows * (tree_height * cpu_index + matches * cpu_tuple)
  if (hasEquiJoin && hasRightIndex) {
    const treeHeight = Math.max(1, Math.ceil(Math.log2(Math.max(2, rightRows))));
    const estimatedMatches = Math.max(1, rightRows / Math.max(1, rightRows)); // ~1 for unique
    costs.index_nl = leftRows * (treeHeight * C.cpu_index_tuple_cost + estimatedMatches * C.cpu_tuple_cost);
  }
  
  // Nested loop: for each left row, scan all right rows
  // Cost: leftRows * rightRows * cpu_operator (worst case)
  costs.nested_loop = leftRows * rightRows * C.cpu_operator_cost +
                      (leftRows + rightRows) * C.cpu_tuple_cost;
  
  // Merge join: sort both sides + linear merge
  // Cost: sort(left) + sort(right) + merge
  if (hasEquiJoin) {
    const sortLeft = leftRows > 1 ? leftRows * Math.log2(leftRows) * C.cpu_operator_cost : 0;
    const sortRight = rightRows > 1 ? rightRows * Math.log2(rightRows) * C.cpu_operator_cost : 0;
    const mergeCost = (leftRows + rightRows) * C.cpu_tuple_cost;
    costs.merge = sortLeft + sortRight + mergeCost;
  }
  
  // Pick cheapest
  let bestMethod = 'nested_loop';
  let bestCost = costs.nested_loop;
  
  if (costs.hash !== undefined && costs.hash < bestCost) {
    bestMethod = 'hash';
    bestCost = costs.hash;
  }
  if (costs.index_nl !== undefined && costs.index_nl < bestCost) {
    bestMethod = 'index_nl';
    bestCost = costs.index_nl;
  }
  if (costs.merge !== undefined && costs.merge < bestCost) {
    bestMethod = 'merge';
    bestCost = costs.merge;
  }
  
  return {
    method: bestMethod,
    bestCost: Math.round(bestCost * 100) / 100,
    costs: Object.fromEntries(Object.entries(costs).map(([k, v]) => [k, Math.round(v * 100) / 100])),
  };
}

/**
 * Estimate rows for a WHERE condition using ANALYZE stats.
 * Returns { estimated: number, method: string }
 */
export function estimateFilteredRows(db, tableName, where, totalRows) {
  if (!where) return { estimated: totalRows, method: 'no filter' };
  
  const stats = db._tableStats.get(tableName);
  if (!stats) return { estimated: Math.ceil(totalRows * 0.33), method: 'default 33%' };
  
  // Equality: col = value → use histogram if available, else uniform selectivity
  if (where.type === 'COMPARE' && where.op === 'EQ') {
    const col = where.left?.type === 'column_ref' ? where.left.name : 
                (where.right?.type === 'column_ref' ? where.right.name : null);
    const val = where.left?.type === 'literal' ? where.left.value :
                (where.right?.type === 'literal' ? where.right.value : null);
    if (col) {
      const colName = col.includes('.') ? col.split('.').pop() : col;
      const colStats = stats.columns[colName];
      if (colStats) {
        // Try histogram for better estimate on skewed data
        if (colStats.histogram && val != null && typeof val === 'number') {
          // Sum all buckets containing the target value
          // For exact-match buckets (lo===hi===val), use full count
          // For mixed buckets, estimate as count/ndv
          let est = 0;
          let found = false;
          for (const bucket of colStats.histogram) {
            if (val >= bucket.lo && val <= bucket.hi) {
              found = true;
              if (bucket.lo === bucket.hi) {
                // Bucket contains only this value
                est += bucket.count;
              } else {
                // Mixed bucket: estimate frequency as count/ndv
                est += (bucket.ndv > 0 ? bucket.count / bucket.ndv : bucket.count);
              }
            }
          }
          if (found) {
            return {
              estimated: Math.max(1, Math.ceil(est)),
              method: `histogram_eq(${colName})=${est.toFixed(1)}`,
            };
          }
          // Value outside all buckets → likely 0 rows
          return { estimated: 1, method: `histogram_eq(${colName})=out_of_range` };
        }
        return {
          estimated: Math.max(1, Math.ceil(totalRows * colStats.selectivity)),
          method: `selectivity(${colName})=${colStats.selectivity.toFixed(3)}`,
        };
      }
    }
  }
  
  // Range: col > val, col < val → use histogram if available, else linear interpolation
  if (where.type === 'COMPARE' && ['GT', 'GE', 'GTE', 'LT', 'LE', 'LTE'].includes(where.op)) {
    const col = where.left?.type === 'column_ref' ? where.left.name : null;
    const val = where.right?.type === 'literal' ? where.right.value : null;
    if (col && val != null) {
      const colName = col.includes('.') ? col.split('.').pop() : col;
      const colStats = stats.columns[colName];
      if (colStats) {
        // Try histogram for better range estimate
        if (colStats.histogram && typeof val === 'number') {
          let matchingRows = 0;
          const isGreater = where.op === 'GT' || where.op === 'GE' || where.op === 'GTE';
          const isInclusive = where.op === 'GE' || where.op === 'GTE' || where.op === 'LE' || where.op === 'LTE';
          for (const bucket of colStats.histogram) {
            if (isGreater) {
              // For > or >=: count rows in buckets above val
              if (bucket.lo > val || (isInclusive && bucket.lo >= val)) {
                matchingRows += bucket.count; // entire bucket qualifies
              } else if (val >= bucket.lo && val <= bucket.hi) {
                // Partial bucket: linear interpolation within bucket
                const bucketRange = bucket.hi - bucket.lo;
                const fraction = bucketRange > 0 ? (bucket.hi - val) / bucketRange : 0.5;
                matchingRows += Math.ceil(bucket.count * fraction);
              }
            } else {
              // For < or <=: count rows in buckets below val
              if (bucket.hi < val || (isInclusive && bucket.hi <= val)) {
                matchingRows += bucket.count; // entire bucket qualifies
              } else if (val >= bucket.lo && val <= bucket.hi) {
                // Partial bucket
                const bucketRange = bucket.hi - bucket.lo;
                const fraction = bucketRange > 0 ? (val - bucket.lo) / bucketRange : 0.5;
                matchingRows += Math.ceil(bucket.count * fraction);
              }
            }
          }
          return {
            estimated: Math.max(1, matchingRows),
            method: `histogram_range(${colName} ${where.op} ${val})=${matchingRows}`,
          };
        }
        // Fallback: linear interpolation with min/max
        if (colStats.min != null && colStats.max != null && colStats.max > colStats.min) {
          const range = colStats.max - colStats.min;
          let fraction;
          if (where.op === 'GT' || where.op === 'GE' || where.op === 'GTE') {
            fraction = Math.max(0, Math.min(1, (colStats.max - val) / range));
          } else {
            fraction = Math.max(0, Math.min(1, (val - colStats.min) / range));
          }
          return {
            estimated: Math.max(1, Math.ceil(totalRows * fraction)),
            method: `range(${colName}: ${fraction.toFixed(3)})`,
          };
        }
      }
    }
    return { estimated: Math.ceil(totalRows * 0.33), method: 'range ~33%' };
  }
  
  // AND: multiply selectivities
  if (where.type === 'AND') {
    const left = estimateFilteredRows(db, tableName, where.left, totalRows);
    const right = estimateFilteredRows(db, tableName, where.right, totalRows);
    return {
      estimated: Math.max(1, Math.ceil(left.estimated * right.estimated / totalRows)),
      method: `AND(${left.method}, ${right.method})`,
    };
  }
  
  // OR: add selectivities (capped at totalRows)
  if (where.type === 'OR') {
    const left = estimateFilteredRows(db, tableName, where.left, totalRows);
    const right = estimateFilteredRows(db, tableName, where.right, totalRows);
    return {
      estimated: Math.min(totalRows, left.estimated + right.estimated),
      method: `OR(${left.method}, ${right.method})`,
    };
  }
  
  // IS NULL / IS NOT NULL
  if (where.type === 'IS_NULL' || where.type === 'IS_NOT_NULL' || 
      (where.type === 'COMPARE' && (where.op === 'IS' || where.op === 'IS_NOT'))) {
    const col = where.left?.name || where.column?.name || where.operand?.name;
    if (col) {
      const colName = col.includes('.') ? col.split('.').pop() : col;
      const colStats = stats.columns[colName];
      if (colStats) {
        const nullFraction = totalRows > 0 ? colStats.nulls / totalRows : 0;
        if (where.type === 'IS_NULL' || where.op === 'IS') {
          return { estimated: Math.max(1, Math.ceil(totalRows * nullFraction)), method: `null_frac(${colName})=${nullFraction.toFixed(3)}` };
        } else {
          return { estimated: Math.max(1, Math.ceil(totalRows * (1 - nullFraction))), method: `not_null_frac(${colName})=${(1-nullFraction).toFixed(3)}` };
        }
      }
    }
  }

  // BETWEEN: use min/max interpolation
  if (where.type === 'BETWEEN') {
    const col = where.left?.name || where.column?.name || where.expr?.name;
    if (col) {
      const colName = col.includes('.') ? col.split('.').pop() : col;
      const colStats = stats.columns[colName];
      const lo = where.low?.value;
      const hi = where.high?.value;
      if (colStats && colStats.min != null && colStats.max != null && lo != null && hi != null) {
        const range = colStats.max - colStats.min;
        if (range > 0) {
          const fraction = Math.max(0, Math.min(1, (Math.min(hi, colStats.max) - Math.max(lo, colStats.min)) / range));
          return { estimated: Math.max(1, Math.ceil(totalRows * fraction)), method: `between(${colName}: ${fraction.toFixed(3)})` };
        }
      }
    }
  }

  // IN list: sum of per-value selectivities
  if (where.type === 'IN' || where.type === 'IN_LIST') {
    const col = where.left?.name || where.column?.name;
    if (col) {
      const colName = col.includes('.') ? col.split('.').pop() : col;
      const colStats = stats.columns[colName];
      const listLen = where.values?.length || where.list?.length || 3;
      if (colStats) {
        return { estimated: Math.max(1, Math.ceil(totalRows * colStats.selectivity * listLen)), method: `in(${colName}, ${listLen} values)` };
      }
    }
  }

  return { estimated: Math.ceil(totalRows * 0.33), method: 'default 33%' };
}

/**
 * Estimate the result size of joining two relations.
 * Uses the principle: |R ⋈ S| = |R| * |S| / max(ndv(R.key), ndv(S.key))
 */
export function estimateJoinSize(db, leftTable, leftRows, rightTableName, joinOn) {
  if (!joinOn) return leftRows * 10; // No join condition — cross join estimate
  
  // Extract join columns from ON condition (e.g., a.id = b.foreign_id)
  const joinCols = extractJoinColumns(db, joinOn);
  if (!joinCols) return leftRows * 10; // Can't parse — conservative estimate
  
  const rightTable = db.tables.get(rightTableName);
  if (!rightTable) return leftRows * 10;
  const rightRows = estimateRowCount(db, rightTable);
  
  // Get ndv from stats
  const leftStats = db._tableStats.get(joinCols.leftTable);
  const rightStats = db._tableStats.get(rightTableName);
  
  let leftNdv = leftRows; // default: assume unique
  let rightNdv = rightRows;
  
  if (leftStats?.columns[joinCols.leftCol]) {
    leftNdv = leftStats.columns[joinCols.leftCol].distinct || leftRows;
  }
  if (rightStats?.columns[joinCols.rightCol]) {
    rightNdv = rightStats.columns[joinCols.rightCol].distinct || rightRows;
  }
  
  // Standard formula: |R ⋈ S| = |R| * |S| / max(ndv_R, ndv_S)
  const maxNdv = Math.max(leftNdv, rightNdv, 1);
  return Math.max(1, Math.ceil(leftRows * rightRows / maxNdv));
}

/**
 * Extract left/right table and column from a join ON condition.
 * Handles: a.col = b.col or col = col patterns.
 */
export function extractJoinColumns(db, on) {
  if (!on || on.type !== 'COMPARE' || on.op !== 'EQ') return null;
  
  const left = on.left;
  const right = on.right;
  
  if (left?.type === 'column_ref' && right?.type === 'column_ref') {
    const leftParts = (left.table ? [left.table, left.name] : left.name.split('.'));
    const rightParts = (right.table ? [right.table, right.name] : right.name.split('.'));
    
    return {
      leftTable: leftParts.length > 1 ? leftParts[0] : null,
      leftCol: leftParts.length > 1 ? leftParts[1] : leftParts[0],
      rightTable: rightParts.length > 1 ? rightParts[0] : null,
      rightCol: rightParts.length > 1 ? rightParts[1] : rightParts[0],
    };
  }
  return null;
}

/**
 * Cost-based join ordering using dynamic programming (System R style).
 * For N tables, considers all orderings and picks the cheapest.
 * Only reorders INNER joins — LEFT/RIGHT/FULL preserve user order.
 */
export function optimizeJoinOrder(db, fromTable, joins) {
  // Only optimize if we have 2+ INNER joins and stats available
  const innerJoins = joins.filter(j => !j.joinType || j.joinType === 'INNER');
  if (innerJoins.length < 2) return joins;
  
  // Check if all joined tables have stats
  const tables = [fromTable, ...innerJoins.map(j => j.table)];
  const allHaveStats = tables.every(t => db._tableStats.has(t));
  if (!allHaveStats) return joins; // Can't optimize without stats
  
  // For small join counts (≤6 tables), do full DP enumeration
  if (innerJoins.length > 5) return joins; // Too many — don't try
  
  // Build adjacency: which tables can join which?
  const joinConditions = new Map(); // "tableA:tableB" -> join ON condition
  for (const j of innerJoins) {
    const cols = extractJoinColumns(db, j.on);
    if (cols) {
      const key1 = `${cols.leftTable || fromTable}:${j.table}`;
      const key2 = `${j.table}:${cols.leftTable || fromTable}`;
      joinConditions.set(key1, j);
      joinConditions.set(key2, j);
    }
  }
  
  // DP over subsets: dp[bitmask] = { cost, order, resultRows }
  const n = innerJoins.length;
  const tableNames = innerJoins.map(j => j.table);
  const allTables = [fromTable, ...tableNames];
  
  // Initialize single tables
  const dp = new Map();
  for (let i = 0; i < allTables.length; i++) {
    const mask = 1 << i;
    const table = db.tables.get(allTables[i]);
    const rows = table ? estimateRowCount(db, table) : 100;
    dp.set(mask, { cost: rows, rows, order: [i], lastTable: i });
  }
  
  // Build up larger subsets
  const fullMask = (1 << allTables.length) - 1;
  for (let size = 2; size <= allTables.length; size++) {
    // Enumerate all subsets of this size
    for (let mask = 1; mask <= fullMask; mask++) {
      if (popcount(db, mask) !== size) continue;
      
      let bestCost = Infinity;
      let bestPlan = null;
      
      // Try all ways to split this subset into (subset-1) + 1
      for (let i = 0; i < allTables.length; i++) {
        if (!(mask & (1 << i))) continue; // i not in mask
        
        const subMask = mask ^ (1 << i); // mask without table i
        const subPlan = dp.get(subMask);
        if (!subPlan) continue;
        
        // Check if table i can join with any table in subPlan
        const leftTable = allTables[subPlan.lastTable];
        const key = `${leftTable}:${allTables[i]}`;
        const altKey = `${allTables[i]}:${leftTable}`;
        
        // Also check any table in the subset
        let canJoin = joinConditions.has(key) || joinConditions.has(altKey);
        if (!canJoin) {
          for (const idx of subPlan.order) {
            const k1 = `${allTables[idx]}:${allTables[i]}`;
            const k2 = `${allTables[i]}:${allTables[idx]}`;
            if (joinConditions.has(k1) || joinConditions.has(k2)) {
              canJoin = true;
              break;
            }
          }
        }
        if (!canJoin) continue;
        
        // Estimate cost: subPlan.cost + join cost
        const rightTable = db.tables.get(allTables[i]);
        const rightRows = rightTable ? estimateRowCount(db, rightTable) : 100;
        
        // Join result estimate
        const maxNdv = Math.max(
          getTableNdv(db, allTables[subPlan.lastTable], allTables[i], joinConditions),
          1
        );
        const joinRows = Math.max(1, Math.ceil(subPlan.rows * rightRows / maxNdv));
        const cost = subPlan.cost + joinRows; // Total tuples processed
        
        if (cost < bestCost) {
          bestCost = cost;
          bestPlan = { cost, rows: joinRows, order: [...subPlan.order, i], lastTable: i };
        }
      }
      
      if (bestPlan) {
        const existing = dp.get(mask);
        if (!existing || bestPlan.cost < existing.cost) {
          dp.set(mask, bestPlan);
        }
      }
    }
  }
  
  // Get optimal order for all tables
  const optimal = dp.get(fullMask);
  if (!optimal) return joins; // DP failed, use original order
  
  // Reconstruct join list in optimal order
  // optimal.order gives indices into allTables; index 0 is fromTable (already the base)
  const reordered = [];
  const availableTables = new Set([fromTable]); // Tables whose columns are available
  const remainingJoins = [];
  
  for (const idx of optimal.order) {
    if (idx === 0) continue; // Skip the base table
    const tableName = allTables[idx];
    const join = innerJoins.find(j => j.table === tableName);
    if (join) remainingJoins.push(join);
  }
  
  // Greedy: emit joins in order where all referenced tables are available
  while (remainingJoins.length > 0) {
    let found = false;
    for (let i = 0; i < remainingJoins.length; i++) {
      const join = remainingJoins[i];
      const cols = extractJoinColumns(db, join.on);
      // Check if both sides of the ON condition reference available tables
      let canExecute = true;
      if (cols) {
        if (cols.leftTable && cols.leftTable !== join.table && !availableTables.has(cols.leftTable)) {
          canExecute = false;
        }
        if (cols.rightTable && cols.rightTable !== join.table && !availableTables.has(cols.rightTable)) {
          canExecute = false;
        }
      }
      if (canExecute) {
        reordered.push(join);
        availableTables.add(join.table);
        remainingJoins.splice(i, 1);
        found = true;
        break;
      }
    }
    if (!found) {
      // Can't find a valid next join — fallback: emit remaining in original order
      reordered.push(...remainingJoins);
      break;
    }
  }
  
  // Append any non-inner joins at the end (preserved in original order)
  const nonInner = joins.filter(j => j.joinType && j.joinType !== 'INNER');
  return [...reordered, ...nonInner];
}

export function popcount(db, n) {
  let count = 0;
  while (n) { count += n & 1; n >>= 1; }
  return count;
}

export function getTableNdv(db, table1, table2, joinConditions) {
  const key = `${table1}:${table2}`;
  const join = joinConditions.get(key);
  if (!join) return 1;
  
  const cols = extractJoinColumns(db, join.on);
  if (!cols) return 1;
  
  const stats1 = db._tableStats.get(table1);
  const stats2 = db._tableStats.get(table2);
  
  const ndv1 = stats1?.columns[cols.leftCol]?.distinct || 
               stats1?.columns[cols.rightCol]?.distinct || 1;
  const ndv2 = stats2?.columns[cols.leftCol]?.distinct ||
               stats2?.columns[cols.rightCol]?.distinct || 1;
  
  return Math.max(ndv1, ndv2);
}

