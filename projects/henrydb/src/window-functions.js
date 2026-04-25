// window-functions.js — SQL window function evaluator
// Extracted from db.js to reduce monolith size (~420 LOC)

import { exprContains, exprCollect } from './expr-walker.js';

export function exprContainsWindow(db, node) {
  return exprContains(node, n => n.type === 'window');
}

export function extractWindowNodes(db, node, results = [], prefix = '__wexpr') {
  const found = exprCollect(node, n => n.type === 'window');
  found.forEach((wn, i) => {
    wn._windowKey = `${prefix}_${results.length + i}`;
    results.push(wn);
  });
  return results;
}

export function columnsHaveWindow(db, columns) {
  return columns.some(c => c.type === 'window' || (c.type === 'expression' && exprContainsWindow(db, c.expr)));
}

export function validateNoNestedAggregates(db, columns) {
  for (const col of columns) {
    if (col.type === 'aggregate') {
      // Check if the argument contains another aggregate
      if (col.arg && typeof col.arg === 'object' && db._exprContainsAggregate(col.arg)) {
        throw new Error(`Aggregate function calls cannot be nested: ${col.func}(${col.arg.func || '...'}(...))`);
      }
    }
  }
}

export function validateNoWindowInWhere(db, where, clause = 'WHERE') {
  if (!where) return;
  if (exprContainsWindow(db, where)) {
    throw new Error(`Window functions are not allowed in ${clause} clause`);
  }
}

export function computeWindowFunctions(db, columns, rows, windowDefs) {
  // Collect both top-level window columns AND window functions nested in expressions
  const windowCols = columns.filter(c => c.type === 'window');
  // Also extract window nodes from expression columns
  const exprWindowNodes = [];
  for (const col of columns) {
    if (col.type === 'expression' && col.expr) {
      extractWindowNodes(db, col.expr, exprWindowNodes, `__wexpr_${col.alias || 'e'}`);
    }
  }
  // Process extracted expression window nodes as if they were top-level
  for (const wNode of exprWindowNodes) {
    windowCols.push(wNode);
  }

  for (const col of windowCols) {
    const name = col._windowKey || col.alias || `${col.func}(${col.arg || ''})`;
    
    // Resolve named window reference
    let overSpec = col.over;
    if (overSpec && overSpec.windowRef && windowDefs && windowDefs[overSpec.windowRef]) {
      overSpec = windowDefs[overSpec.windowRef];
    }
    const { partitionBy, orderBy, frame } = overSpec || {};

    // Partition rows
    const partitions = new Map();
    for (const row of rows) {
      const key = partitionBy
        ? partitionBy.map(c => typeof c === 'string' ? db._resolveColumn(c, row) : db._evalValue(c, row)).join('\0')
        : '__all__';
      if (!partitions.has(key)) partitions.set(key, []);
      partitions.get(key).push(row);
    }

    // Sort each partition
    for (const [, partition] of partitions) {
      if (orderBy) {
        partition.sort((a, b) => {
          for (const { column, direction } of orderBy) {
            const av = db._orderByValue(column, a);
            const bv = db._orderByValue(column, b);
            const cmp = av < bv ? -1 : av > bv ? 1 : 0;
            if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
          }
          return 0;
        });
      }

      // Compute window function values
      // Helper: get frame bounds for row at index i
      const getFrameBounds = (i, len) => {
        if (!frame) {
          // Default: with ORDER BY → RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          // Without ORDER BY → entire partition
          return orderBy ? [0, i] : [0, len - 1];
        }
        
        // RANGE mode: offset is based on ORDER BY value, not row position
        const isRange = frame.type === 'RANGE';
        const isGroups = frame.type === 'GROUPS';
        
        let start = 0, end = len - 1;
        
        if (isGroups && orderBy && orderBy.length > 0) {
          // GROUPS mode: offset counts groups of peer rows
          const orderCol = orderBy[0].column;
          const vals = partition.map(r => Number(db._orderByValue(orderCol, r)));
          
          // Build group boundaries: groups[g] = { startIdx, endIdx }
          const groups = [];
          let gStart = 0;
          for (let j = 1; j <= len; j++) {
            if (j === len || vals[j] !== vals[j - 1]) {
              groups.push({ start: gStart, end: j - 1 });
              gStart = j;
            }
          }
          
          // Find which group current row belongs to
          let currentGroup = 0;
          for (let g = 0; g < groups.length; g++) {
            if (i >= groups[g].start && i <= groups[g].end) { currentGroup = g; break; }
          }
          
          // Start bound
          if (frame.start.type === 'UNBOUNDED' && frame.start.direction === 'PRECEDING') {
            start = 0;
          } else if (frame.start.type === 'CURRENT ROW') {
            start = groups[currentGroup].start;
          } else if (frame.start.type === 'OFFSET') {
            const gIdx = frame.start.direction === 'PRECEDING'
              ? Math.max(0, currentGroup - frame.start.offset)
              : Math.min(groups.length - 1, currentGroup + frame.start.offset);
            start = groups[gIdx].start;
          }
          
          // End bound
          if (frame.end.type === 'UNBOUNDED' && frame.end.direction === 'FOLLOWING') {
            end = len - 1;
          } else if (frame.end.type === 'CURRENT ROW') {
            end = groups[currentGroup].end;
          } else if (frame.end.type === 'OFFSET') {
            const gIdx = frame.end.direction === 'FOLLOWING'
              ? Math.min(groups.length - 1, currentGroup + frame.end.offset)
              : Math.max(0, currentGroup - frame.end.offset);
            end = groups[gIdx].end;
          }
        } else if (isRange && orderBy && orderBy.length > 0) {
          // RANGE mode: find frame bounds by comparing ORDER BY values
          const orderCol = orderBy[0].column;
          const currentVal = Number(db._orderByValue(orderCol, partition[i]));
          
          // Start bound
          if (frame.start.type === 'UNBOUNDED' && frame.start.direction === 'PRECEDING') {
            start = 0;
          } else if (frame.start.type === 'CURRENT ROW') {
            // Find first row with same ORDER BY value (peers)
            start = i;
            while (start > 0 && Number(db._orderByValue(orderCol, partition[start - 1])) === currentVal) start--;
          } else if (frame.start.type === 'OFFSET') {
            const offset = frame.start.offset;
            const targetVal = frame.start.direction === 'PRECEDING' ? currentVal - offset : currentVal + offset;
            start = 0;
            for (let j = 0; j < len; j++) {
              if (Number(db._orderByValue(orderCol, partition[j])) >= targetVal) {
                start = j;
                break;
              }
            }
          }
          
          // End bound
          if (frame.end.type === 'UNBOUNDED' && frame.end.direction === 'FOLLOWING') {
            end = len - 1;
          } else if (frame.end.type === 'CURRENT ROW') {
            // Find last row with same ORDER BY value (peers)
            end = i;
            while (end < len - 1 && Number(db._orderByValue(orderCol, partition[end + 1])) === currentVal) end++;
          } else if (frame.end.type === 'OFFSET') {
            const offset = frame.end.offset;
            const targetVal = frame.end.direction === 'FOLLOWING' ? currentVal + offset : currentVal - offset;
            end = len - 1;
            for (let j = len - 1; j >= 0; j--) {
              if (Number(db._orderByValue(orderCol, partition[j])) <= targetVal) {
                end = j;
                break;
              }
            }
          }
        } else {
          // ROWS mode: offset is based on row position
          // Start bound
          if (frame.start.type === 'UNBOUNDED' && frame.start.direction === 'PRECEDING') {
            start = 0;
          } else if (frame.start.type === 'CURRENT ROW') {
            start = i;
          } else if (frame.start.type === 'OFFSET') {
            if (frame.start.direction === 'PRECEDING') {
              start = Math.max(0, i - frame.start.offset);
            } else {
              start = Math.min(len - 1, i + frame.start.offset);
            }
          }
          // End bound
          if (frame.end.type === 'UNBOUNDED' && frame.end.direction === 'FOLLOWING') {
            end = len - 1;
          } else if (frame.end.type === 'CURRENT ROW') {
            end = i;
          } else if (frame.end.type === 'OFFSET') {
            if (frame.end.direction === 'PRECEDING') {
              end = Math.max(0, i - frame.end.offset);
            } else {
              end = Math.min(len - 1, i + frame.end.offset);
            }
          }
        }
        
        // Apply EXCLUDE clause
        let excludeSet = null;
        if (frame.exclude) {
          excludeSet = new Set();
          if (frame.exclude === 'CURRENT ROW') {
            excludeSet.add(i);
          } else if (frame.exclude === 'GROUP' || frame.exclude === 'TIES') {
            // Find peer group (rows with same ORDER BY values)
            if (orderBy && orderBy.length > 0) {
              const orderCol = orderBy[0].column;
              const currentVal = Number(db._orderByValue(orderCol, partition[i]));
              for (let j = start; j <= end; j++) {
                if (Number(db._orderByValue(orderCol, partition[j])) === currentVal) {
                  if (frame.exclude === 'GROUP') {
                    excludeSet.add(j); // Exclude entire group
                  } else if (j !== i) {
                    excludeSet.add(j); // TIES: exclude peers but keep current
                  }
                }
              }
            }
          }
          // NO OTHERS = exclude nothing (default)
        }
        
        return [start, end, excludeSet];
      };

      // Helper: get frame rows for row at index i, respecting EXCLUDE
      const getFrameRows = (i, partition) => {
        const [start, end, excludeSet] = getFrameBounds(i, partition.length);
        const rows = [];
        for (let j = start; j <= end; j++) {
          if (excludeSet && excludeSet.has(j)) continue;
          rows.push(partition[j]);
        }
        return { start, end, excludeSet, rows };
      };

      // Helper: resolve window function argument value from a row
      const resolveArg = (arg, row) => {
        if (arg === '*') return 1;
        if (typeof arg === 'string') return db._resolveColumn(arg, row);
        if (arg && typeof arg === 'object') {
          if (arg.type === 'column_ref') return db._resolveColumn(arg.name, row);
          return db._evalValue(arg, row);
        }
        return db._resolveColumn(arg, row);
      };

      switch (col.func) {
        case 'ROW_NUMBER': {
          for (let i = 0; i < partition.length; i++) {
            partition[i][`__window_${name}`] = i + 1;
          }
          break;
        }
        case 'RANK': {
          let rank = 1;
          for (let i = 0; i < partition.length; i++) {
            if (i > 0 && orderBy) {
              const same = orderBy.every(({ column }) =>
                db._orderByValue(column, partition[i]) === db._orderByValue(column, partition[i - 1])
              );
              if (!same) rank = i + 1;
            }
            partition[i][`__window_${name}`] = rank;
          }
          break;
        }
        case 'DENSE_RANK': {
          let rank = 1;
          for (let i = 0; i < partition.length; i++) {
            if (i > 0 && orderBy) {
              const same = orderBy.every(({ column }) =>
                db._orderByValue(column, partition[i]) === db._orderByValue(column, partition[i - 1])
              );
              if (!same) rank++;
            }
            partition[i][`__window_${name}`] = rank;
          }
          break;
        }
        case 'COUNT': {
          for (let i = 0; i < partition.length; i++) {
            const { rows } = getFrameRows(i, partition);
            partition[i][`__window_${name}`] = rows.length;
          }
          break;
        }
        case 'SUM': {
          for (let i = 0; i < partition.length; i++) {
            const { rows } = getFrameRows(i, partition);
            let sum = 0;
            for (const row of rows) {
              const val = resolveArg(col.arg, row);
              const num = Number(val);
              sum += (isNaN(num) ? 0 : num);
            }
            partition[i][`__window_${name}`] = sum;
          }
          break;
        }
        case 'AVG': {
          for (let i = 0; i < partition.length; i++) {
            const { rows } = getFrameRows(i, partition);
            let sum = 0;
            for (const row of rows) {
              const val = resolveArg(col.arg, row);
              const num = Number(val);
              sum += (isNaN(num) ? 0 : num);
            }
            partition[i][`__window_${name}`] = rows.length > 0 ? sum / rows.length : null;
          }
          break;
        }
        case 'MIN': {
          for (let i = 0; i < partition.length; i++) {
            const { rows } = getFrameRows(i, partition);
            let min = Infinity;
            for (const row of rows) {
              const v = resolveArg(col.arg, row);
              if (v != null && v < min) min = v;
            }
            partition[i][`__window_${name}`] = min === Infinity ? null : min;
          }
          break;
        }
        case 'MAX': {
          for (let i = 0; i < partition.length; i++) {
            const { rows } = getFrameRows(i, partition);
            let max = -Infinity;
            for (const row of rows) {
              const v = resolveArg(col.arg, row);
              if (v != null && v > max) max = v;
            }
            partition[i][`__window_${name}`] = max === -Infinity ? null : max;
          }
          break;
        }
        case 'LAG': {
          const lagArg = typeof col.arg === 'object' && col.arg?.name ? col.arg.name : col.arg;
          const offset = col.offset || 1;
          const defaultVal = col.defaultValue ?? null;
          for (let i = 0; i < partition.length; i++) {
            const prevIdx = i - offset;
            if (prevIdx >= 0) {
              partition[i][`__window_${name}`] = db._resolveColumn(lagArg, partition[prevIdx]);
            } else {
              partition[i][`__window_${name}`] = defaultVal;
            }
          }
          break;
        }
        case 'LEAD': {
          const leadArg = typeof col.arg === 'object' && col.arg?.name ? col.arg.name : col.arg;
          const offset2 = col.offset || 1;
          const defaultVal2 = col.defaultValue ?? null;
          for (let i = 0; i < partition.length; i++) {
            const nextIdx = i + offset2;
            if (nextIdx < partition.length) {
              partition[i][`__window_${name}`] = db._resolveColumn(leadArg, partition[nextIdx]);
            } else {
              partition[i][`__window_${name}`] = defaultVal2;
            }
          }
          break;
        }
        case 'NTILE': {
          const nArg = typeof col.arg === 'object' && col.arg?.value ? col.arg.value : (col.arg || 4);
          const n = Math.min(nArg, partition.length);
          const baseSize = Math.floor(partition.length / n);
          const remainder = partition.length % n;
          let idx = 0;
          for (let tile = 1; tile <= n; tile++) {
            const size = baseSize + (tile <= remainder ? 1 : 0);
            for (let j = 0; j < size; j++) {
              partition[idx++][`__window_${name}`] = tile;
            }
          }
          break;
        }
        case 'FIRST_VALUE': {
          const fvArg = typeof col.arg === 'object' && col.arg?.name ? col.arg.name : col.arg;
          for (let i = 0; i < partition.length; i++) {
            const [start] = getFrameBounds(i, partition.length);
            partition[i][`__window_${name}`] = db._resolveColumn(fvArg, partition[start]);
          }
          break;
        }
        case 'LAST_VALUE': {
          const lvArg = typeof col.arg === 'object' && col.arg?.name ? col.arg.name : col.arg;
          for (let i = 0; i < partition.length; i++) {
            const [, end] = getFrameBounds(i, partition.length);
            partition[i][`__window_${name}`] = db._resolveColumn(lvArg, partition[end]);
          }
          break;
        }
        case 'NTH_VALUE': {
          // NTH_VALUE(col, n) — returns the value of col at the nth row in the partition
          const nvArg = typeof col.arg === 'object' && col.arg?.name ? col.arg.name : col.arg;
          const n = col.offset || 1; // offset stores the second argument
          if (partition.length >= n) {
            const nthVal = db._resolveColumn(nvArg, partition[n - 1]);
            for (let i = 0; i < partition.length; i++) {
              // With default frame (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
              // NTH_VALUE returns null until the frame includes the nth row
              if (orderBy && i < n - 1) {
                partition[i][`__window_${name}`] = null;
              } else {
                partition[i][`__window_${name}`] = nthVal;
              }
            }
          } else {
            for (const r of partition) r[`__window_${name}`] = null;
          }
          break;
        }
        case 'CUME_DIST': {
          // CUME_DIST = fraction of rows with value <= current row's value
          // For ties, all tied rows get the same value (highest rank / total)
          const n = partition.length;
          if (n === 0) break;
          for (let i = 0; i < n; i++) {
            // Find last row with same ORDER BY value (ties)
            let lastTie = i;
            while (lastTie + 1 < n && windowOrderEqual(db, partition[i], partition[lastTie + 1], orderBy)) {
              lastTie++;
            }
            const cumeDist = (lastTie + 1) / n;
            for (let j = i; j <= lastTie; j++) {
              partition[j][`__window_${name}`] = cumeDist;
            }
            i = lastTie; // Skip ties
          }
          break;
        }
        case 'PERCENT_RANK': {
          // PERCENT_RANK = (rank - 1) / (N - 1), 0 for first row
          const n = partition.length;
          if (n <= 1) {
            for (const r of partition) r[`__window_${name}`] = 0;
            break;
          }
          let rank = 1;
          for (let i = 0; i < n; i++) {
            if (i > 0 && !windowOrderEqual(db, partition[i - 1], partition[i], orderBy)) {
              rank = i + 1;
            }
            partition[i][`__window_${name}`] = (rank - 1) / (n - 1);
          }
          break;
        }
      }
    }
  }

  // Sort output by the first window function's ORDER BY
  // This matches PostgreSQL behavior: window ORDER BY implies output order
  // when no explicit SELECT-level ORDER BY is present
  if (windowCols.length > 0) {
    let firstOver = windowCols[0].over;
    if (firstOver && firstOver.windowRef && windowDefs && windowDefs[firstOver.windowRef]) {
      firstOver = windowDefs[firstOver.windowRef];
    }
    const firstOrderBy = firstOver?.orderBy;
    if (firstOrderBy && firstOrderBy.length > 0) {
      rows.sort((a, b) => {
        for (const { column, direction } of firstOrderBy) {
          const av = typeof column === 'string' ? db._resolveColumn(column, a) : db._evalValue(column, a);
          const bv = typeof column === 'string' ? db._resolveColumn(column, b) : db._evalValue(column, b);
          const cmp = av < bv ? -1 : av > bv ? 1 : 0;
          if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
        }
        return 0;
      });
    }
  }

  return rows;
}

export function windowOrderEqual(db, rowA, rowB, orderBy) {
  if (!orderBy || orderBy.length === 0) return true;
  for (const ob of orderBy) {
    const va = db._orderByValue(ob.column, rowA);
    const vb = db._orderByValue(ob.column, rowB);
    if (va !== vb) return false;
  }
  return true;
}
