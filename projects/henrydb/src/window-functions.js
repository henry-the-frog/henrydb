// window-functions.js — SQL Window Functions implementation
// Supports: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/AVG/MIN/MAX/COUNT OVER
// With PARTITION BY and ORDER BY clauses.

/**
 * Execute window functions on a result set.
 * 
 * @param {Array<Object>} rows - Input rows
 * @param {Array<Object>} windowSpecs - Window function specifications
 * @returns {Array<Object>} Rows with window function results appended
 */
export function applyWindowFunctions(rows, windowSpecs) {
  if (!rows.length || !windowSpecs.length) return rows;

  // Clone rows to avoid mutation
  let result = rows.map(r => ({ ...r }));

  for (const spec of windowSpecs) {
    result = applyOneWindow(result, spec);
  }

  return result;
}

/**
 * Apply a single window function.
 * 
 * @param {Array<Object>} rows
 * @param {Object} spec - { func, args, partitionBy, orderBy, alias, frameStart, frameEnd }
 */
function applyOneWindow(rows, spec) {
  const { func, args = [], partitionBy = [], orderBy = [], alias } = spec;

  // Partition rows
  const partitions = partition(rows, partitionBy);

  for (const group of partitions.values()) {
    // Sort partition
    if (orderBy.length > 0) {
      group.sort((a, b) => {
        for (const { column, direction = 'ASC' } of orderBy) {
          const va = a[column], vb = b[column];
          let cmp = va < vb ? -1 : va > vb ? 1 : 0;
          if (direction === 'DESC') cmp = -cmp;
          if (cmp !== 0) return cmp;
        }
        return 0;
      });
    }

    // Apply function
    switch (func.toUpperCase()) {
      case 'ROW_NUMBER':
        group.forEach((row, i) => { row[alias] = i + 1; });
        break;

      case 'RANK':
        applyRank(group, orderBy, alias, false);
        break;

      case 'DENSE_RANK':
        applyRank(group, orderBy, alias, true);
        break;

      case 'NTILE': {
        const n = args[0] || 1;
        const size = group.length;
        group.forEach((row, i) => {
          row[alias] = Math.floor(i * n / size) + 1;
        });
        break;
      }

      case 'LAG': {
        const col = args[0];
        const offset = args[1] || 1;
        const defaultVal = args[2] ?? null;
        group.forEach((row, i) => {
          row[alias] = i >= offset ? group[i - offset][col] : defaultVal;
        });
        break;
      }

      case 'LEAD': {
        const col = args[0];
        const offset = args[1] || 1;
        const defaultVal = args[2] ?? null;
        group.forEach((row, i) => {
          row[alias] = i + offset < group.length ? group[i + offset][col] : defaultVal;
        });
        break;
      }

      case 'FIRST_VALUE': {
        const col = args[0];
        const val = group.length > 0 ? group[0][col] : null;
        group.forEach(row => { row[alias] = val; });
        break;
      }

      case 'LAST_VALUE': {
        const col = args[0];
        // Default frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        group.forEach((row, i) => { row[alias] = group[i][col]; });
        break;
      }

      case 'SUM':
      case 'AVG':
      case 'MIN':
      case 'MAX':
      case 'COUNT':
        applyAggregate(group, func.toUpperCase(), args[0], alias, spec);
        break;

      default:
        throw new Error(`Unknown window function: ${func}`);
    }
  }

  return rows; // Original rows were mutated via references in partitions
}

function applyRank(group, orderBy, alias, dense) {
  let rank = 1;
  let denseRank = 1;
  
  group.forEach((row, i) => {
    if (i === 0) {
      row[alias] = 1;
      return;
    }

    const prev = group[i - 1];
    let same = true;
    for (const { column } of orderBy) {
      if (row[column] !== prev[column]) { same = false; break; }
    }

    if (same) {
      row[alias] = dense ? denseRank : rank;
    } else {
      if (dense) {
        denseRank++;
        row[alias] = denseRank;
      } else {
        rank = i + 1;
        row[alias] = rank;
      }
    }
  });
}

function applyAggregate(group, func, col, alias, spec) {
  const frameStart = spec.frameStart || 'UNBOUNDED PRECEDING';
  const frameEnd = spec.frameEnd || 'CURRENT ROW';

  group.forEach((row, i) => {
    const start = resolveFrame(frameStart, i, group.length);
    const end = resolveFrame(frameEnd, i, group.length);

    let result;
    const values = [];
    for (let j = start; j <= end; j++) {
      if (col) values.push(group[j][col]);
      else values.push(1); // COUNT(*)
    }

    switch (func) {
      case 'SUM': result = values.reduce((a, b) => a + b, 0); break;
      case 'AVG': result = values.length > 0 ? values.reduce((a, b) => a + b, 0) / values.length : null; break;
      case 'MIN': result = Math.min(...values); break;
      case 'MAX': result = Math.max(...values); break;
      case 'COUNT': result = values.length; break;
    }

    row[alias] = result;
  });
}

function resolveFrame(frame, currentIdx, size) {
  if (frame === 'UNBOUNDED PRECEDING') return 0;
  if (frame === 'CURRENT ROW') return currentIdx;
  if (frame === 'UNBOUNDED FOLLOWING') return size - 1;
  if (typeof frame === 'number') return Math.max(0, Math.min(size - 1, currentIdx + frame));
  return currentIdx;
}

function partition(rows, partitionBy) {
  if (partitionBy.length === 0) return new Map([['_all_', rows]]);

  const groups = new Map();
  for (const row of rows) {
    const key = partitionBy.map(col => String(row[col])).join('|');
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  return groups;
}
