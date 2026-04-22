// cte-executor.js — CTE execution extracted from db.js
// Functions take "db" as first parameter (database context)

export function withCTEs(db, ast, fn) {
  if (!ast.ctes || ast.ctes.length === 0) return fn();
  const tempViews = [];
  for (const cte of ast.ctes) {
    if (db.views.has(cte.name)) throw new Error(`CTE name ${cte.name} conflicts with existing view`);
    if (cte.recursive && (cte.unionQuery || cte.query.type === 'UNION')) {
      const allRows = executeRecursiveCTE(db, cte);
      db.views.set(cte.name, { materializedRows: allRows, isCTE: true });
    } else if (cte.unionQuery || cte.query.type === 'UNION') {
      const leftResult = db._select(cte.query);
      const rightResult = db.execute_ast(cte.unionQuery);
      const allRows = [...leftResult.rows, ...rightResult.rows];
      db.views.set(cte.name, { materializedRows: allRows, isCTE: true });
    } else {
      db.views.set(cte.name, { query: cte.query, isCTE: true });
    }
    tempViews.push(cte.name);
  }
  try {
    return fn();
  } finally {
    for (const name of tempViews) db.views.delete(name);
  }
}


export function executeRecursiveCTE(db, cte) {
  const MAX_ITERATIONS = 1000;

  // Split into base and recursive parts
  let baseQuery, recursiveQuery;
  if (cte.query.type === 'UNION') {
    baseQuery = cte.query.left;
    recursiveQuery = cte.query.right;
  } else {
    baseQuery = cte.query;
    recursiveQuery = cte.unionQuery;
  }

  // Step 1: Execute base query
  const baseResult = db._select(baseQuery);
  let columnNames = Object.keys(baseResult.rows[0] || {});
  
  // Apply CTE column aliases if provided: WITH RECURSIVE cnt(x) AS (...)
  if (cte.columns && cte.columns.length > 0) {
    // Determine mapping: if CTE has more column names than row keys, there
    // are duplicate column names in the base query. Use AST to resolve them.
    // Otherwise, use row keys (more reliable).
    const rowKeys = Object.keys(baseResult.rows[0] || {});
    let mappingKeys;
    if (rowKeys.length < cte.columns.length) {
      // Duplicate column names detected — use AST column order
      // Map AST column references to row keys
      mappingKeys = baseQuery.columns.map(c => {
        if (c.alias) return c.alias;
        if (c.name) return c.name;
        if (c.type === 'aggregate') return c.alias || `${c.func}(${c.arg || '*'})`;
        return c.alias || c.name || 'expr';
      });
    } else {
      mappingKeys = rowKeys;
    }
    const aliasedRows = baseResult.rows.map(row => {
      const aliased = {};
      for (let i = 0; i < cte.columns.length && i < mappingKeys.length; i++) {
        aliased[cte.columns[i]] = row[mappingKeys[i]];
      }
      return aliased;
    });
    baseResult.rows = aliasedRows;
    columnNames = cte.columns.slice(0, mappingKeys.length);
  }
  let allRows = [...baseResult.rows];
  let workingSet = [...baseResult.rows];

  // Initialize CYCLE tracking with base rows
  if (cte.cycle) {
    db._cycleVisited = new Set();
    const cycleCols = cte.cycle.columns;
    for (const row of allRows) {
      const key = cycleCols.map(c => String(row[c] ?? '')).join('|||');
      db._cycleVisited.add(key);
      row[cte.cycle.setCycleCol] = cte.cycle.defaultVal;
      row[cte.cycle.pathCol] = key;
    }
  }

  // Step 2: Iterate until fixed point
  for (let iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
    if (workingSet.length === 0) break;

    // Register current working set as the CTE view
    db.views.set(cte.name, { materializedRows: workingSet, isCTE: true });

    // Execute recursive part
    const recursiveResult = db._select(recursiveQuery);
    let newRows = recursiveResult.rows;

    if (newRows.length === 0) break;

    // Normalize column names to match base query
    if (columnNames.length > 0) {
      newRows = newRows.map(row => {
        const normalized = {};
        const rowKeys = Object.keys(row);
        for (let i = 0; i < columnNames.length && i < rowKeys.length; i++) {
          normalized[columnNames[i]] = row[rowKeys[i]];
        }
        return normalized;
      });
    }

    // CYCLE clause handling
    if (cte.cycle) {
      const { columns: cycleCols, setCycleCol, cycleMarkVal, defaultVal, pathCol } = cte.cycle;
      // Track visited states by cycle columns
      if (!db._cycleVisited) db._cycleVisited = new Set();
      
      // Compute cycle key for each new row
      const filteredNew = [];
      for (const row of newRows) {
        const cycleKey = cycleCols.map(c => String(row[c] ?? '')).join('|||');
        if (db._cycleVisited.has(cycleKey)) {
          // This row would create a cycle — mark it but don't recurse
          row[setCycleCol] = cycleMarkVal;
          row[pathCol] = '(cycle)';
          filteredNew.push(row); // Include the cycle row but don't add to working set
        } else {
          db._cycleVisited.add(cycleKey);
          row[setCycleCol] = defaultVal;
          row[pathCol] = cycleKey;
          filteredNew.push(row);
        }
      }
      
      // Only non-cycle rows continue recursion
      const nonCycleRows = filteredNew.filter(r => r[setCycleCol] !== cycleMarkVal);
      allRows.push(...filteredNew);
      workingSet = nonCycleRows;
      
      if (nonCycleRows.length === 0) {
        delete db._cycleVisited;
        break;
      }
      continue;
    }

    // Default cycle detection: check if any new row already exists in allRows
    const seenKeys = new Set(allRows.map(r => JSON.stringify(Object.values(r))));
    const uniqueNew = newRows.filter(r => !seenKeys.has(JSON.stringify(Object.values(r))));

    if (uniqueNew.length === 0) break;

    allRows.push(...uniqueNew);
    workingSet = uniqueNew;
  }

  return allRows;
}

