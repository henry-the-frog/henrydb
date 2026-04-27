// group-by-executor.js — GROUP BY executor extracted from db.js
// Functions take "db" as first parameter (database context)

import { percentileCont, median } from './percentile.js';
import { sqliteCompare } from './type-affinity.js';

export function selectWithGroupBy(db, ast, rows) {
  // Build alias→expression map from SELECT columns
  const aliasMap = new Map();
  // Also build alias→column name map for simple renames (name AS person)
  const columnAliasMap = new Map();
  for (const col of ast.columns) {
    if (col.alias) {
      if (col.type === 'column') columnAliasMap.set(col.alias, col.name);
      else if (col.type === 'expression' && col.expr) aliasMap.set(col.alias, col.expr);
      else if (col.type === 'function') aliasMap.set(col.alias, col);
      else if (col.type === 'case') aliasMap.set(col.alias, col);
    }
  }
  
  // Helper: resolve GROUP BY column (string or expression)
  // If string matches a SELECT alias, use that expression instead
  const resolveGroupKey = (col, row) => {
    if (typeof col === 'string') {
      if (aliasMap.has(col)) {
        const expr = aliasMap.get(col);
        if (expr.type === 'function') return db._evalFunction(expr.func, expr.args, row);
        if (expr.type === 'case') return db._evalCase(expr, row);
        return db._evalValue(expr, row);
      }
      // Simple column alias (e.g., name AS person → resolve to name)
      if (columnAliasMap.has(col)) {
        return db._resolveColumn(columnAliasMap.get(col), row);
      }
      return db._resolveColumn(col, row);
    }
    // Ordinal position: GROUP BY 1 → first SELECT column
    if (col.type === 'literal' && typeof col.value === 'number') {
      const idx = col.value - 1; // 1-based
      if (idx >= 0 && idx < ast.columns.length) {
        const selCol = ast.columns[idx];
        if (selCol.type === 'column') return db._resolveColumn(selCol.name, row);
        if (selCol.type === 'expression' && selCol.expr) return db._evalValue(selCol.expr, row);
        if (selCol.type === 'function') return db._evalFunction(selCol.func, selCol.args, row);
        if (selCol.type === 'case') return db._evalCase(selCol, row);
        return db._evalValue(selCol, row);
      }
    }
    return db._evalValue(col, row); // Expression
  };

  // Handle GROUPING SETS / ROLLUP / CUBE
  let groupingSets = null;
  let effectiveGroupBy = ast.groupBy;
  
  if (ast.groupBy && !Array.isArray(ast.groupBy)) {
    if (ast.groupBy.type === 'ROLLUP') {
      // ROLLUP(a, b, c) = GROUPING SETS ((a,b,c), (a,b), (a), ())
      const cols = ast.groupBy.columns;
      groupingSets = [];
      for (let i = cols.length; i >= 0; i--) {
        groupingSets.push(cols.slice(0, i));
      }
    } else if (ast.groupBy.type === 'CUBE') {
      // CUBE(a, b) = all subsets: (a,b), (a), (b), ()
      const cols = ast.groupBy.columns;
      groupingSets = [];
      for (let mask = (1 << cols.length) - 1; mask >= 0; mask--) {
        const set = [];
        for (let i = 0; i < cols.length; i++) {
          if (mask & (1 << (cols.length - 1 - i))) set.push(cols[i]);
        }
        groupingSets.push(set);
      }
    } else if (ast.groupBy.type === 'GROUPING_SETS') {
      groupingSets = ast.groupBy.sets;
    }
  }

  if (groupingSets) {
    // Execute query for each grouping set and UNION ALL
    const allCols = ast.groupBy.columns || groupingSets.flat().filter((v, i, a) => a.indexOf(v) === i);
    let allRows = [];
    // Remove ORDER BY from sub-queries to avoid re-sorting
    const baseAst = { ...ast, orderBy: null, limit: null, offset: null };
    for (const setCols of groupingSets) {
      const subAst = { ...baseAst, groupBy: setCols.length > 0 ? setCols : null };
      const subResult = db._select(subAst);
      // NULL out columns not in this grouping set
      for (const row of subResult.rows) {
        for (const col of allCols) {
          const colName = typeof col === 'string' ? col : (col.alias || col.name);
          if (!setCols.includes(col)) {
            row[colName] = null;
          }
        }
      }
      allRows = allRows.concat(subResult.rows);
    }
    // Apply ORDER BY and LIMIT to combined results
    if (ast.orderBy) {
      allRows.sort((a, b) => {
        for (const o of ast.orderBy) {
          const colName = typeof o.column === 'string' ? o.column : (o.column.name || o.column.alias);
          const av = a[colName], bv = b[colName];
          if (av === null && bv !== null) return o.direction === 'DESC' ? -1 : 1;
          if (av !== null && bv === null) return o.direction === 'DESC' ? 1 : -1;
          if (av < bv) return o.direction === 'DESC' ? 1 : -1;
          if (av > bv) return o.direction === 'DESC' ? -1 : 1;
        }
        return 0;
      });
    }
    if (ast.offset) allRows = allRows.slice(Math.max(0, ast.offset));
    if (ast.limit != null) allRows = allRows.slice(0, ast.limit);
    return { rows: allRows, columns: allRows.length > 0 ? Object.keys(allRows[0]) : [] };
  }

  // Group rows by GROUP BY columns
  const groups = new Map();
  for (const row of rows) {
    const key = ast.groupBy.map(col => resolveGroupKey(col, row)).join('\0');
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  // Compute aggregates per group
  let resultRows = [];
  for (const [, groupRows] of groups) {
    const result = {};

    // Add GROUP BY columns
    for (const col of ast.groupBy) {
      if (typeof col === 'string') {
        if (aliasMap.has(col)) {
          // Alias refers to a SELECT expression — evaluate and use alias as key
          const expr = aliasMap.get(col);
          let val;
          if (expr.type === 'function') val = db._evalFunction(expr.func, expr.args, groupRows[0]);
          else if (expr.type === 'case') val = db._evalCase(expr, groupRows[0]);
          else val = db._evalValue(expr, groupRows[0]);
          result[col] = val;
        } else if (columnAliasMap.has(col)) {
          // Simple column alias (name AS person) — resolve to real column, use alias as output key
          const realCol = columnAliasMap.get(col);
          const val = db._resolveColumn(realCol, groupRows[0]);
          result[col] = val;
        } else {
          const val = db._resolveColumn(col, groupRows[0]);
          result[col] = val;
        }
      } else {
        // Ordinal position: GROUP BY 1 → resolve to SELECT column
        if (col.type === 'literal' && typeof col.value === 'number') {
          const idx = col.value - 1;
          if (idx >= 0 && idx < ast.columns.length) {
            const selCol = ast.columns[idx];
            const outKey = selCol.alias || selCol.name || `col_${col.value}`;
            let val;
            if (selCol.type === 'column') val = db._resolveColumn(selCol.name, groupRows[0]);
            else if (selCol.type === 'expression' && selCol.expr) val = db._evalValue(selCol.expr, groupRows[0]);
            else if (selCol.type === 'function') val = db._evalFunction(selCol.func, selCol.args, groupRows[0]);
            else if (selCol.type === 'case') val = db._evalCase(selCol, groupRows[0]);
            else val = db._evalValue(selCol, groupRows[0]);
            result[outKey] = val;
            continue;
          }
        }
        // Expression group key — evaluate and use the matching SELECT column alias
        const val = db._evalValue(col, groupRows[0]);
        // Find matching SELECT column alias for this expression
        let key;
        for (const selCol of ast.columns) {
          if (selCol.alias && selCol.type === 'expression') {
            // Check if the expressions match (by comparing stringified AST)
            const selExpr = selCol.expr || selCol;
            if (JSON.stringify(selExpr) === JSON.stringify(col)) {
              key = selCol.alias;
              break;
            }
          }
        }
        if (!key) {
          // No alias found — try to generate a readable name
          if (col.type === 'arith') {
            const left = typeof col.left === 'string' ? col.left : (col.left?.name || '?');
            const right = typeof col.right === 'object' ? (col.right?.value ?? '?') : col.right;
            key = `${left} ${col.op} ${right}`;
          } else {
            key = col.alias || `expr_${ast.groupBy.indexOf(col)}`;
          }
        }
        result[key] = val;
      }
    }

    // Helper to compute an aggregate on this group
    const computeAgg = (func, arg, distinct, extra = {}) => {
      // Apply FILTER clause if present
      let effectiveRows = groupRows;
      if (extra.filter) {
        effectiveRows = groupRows.filter(r => {
          try { return !!db._evalExpr(extra.filter, r); } catch { return false; }
        });
      }
      // Normalize star arg: parser may produce string '*' or {type:'column_ref', name:'*'}
      const isStar = arg === '*' || (typeof arg === 'object' && arg?.name === '*');
      let values;
      if (isStar) {
        values = effectiveRows;
      } else if (typeof arg === 'object') {
        values = effectiveRows.map(r => db._evalValue(arg, r)).filter(v => v != null);
      } else {
        values = effectiveRows.map(r => db._resolveColumn(arg, r)).filter(v => v != null);
      }
      switch (func) {
        case 'COUNT': {
          if (distinct && !isStar) return new Set(values).size;
          return isStar ? effectiveRows.length : values.length;
        }
        case 'SUM': return values.length ? values.reduce((s, v) => s + v, 0) : null;
        case 'TOTAL': return values.reduce((s, v) => s + v, 0.0); // Always float, 0.0 if empty
        case 'AVG': return values.length ? values.reduce((s, v) => s + v, 0) / values.length : null;
        case 'MIN': return values.length ? values.reduce((a, b) => sqliteCompare(a, b) < 0 ? a : b) : null;
        case 'MAX': return values.length ? values.reduce((a, b) => sqliteCompare(a, b) > 0 ? a : b) : null;
        case 'MEDIAN': return median(values);
        case 'GROUP_CONCAT':
        case 'STRING_AGG': {
          const sep = extra.separator || ',';
          let items = distinct ? [...new Set(values)] : values;
          // Apply ORDER BY if specified inside the aggregate
          if (extra.aggOrderBy && extra.aggOrderBy.length > 0 && extra.groupRows) {
            const ordered = extra.groupRows.slice();
            ordered.sort((a, b) => {
              for (const ob of extra.aggOrderBy) {
                const av = db._evalValue(ob.column, a);
                const bv = db._evalValue(ob.column, b);
                if (av < bv) return ob.direction === 'DESC' ? 1 : -1;
                if (av > bv) return ob.direction === 'DESC' ? -1 : 1;
              }
              return 0;
            });
            items = ordered.map(r => {
              const v = typeof extra.aggArg === 'string' ? r[extra.aggArg] : db._evalValue(extra.aggArg, r);
              return v;
            }).filter(v => v != null);
          }
          const strs = items.map(String);
          return strs.length ? strs.join(sep) : null;
        }
        case 'JSON_AGG':
        case 'JSONB_AGG': {
          const vals = distinct ? [...new Set(values)] : values;
          const parsed = vals.map(v => {
            if (typeof v === 'string') {
              try { return JSON.parse(v); } catch { return v; }
            }
            return v;
          });
          return JSON.stringify(parsed);
        }
        case 'ARRAY_AGG': {
          let items = distinct ? [...new Set(values)] : values;
          // Apply ORDER BY if specified inside the aggregate
          if (extra.aggOrderBy && extra.aggOrderBy.length > 0 && extra.groupRows) {
            const ordered = extra.groupRows.slice();
            ordered.sort((a, b) => {
              for (const ob of extra.aggOrderBy) {
                const av = db._evalValue(ob.column, a);
                const bv = db._evalValue(ob.column, b);
                if (av < bv) return ob.direction === 'DESC' ? 1 : -1;
                if (av > bv) return ob.direction === 'DESC' ? -1 : 1;
              }
              return 0;
            });
            items = ordered.map(r => {
              const v = typeof extra.aggArg === 'string' ? r[extra.aggArg] : db._evalValue(extra.aggArg, r);
              return v;
            }).filter(v => v != null);
          }
          return items;
        }
        case 'BOOL_AND':
        case 'EVERY': {
          const boolVals = (arg === '*' ? groupRows : values).filter(v => v != null);
          return boolVals.length === 0 ? null : boolVals.every(v => !!v);
        }
        case 'BOOL_OR': {
          const boolVals = (arg === '*' ? groupRows : values).filter(v => v != null);
          return boolVals.length === 0 ? null : boolVals.some(v => !!v);
        }
        case 'PERCENTILE_CONT': {
          const fraction = extra.percentile ?? 0.5;
          return percentileCont(values, fraction);
        }
        case 'PERCENTILE_DISC': {
          const fraction2 = extra.percentile ?? 0.5;
          const sorted2 = values.map(Number).sort((a, b) => a - b);
          if (sorted2.length === 0) return null;
          const idx2 = Math.ceil(fraction2 * sorted2.length) - 1;
          return sorted2[Math.max(0, Math.min(idx2, sorted2.length - 1))];
        }
        case 'MODE': {
          if (values.length === 0) return null;
          const freq = new Map();
          for (const v of values) freq.set(v, (freq.get(v) || 0) + 1);
          let maxFreq = 0, modeVal = null;
          for (const [v, count] of freq) {
            if (count > maxFreq) { maxFreq = count; modeVal = v; }
          }
          return modeVal;
        }
        case 'STDDEV':
        case 'STDDEV_SAMP': {
          const nums = values.map(Number);
          if (nums.length < 2) return null;
          const mean = nums.reduce((a, b) => a + b, 0) / nums.length;
          return Math.sqrt(nums.reduce((sum, x) => sum + (x - mean) ** 2, 0) / (nums.length - 1));
        }
        case 'STDDEV_POP': {
          const nums2 = values.map(Number);
          if (nums2.length === 0) return null;
          const mean2 = nums2.reduce((a, b) => a + b, 0) / nums2.length;
          return Math.sqrt(nums2.reduce((sum, x) => sum + (x - mean2) ** 2, 0) / nums2.length);
        }
        case 'VARIANCE':
        case 'VAR_SAMP': {
          const nums3 = values.map(Number);
          if (nums3.length < 2) return null;
          const mean3 = nums3.reduce((a, b) => a + b, 0) / nums3.length;
          return nums3.reduce((sum, x) => sum + (x - mean3) ** 2, 0) / (nums3.length - 1);
        }
        case 'VAR_POP': {
          const nums4 = values.map(Number);
          if (nums4.length === 0) return null;
          const mean4 = nums4.reduce((a, b) => a + b, 0) / nums4.length;
          return nums4.reduce((sum, x) => sum + (x - mean4) ** 2, 0) / nums4.length;
        }
        case 'CORR':
        case 'COVAR_POP':
        case 'COVAR_SAMP':
        case 'REGR_SLOPE':
        case 'REGR_INTERCEPT':
        case 'REGR_R2':
        case 'REGR_COUNT': {
          const arg2Col = extra.arg2;
          if (!arg2Col) return null;
          const pairs = [];
          for (const row of effectiveRows) {
            const y = typeof arg === 'object' ? db._evalValue(arg, row) : db._resolveColumn(arg, row);
            const x = typeof arg2Col === 'object' ? db._evalValue(arg2Col, row) : db._resolveColumn(arg2Col, row);
            if (y != null && x != null) pairs.push([Number(y), Number(x)]);
          }
          if (func === 'REGR_COUNT') return pairs.length;
          if (pairs.length < 2) return null;
          const n = pairs.length;
          const mY = pairs.reduce((s, [y]) => s + y, 0) / n;
          const mX = pairs.reduce((s, [, x]) => s + x, 0) / n;
          const covP = pairs.reduce((s, [y, x]) => s + (y - mY) * (x - mX), 0) / n;
          const vXP = pairs.reduce((s, [, x]) => s + (x - mX) ** 2, 0) / n;
          const vYP = pairs.reduce((s, [y]) => s + (y - mY) ** 2, 0) / n;
          switch (func) {
            case 'COVAR_POP': return covP;
            case 'COVAR_SAMP': return covP * n / (n - 1);
            case 'CORR': { const d = Math.sqrt(vXP * vYP); return d > 0 ? covP / d : null; }
            case 'REGR_SLOPE': return vXP > 0 ? covP / vXP : null;
            case 'REGR_INTERCEPT': return vXP > 0 ? mY - (covP / vXP) * mX : null;
            case 'REGR_R2': { const c = (vXP > 0 && vYP > 0) ? covP / Math.sqrt(vXP * vYP) : null; return c !== null ? c ** 2 : null; }
          }
        }
      }
    };

    // Add aggregate and non-aggregate columns
    for (const col of ast.columns) {
      if (col.type === 'aggregate') {
        let name = col.alias || `${col.func}(${col.arg})`;
        if (name in result) { let s = 1; while (`${name}_${s}` in result) s++; name = `${name}_${s}`; }
        result[name] = computeAgg(col.func, col.arg, col.distinct, { separator: col.separator, aggOrderBy: col.aggOrderBy, filter: col.filter, groupRows, aggArg: col.arg, percentile: col.percentile, arg2: col.arg2 });
        // Also store under canonical key for HAVING resolution
        const canonKey = `${col.func}(${col.arg})`;
        if (name !== canonKey) result[`__agg_${canonKey}`] = result[name];
      } else if (col.type === 'column') {
        const baseName = col.name.includes('.') ? col.name.split('.').pop() : col.name;
        const name = col.alias || baseName;
        result[name] = db._resolveColumn(col.name, groupRows[0]);
      } else if (col.type === 'expression') {
        // Expression columns (CASE, arithmetic, etc.) — evaluate with aggregate support
        const name = col.alias || 'expr';
        const expr = col.expr;
        // Check if expression contains aggregate references — if so, compute them
        result[name] = db._evalGroupExpr(expr, groupRows, result, computeAgg);
      } else if (col.type === 'function') {
        // Function columns (COALESCE, ROUND, etc.) — may contain aggregates
        const name = col.alias || `${col.func}(...)`;
        if (col.args && col.args.some(a => db._exprContainsAggregate(a))) {
          // Evaluate each argument, computing aggregates
          const evaluatedArgs = col.args.map(arg => {
            if (db._exprContainsAggregate(arg)) {
              if (arg.type === 'aggregate_expr') {
                return computeAgg(arg.func, arg.arg, arg.distinct);
              }
              return db._evalGroupExpr(arg, groupRows, result, computeAgg);
            }
            return arg.type === 'literal' ? arg.value : db._evalValue(arg, groupRows[0]);
          });
          // Apply the function
          if (col.func.toUpperCase() === 'COALESCE') {
            result[name] = evaluatedArgs.find(v => v !== null && v !== undefined) ?? null;
          } else if (col.func.toUpperCase() === 'ROUND') {
            result[name] = evaluatedArgs[1] !== undefined ? 
              Number(Number(evaluatedArgs[0]).toFixed(evaluatedArgs[1])) :
              Math.round(evaluatedArgs[0]);
          } else if (col.func.toUpperCase() === 'NULLIF') {
            result[name] = evaluatedArgs[0] === evaluatedArgs[1] ? null : evaluatedArgs[0];
          } else if (col.func.toUpperCase() === 'IFNULL' || col.func.toUpperCase() === 'NVL') {
            result[name] = evaluatedArgs[0] ?? evaluatedArgs[1];
          } else {
            result[name] = db._evalFunction(col.func, evaluatedArgs.map(v => ({ type: 'literal', value: v })), groupRows[0]);
          }
        } else {
          result[name] = db._evalFunction(col.func, col.args, groupRows[0]);
        }
      }
    }

    // Pre-compute aggregates used in HAVING that aren't in SELECT
    if (ast.having) {
      db._collectAggregateExprs(ast.having).forEach(agg => {
        const argStr = db._serializeExpr(agg.arg);
        const key = `${agg.func}(${argStr})`;
        if (!(key in result) && !(`__agg_${key}` in result)) {
          result[`__agg_${key}`] = computeAgg(agg.func, agg.arg, agg.distinct);
        }
      });
    }

    resultRows.push(result);
  }

  // HAVING
  if (ast.having) {
    resultRows = resultRows.filter(row => db._evalExpr(ast.having, row));
  }

  // ORDER BY
  if (ast.orderBy) {
    resultRows.sort((a, b) => {
      for (const { column, direction } of ast.orderBy) {
        const av = db._orderByValue(column, a);
        const bv = db._orderByValue(column, b);
        const aNull = av === null || av === undefined;
        const bNull = bv === null || bv === undefined;
        if (aNull && bNull) continue;
        if (aNull) return direction === 'DESC' ? 1 : -1;
        if (bNull) return direction === 'DESC' ? -1 : 1;
        const cmp = av < bv ? -1 : av > bv ? 1 : 0;
        if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
      }
      return 0;
    });
  }

  // LIMIT
  if (ast.offset) resultRows = resultRows.slice(Math.max(0, ast.offset));
  if (ast.limit != null) resultRows = resultRows.slice(0, ast.limit);

  // Strip internal __agg_ keys before returning
  resultRows = resultRows.map(row => {
    const clean = {};
    for (const [k, v] of Object.entries(row)) {
      if (k.startsWith('__agg_')) continue;
      // Skip qualified GROUP BY keys that duplicate a SELECT column
      if (k.includes('.')) {
        const unqualified = k.split('.').pop();
        if (unqualified in row) continue;
      }
      clean[k] = v;
    }
    return clean;
  });

  // Apply window functions on grouped results (GROUP BY + window function combo)
  const hasWindow = db._columnsHaveWindow(ast.columns);
  if (hasWindow) {
    // Build a map of aggregate expressions to their aliases in the grouped results
    const aggAliasMap = new Map();
    for (const col of ast.columns) {
      if (col.type === 'aggregate' && col.alias) {
        aggAliasMap.set(JSON.stringify({ func: col.func, arg: col.arg }), col.alias);
      }
    }
    
    // Rewrite window ORDER BY columns that reference aggregates to use the alias
    const rewrittenColumns = ast.columns.map(col => {
      if (col.type !== 'window' || !col.over?.orderBy) return col;
      const newOrderBy = col.over.orderBy.map(ob => {
        if (ob.column && typeof ob.column === 'object' && ob.column.type === 'aggregate_expr') {
          const key = JSON.stringify({ func: ob.column.func, arg: ob.column.arg?.name || ob.column.arg });
          const alias = aggAliasMap.get(key);
          if (alias) {
            return { ...ob, column: alias };
          }
        }
        return ob;
      });
      return { ...col, over: { ...col.over, orderBy: newOrderBy } };
    });
    
    resultRows = db._computeWindowFunctions(rewrittenColumns, resultRows, ast.windowDefs);
    // Re-project to include window columns
    resultRows = resultRows.map(row => {
      const clean = {};
      for (const col of ast.columns) {
        const alias = col.alias || col.name || col.func;
        if (col.type === 'window') {
          clean[alias] = row[`__window_${alias}`];
        } else if (alias in row) {
          clean[alias] = row[alias];
        }
      }
      return clean;
    });
  }

  return { type: 'ROWS', rows: resultRows };
}
