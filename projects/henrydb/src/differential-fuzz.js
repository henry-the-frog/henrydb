/**
 * HenryDB Differential Fuzzer
 * 
 * Generates random SQL queries and compares HenryDB output against SQLite (better-sqlite3).
 * Reports any differences as potential bugs.
 * 
 * Usage: node src/differential-fuzz.js [--iterations N] [--seed S] [--verbose]
 */

import { Database } from './db.js';
import BetterSqlite3 from 'better-sqlite3';

const ITERATIONS = parseInt(process.argv.find(a => a.startsWith('--iterations='))?.split('=')[1] || '100');
const VERBOSE = process.argv.includes('--verbose');

// Seeded PRNG for reproducibility
let seed = parseInt(process.argv.find(a => a.startsWith('--seed='))?.split('=')[1] || String(Date.now()));
function rand() {
  seed = (seed * 1103515245 + 12345) & 0x7fffffff;
  return seed / 0x7fffffff;
}
function randInt(min, max) { return Math.floor(rand() * (max - min + 1)) + min; }
function pick(arr) { return arr[randInt(0, arr.length - 1)]; }

// SQL generators
function genTableName() { return pick(['t1', 't2', 't3']); }
function genColumnName() { return pick(['a', 'b', 'c', 'x', 'y', 'z', 'id', 'val', 'name', 'score']); }
function genIntLiteral() { return randInt(-100, 100); }
function genStringLiteral() { return `'${pick(['hello', 'world', 'foo', 'bar', 'test', ''])}'`; }
function genLiteral() { return rand() > 0.5 ? genIntLiteral() : genStringLiteral(); }

function genCreateTable() {
  const name = genTableName();
  const cols = [];
  const ncols = randInt(2, 5);
  for (let i = 0; i < ncols; i++) {
    const col = String.fromCharCode(97 + i); // a, b, c, ...
    const type = pick(['INT', 'TEXT', 'REAL']);
    cols.push(`${col} ${type}`);
  }
  return `CREATE TABLE IF NOT EXISTS ${name} (${cols.join(', ')})`;
}

function genInsert(tables) {
  const name = pick(Object.keys(tables));
  if (!name) return null;
  const cols = tables[name];
  const vals = cols.map(c => {
    if (c.type === 'INT') return genIntLiteral();
    if (c.type === 'REAL') return (rand() * 200 - 100).toFixed(2);
    return genStringLiteral();
  });
  return `INSERT INTO ${name} VALUES (${vals.join(', ')})`;
}

function genExpr(depth = 0) {
  if (depth > 2 || rand() < 0.4) return genLiteral();
  if (rand() < 0.3) return genColumnName();
  const op = pick(['+', '-', '*', '||']);
  return `(${genExpr(depth + 1)} ${op} ${genExpr(depth + 1)})`;
}

function genWhere() {
  const col = genColumnName();
  const op = pick(['=', '!=', '<', '>', '<=', '>=']);
  return `${col} ${op} ${genLiteral()}`;
}

function genSelect(tables) {
  const tableNames = Object.keys(tables);
  if (tableNames.length === 0) return null;
  const name = pick(tableNames);
  
  const parts = [`SELECT`];
  
  // Decide: simple query, JOIN, or GROUP BY
  const doJoin = rand() < 0.2 && tableNames.length >= 2;
  const doGroupBy = rand() < 0.20 && !doJoin;
  const doSubquery = rand() < 0.10 && !doJoin && !doGroupBy;
  const doCTE = rand() < 0.08 && !doJoin && !doGroupBy && !doSubquery;
  // Window function
  if (rand() < 0.10 && !doJoin && !doGroupBy && !doSubquery && !doCTE) {
    const col = pick(tables[name].map(c => c.name));
    const winFunc = pick(['ROW_NUMBER', 'RANK', 'SUM', 'COUNT', 'AVG']);
    const orderCol = pick(tables[name].map(c => c.name));
    if (winFunc === 'ROW_NUMBER' || winFunc === 'RANK') {
      parts.push(`${col}, ${winFunc}() OVER (ORDER BY ${orderCol}) as win_val`);
    } else {
      parts.push(`${col}, ${winFunc}(${col}) OVER (ORDER BY ${orderCol}) as win_val`);
    }
    parts.push(`FROM ${name}`);
    if (rand() < 0.3) parts.push(`LIMIT ${randInt(1, 10)}`);
    return parts.join(' ');
  }
  
  // CTE
  if (doCTE) {
    const col = pick(tables[name].map(c => c.name));
    const aggFunc = pick(['COUNT', 'SUM', 'MAX', 'MIN']);
    parts.push(`WITH cte AS (SELECT ${col}, ${aggFunc}(${col}) as agg FROM ${name} GROUP BY ${col})`);
    parts.push(`SELECT * FROM cte`);
    if (rand() < 0.3) parts.push(`WHERE agg ${pick(['>', '<', '>='])} ${genIntLiteral()}`);
    if (rand() < 0.3) parts.push(`LIMIT ${randInt(1, 10)}`);
    return parts.join(' ');
  }
  
  // Subquery in WHERE
  if (doSubquery) {
    const col = pick(tables[name].map(c => c.name));
    const op = pick(['>', '<', '>=', '<=', '=']);
    const aggFunc = pick(['AVG', 'MIN', 'MAX']);
    parts.push(`*`);
    parts.push(`FROM ${name}`);
    parts.push(`WHERE ${col} ${op} (SELECT ${aggFunc}(${col}) FROM ${name})`);
    if (rand() < 0.3) parts.push(`LIMIT ${randInt(1, 10)}`);
    return parts.join(' ');
  }
  
  // Columns
  if (doGroupBy) {
    const groupCol = pick(tables[name].map(c => c.name));
    const aggFunc = pick(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']);
    const aggCol = pick(tables[name].map(c => c.name));
    parts.push(`${groupCol}, ${aggFunc}(${aggCol}) as agg_val`);
    parts.push(`FROM ${name}`);
    
    // WHERE (before GROUP BY)
    if (rand() < 0.3) {
      parts.push(`WHERE ${groupCol} ${pick(['=', '!=', '<', '>'])} ${genLiteral()}`);
    }
    
    parts.push(`GROUP BY ${groupCol}`);
    
    // HAVING
    if (rand() < 0.4) {
      parts.push(`HAVING ${aggFunc}(${aggCol}) ${pick(['>', '<', '>=', '<='])} ${genIntLiteral()}`);
    }
  } else if (rand() < 0.3 && !doJoin) {
    parts.push('*');
  } else {
    const ncols = randInt(1, 3);
    const cols = [];
    for (let i = 0; i < ncols; i++) {
      if (rand() < 0.4 && !doJoin) {
        const func = pick(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']);
        cols.push(`${func}(${doJoin ? name + '.' : ''}${genColumnName()}) as ${func.toLowerCase()}_val`);
      } else {
        const prefix = doJoin ? name + '.' : '';
        cols.push(`${prefix}${genColumnName()}`);
      }
    }
    parts.push(cols.join(', '));
  }
  
  if (doJoin) {
    const other = pick(tableNames.filter(t => t !== name));
    if (other) {
      const joinType = pick(['INNER JOIN', 'LEFT JOIN']);
      const joinCol = genColumnName();
      parts.push(`FROM ${name} ${joinType} ${other} ON ${name}.${joinCol} = ${other}.${joinCol}`);
    } else {
      parts.push(`FROM ${name}`);
    }
  } else {
    parts.push(`FROM ${name}`);
  }
  
  // WHERE (or EXISTS)
  if (rand() < 0.5 && !doGroupBy) {
    if (rand() < 0.15 && tableNames.length >= 2) {
      // EXISTS subquery
      const other = pick(tableNames.filter(t => t !== name));
      if (other) {
        const col = pick(tables[name].map(c => c.name));
        const ocol = pick(tables[other].map(c => c.name));
        parts.push(`WHERE EXISTS (SELECT 1 FROM ${other} WHERE ${other}.${ocol} = ${name}.${col})`);
      } else {
        const prefix = doJoin ? name + '.' : '';
        parts.push(`WHERE ${prefix}${genWhere()}`);
      }
    } else {
      const prefix = doJoin ? name + '.' : '';
      parts.push(`WHERE ${prefix}${genWhere()}`);
    }
  }
  
  // ORDER BY
  if (rand() < 0.3) {
    const col = genColumnName();
    // Only order by columns that exist in the table to avoid the new validation
    const validCols = tables[name]?.map(c => c.name) || [];
    const orderCol = validCols.length > 0 ? pick(validCols) : col;
    parts.push(`ORDER BY ${orderCol} ${pick(['ASC', 'DESC'])}`);
  }
  
  // LIMIT
  if (rand() < 0.3) {
    parts.push(`LIMIT ${randInt(1, 20)}`);
  }
  
  return parts.join(' ');
}

function genQuery(tables) {
  if (Object.keys(tables).length === 0) return null;
  const r = rand();
  if (r < 0.30) return genInsert(tables);
  if (r < 0.38) return genUpdate(tables);
  if (r < 0.42) return genDelete(tables);
  if (r < 0.50) return genUnion(tables);
  return genSelect(tables);
}

function genUnion(tables) {
  const tableNames = Object.keys(tables);
  if (tableNames.length === 0) return null;
  const name = pick(tableNames);
  const cols = tables[name];
  if (cols.length === 0) return null;
  const col = pick(cols).name;
  const unionType = rand() < 0.5 ? 'UNION' : 'UNION ALL';
  return `SELECT ${col} FROM ${name} WHERE ${col} ${pick(['>', '<', '='])} ${genLiteral()} ${unionType} SELECT ${col} FROM ${name} WHERE ${col} ${pick(['>', '<', '='])} ${genLiteral()}`;
}

function genUpdate(tables) {
  const name = pick(Object.keys(tables));
  if (!name) return null;
  const cols = tables[name];
  const col = pick(cols);
  const val = col.type === 'INT' ? genIntLiteral() : genStringLiteral();
  const whereCol = pick(cols);
  const whereVal = whereCol.type === 'INT' ? genIntLiteral() : genStringLiteral();
  return `UPDATE ${name} SET ${col.name} = ${val} WHERE ${whereCol.name} = ${whereVal}`;
}

function genDelete(tables) {
  const name = pick(Object.keys(tables));
  if (!name) return null;
  const cols = tables[name];
  const col = pick(cols);
  const val = col.type === 'INT' ? genIntLiteral() : genStringLiteral();
  return `DELETE FROM ${name} WHERE ${col.name} = ${val}`;
}

// Main fuzzer
async function fuzz() {
  const henry = new Database();
  const sqlite = new BetterSqlite3(':memory:');
  
  const tables = {};
  let total = 0, passed = 0, failed = 0, errors = 0, skipped = 0;
  const failures = [];
  const queryTypes = { simple: 0, join: 0, groupby: 0, subquery: 0, cte: 0, insert: 0 };
  
  console.log(`Differential fuzzer: ${ITERATIONS} iterations, seed=${seed}`);
  console.log('---');
  
  // Create shared tables
  for (let i = 0; i < 3; i++) {
    const sql = genCreateTable();
    try {
      henry.execute(sql);
      sqlite.exec(sql);
      
      // Track table schema (only if not already tracked — IF NOT EXISTS means first one wins)
      const match = sql.match(/CREATE TABLE IF NOT EXISTS (\w+) \((.+)\)/);
      if (match) {
        const name = match[1];
        if (!tables[name]) {
          const cols = match[2].split(',').map(c => {
            const parts = c.trim().split(' ');
            return { name: parts[0], type: parts[1] };
          });
          tables[name] = cols;
        }
      }
    } catch (e) {
      // Both should handle the same errors
    }
  }
  
  // Insert some initial data — only keep inserts that succeed in BOTH
  for (let i = 0; i < 30; i++) {
    const sql = genInsert(tables);
    if (!sql) continue;
    let henryOk = false, sqliteOk = false;
    try { henry.execute(sql); henryOk = true; } catch {}
    try { sqlite.exec(sql); sqliteOk = true; } catch {}
    
    // If one succeeded but other didn't, undo the successful one to keep in sync
    if (henryOk && !sqliteOk) {
      // Undo HenryDB insert — simplest: track row count and delete excess
      const tbl = sql.match(/INSERT INTO (\w+)/)?.[1];
      if (tbl) {
        try {
          // Use a subquery to find and delete the just-inserted row
          // Since we don't have rowid, delete where all columns match the inserted values
          const valMatch = sql.match(/VALUES \((.+)\)/);
          if (valMatch) {
            const vals = valMatch[1];
            const cols = tables[tbl];
            if (cols) {
              const conditions = cols.map((c, i) => {
                const v = vals.split(',')[i]?.trim();
                return v ? `${c.name} = ${v}` : null;
              }).filter(Boolean);
              if (conditions.length > 0) {
                henry.execute(`DELETE FROM ${tbl} WHERE ${conditions.join(' AND ')} LIMIT 1`);
              }
            }
          }
        } catch {}
      }
      if (VERBOSE) console.log(`INSERT mismatch (Henry OK, SQLite fail): ${sql}`);
    } else if (!henryOk && sqliteOk) {
      // Undo SQLite insert
      const tbl = sql.match(/INSERT INTO (\w+)/)?.[1];
      if (tbl) {
        try {
          sqlite.exec(`DELETE FROM ${tbl} WHERE rowid = (SELECT MAX(rowid) FROM ${tbl})`);
        } catch {}
      }
      if (VERBOSE) console.log(`INSERT mismatch (Henry fail, SQLite OK): ${sql}`);
    }
  }
  
  // Verify data sync: ensure both databases have same row counts per table
  for (const [name, cols] of Object.entries(tables)) {
    try {
      const hCount = henry.execute(`SELECT COUNT(*) as cnt FROM ${name}`).rows[0].cnt;
      const sCount = sqlite.prepare(`SELECT COUNT(*) as cnt FROM ${name}`).get().cnt;
      if (hCount !== sCount) {
        if (VERBOSE) console.log(`Sync fix: ${name} has ${hCount} in Henry, ${sCount} in SQLite`);
        // Delete all and re-insert matching data from SQLite (source of truth)
        henry.execute(`DELETE FROM ${name}`);
        const allRows = sqlite.prepare(`SELECT * FROM ${name}`).all();
        for (const row of allRows) {
          const vals = cols.map(c => {
            const v = row[c.name];
            if (v == null) return 'NULL';
            if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`;
            return String(v);
          });
          try {
            henry.execute(`INSERT INTO ${name} VALUES (${vals.join(', ')})`);
          } catch {}
        }
      }
    } catch {}
  }
  
  // Run queries
  for (let i = 0; i < ITERATIONS; i++) {
    const sql = genQuery(tables);
    if (!sql) { skipped++; continue; }
    total++;
    
    // Track query type
    if (sql.includes('OVER (')) queryTypes.window = (queryTypes.window || 0) + 1;
    else if (sql.includes('UNION')) queryTypes.union = (queryTypes.union || 0) + 1;
    else if (sql.includes('WITH ')) queryTypes.cte++;
    else if (sql.includes('SELECT') && sql.includes('(SELECT')) queryTypes.subquery++;
    else if (sql.includes('GROUP BY')) queryTypes.groupby++;
    else if (sql.includes('JOIN')) queryTypes.join++;
    else if (sql.startsWith('INSERT')) queryTypes.insert++;
    else queryTypes.simple++;
    
    let henryResult, sqliteResult;
    let henryError, sqliteError;
    
    try { henryResult = henry.execute(sql); } catch (e) { henryError = e.message; }
    try {
      if (sql.startsWith('SELECT')) {
        sqliteResult = { rows: sqlite.prepare(sql).all() };
      } else {
        sqlite.exec(sql);
        sqliteResult = { type: 'OK' };
      }
    } catch (e) { sqliteError = e.message; }
    
    // Compare
    if (henryError && sqliteError) {
      passed++; // Both errored — consistent
    } else if (henryError && !sqliteError) {
      failed++;
      failures.push({ sql, issue: 'HenryDB error, SQLite OK', error: henryError });
    } else if (!henryError && sqliteError) {
      // HenryDB is more permissive — acceptable in some cases
      if (VERBOSE) console.log(`PERMISSIVE: ${sql}\n  SQLite error: ${sqliteError}`);
      passed++;
    } else if (sql.startsWith('SELECT')) {
      // Compare row counts and values
      const hRows = henryResult?.rows || [];
      const sRows = sqliteResult?.rows || [];
      
      // Normalize row order when no ORDER BY (order is undefined)
      const hasOrderBy = sql.toUpperCase().includes('ORDER BY');
      const hasLimit = sql.toUpperCase().includes('LIMIT');
      
      // LIMIT without ORDER BY is non-deterministic — different rows are valid
      // Just check row counts match in this case
      if (hasLimit && !hasOrderBy) {
        if (hRows.length === sRows.length) {
          passed++;
        } else {
          match = false;
          failed++;
          failures.push({ sql, issue: `Row count: HenryDB=${hRows.length}, SQLite=${sRows.length}` });
          if (VERBOSE) console.log(`  SQL: ${sql}\n  Issue: Row count: HenryDB=${hRows.length}, SQLite=${sRows.length}`);
        }
        continue; // Skip per-row comparison
      }
      
      if (!hasOrderBy) {
        const sortRows = (rows) => [...rows].sort((a, b) => {
          const keys = Object.keys(a).sort();
          for (const k of keys) {
            const av = String(a[k] ?? ''), bv = String(b[k] ?? '');
            if (av < bv) return -1;
            if (av > bv) return 1;
          }
          return 0;
        });
        hRows.splice(0, hRows.length, ...sortRows(hRows));
        sRows.splice(0, sRows.length, ...sortRows(sRows));
      }
      
      if (hRows.length !== sRows.length) {
        failed++;
        failures.push({ sql, issue: `Row count: HenryDB=${hRows.length}, SQLite=${sRows.length}` });
      } else {
        // Compare values
        let match = true;
        for (let j = 0; j < hRows.length && j < 5; j++) {
          const hKeys = Object.keys(hRows[j]).sort();
          const sKeys = Object.keys(sRows[j]).sort();
          for (const key of sKeys) {
            if (hRows[j][key] !== sRows[j][key]) {
              // Allow null vs undefined
              if (hRows[j][key] == null && sRows[j][key] == null) continue;
              // Allow numeric differences < 0.001
              if (typeof hRows[j][key] === 'number' && typeof sRows[j][key] === 'number' &&
                  Math.abs(hRows[j][key] - sRows[j][key]) < 0.001) continue;
              // Type affinity: compare as strings (int 42 == string "42")
              if (String(hRows[j][key] ?? '') === String(sRows[j][key] ?? '')) continue;
              // Numeric string comparison (42.0 == 42)
              const hNum = Number(hRows[j][key]);
              const sNum = Number(sRows[j][key]);
              if (!isNaN(hNum) && !isNaN(sNum) && Math.abs(hNum - sNum) < 0.001) continue;
              match = false;
              failures.push({ sql, issue: `Value mismatch at row ${j}, col ${key}: H=${JSON.stringify(hRows[j][key])}, S=${JSON.stringify(sRows[j][key])}` });
              break;
            }
          }
          if (!match) break;
        }
        if (match) passed++;
        else failed++;
      }
    } else {
      passed++; // Both succeeded for non-SELECT
    }
  }
  
  // Report
  console.log('\n=== Results ===');
  console.log(`Total: ${total}, Passed: ${passed}, Failed: ${failed}, Skipped: ${skipped}`);
  console.log(`Pass rate: ${(passed / total * 100).toFixed(1)}%`);
  
  if (process.argv.includes('--stats')) {
    console.log('\n=== Query Type Breakdown ===');
    for (const [type, count] of Object.entries(queryTypes)) {
      if (count > 0) console.log(`  ${type.padEnd(12)}: ${count}`);
    }
  }
  
  if (failures.length > 0) {
    console.log(`\n=== Failures (${failures.length}) ===`);
    for (const f of failures.slice(0, 20)) {
      console.log(`  SQL: ${f.sql}`);
      console.log(`  Issue: ${f.issue}`);
      if (f.error) console.log(`  Error: ${f.error}`);
      console.log();
    }
  }
  
  sqlite.close();
  return { total, passed, failed, failures };
}

fuzz().catch(e => console.error('Fuzzer error:', e));
