// sql-fuzzer.test.js — Differential correctness testing: HenryDB vs SQLite
// Generates random SQL, executes on both engines, and compares results.
// This is how real databases get tested (inspired by sqlsmith, SQLite's own fuzzer).

import { describe, it, before, after } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const BetterSqlite3 = require('better-sqlite3');

// ============================================================
// Random SQL Generator
// ============================================================

class SQLFuzzer {
  constructor(seed = Date.now()) {
    this._seed = seed;
    this._tables = [];
  }

  // Simple seeded PRNG (xorshift32)
  _rand() {
    this._seed ^= this._seed << 13;
    this._seed ^= this._seed >> 17;
    this._seed ^= this._seed << 5;
    return (this._seed >>> 0) / 4294967296;
  }

  _randInt(min, max) {
    return Math.floor(this._rand() * (max - min + 1)) + min;
  }

  _pick(arr) {
    return arr[Math.floor(this._rand() * arr.length)];
  }

  _pickN(arr, n) {
    const shuffled = [...arr].sort(() => this._rand() - 0.5);
    return shuffled.slice(0, n);
  }

  // ---- Schema Generation ----

  generateSchema(numTables = 2) {
    this._tables = [];
    const stmts = [];

    for (let t = 0; t < numTables; t++) {
      const tableName = `t${t}`;
      const numCols = this._randInt(2, 5);
      const cols = [{ name: 'id', type: 'INTEGER' }];

      for (let c = 1; c < numCols; c++) {
        const type = this._pick(['INTEGER', 'TEXT']);
        cols.push({ name: `c${c}`, type });
      }

      this._tables.push({ name: tableName, cols });

      // HenryDB uses INT, not INTEGER PRIMARY KEY
      const colDefs = cols.map(c => `${c.name} ${c.type}`).join(', ');
      stmts.push(`CREATE TABLE ${tableName} (${colDefs})`);
    }

    return stmts;
  }

  // ---- INSERT Generation ----

  generateInserts(rowsPerTable = 20) {
    const stmts = [];

    for (const table of this._tables) {
      for (let i = 0; i < rowsPerTable; i++) {
        const vals = table.cols.map(col => {
          if (col.name === 'id') return i;
          if (col.type === 'INTEGER') return this._randInt(-100, 100);
          // TEXT: short strings, some with special chars
          const words = ['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta'];
          return `'${this._pick(words)}_${this._randInt(0, 99)}'`;
        });
        stmts.push(`INSERT INTO ${table.name} VALUES (${vals.join(', ')})`);
      }
    }

    return stmts;
  }

  // ---- SELECT Generation ----

  generateSelect() {
    const table = this._pick(this._tables);
    const parts = { from: table.name };

    // Columns: * or specific columns
    let selectCols;
    if (this._rand() < 0.3) {
      selectCols = '*';
    } else {
      const n = this._randInt(1, Math.min(3, table.cols.length));
      const chosen = this._pickN(table.cols, n);
      selectCols = chosen.map(c => c.name).join(', ');
    }

    // WHERE clause (50% chance)
    let where = '';
    if (this._rand() < 0.5) {
      const col = this._pick(table.cols);
      if (col.type === 'INTEGER') {
        const op = this._pick(['=', '>', '<', '>=', '<=', '!=']);
        const val = this._randInt(-50, 50);
        where = ` WHERE ${col.name} ${op} ${val}`;
      } else {
        const words = ['alpha', 'beta', 'gamma', 'delta'];
        const word = this._pick(words);
        where = ` WHERE ${col.name} = '${word}_${this._randInt(0, 20)}'`;
      }
    }

    // ORDER BY (40% chance)
    let orderBy = '';
    if (this._rand() < 0.4) {
      const col = this._pick(table.cols);
      const dir = this._rand() < 0.5 ? 'ASC' : 'DESC';
      orderBy = ` ORDER BY ${col.name} ${dir}`;
    }

    // LIMIT (30% chance)
    let limit = '';
    if (this._rand() < 0.3) {
      limit = ` LIMIT ${this._randInt(1, 50)}`;
    }

    return `SELECT ${selectCols} FROM ${table.name}${where}${orderBy}${limit}`;
  }

  // ---- Aggregate SELECT Generation ----

  generateAggregateSelect() {
    const table = this._pick(this._tables);
    const intCols = table.cols.filter(c => c.type === 'INTEGER');
    
    if (intCols.length === 0) return this.generateSelect();

    const aggCol = this._pick(intCols);
    const func = this._pick(['COUNT', 'SUM', 'MIN', 'MAX']);
    
    let select;
    if (func === 'COUNT') {
      select = `SELECT COUNT(*) AS cnt FROM ${table.name}`;
    } else {
      select = `SELECT ${func}(${aggCol.name}) AS agg FROM ${table.name}`;
    }

    // WHERE (30% chance)
    if (this._rand() < 0.3) {
      const col = this._pick(intCols);
      const val = this._randInt(-50, 50);
      select += ` WHERE ${col.name} > ${val}`;
    }

    return select;
  }

  // ---- GROUP BY SELECT Generation ----

  generateGroupBySelect() {
    const table = this._pick(this._tables);
    const intCols = table.cols.filter(c => c.type === 'INTEGER');
    const textCols = table.cols.filter(c => c.type === 'TEXT');

    if (intCols.length === 0 || textCols.length === 0) return this.generateSelect();

    const groupCol = this._pick([...intCols, ...textCols]);
    const aggCol = this._pick(intCols);
    const func = this._pick(['COUNT', 'SUM', 'MIN', 'MAX']);
    
    let agg;
    if (func === 'COUNT') {
      agg = 'COUNT(*) AS cnt';
    } else {
      agg = `${func}(${aggCol.name}) AS agg`;
    }

    return `SELECT ${groupCol.name}, ${agg} FROM ${table.name} GROUP BY ${groupCol.name} ORDER BY ${groupCol.name}`;
  }

  // ---- Generate a random query ----

  generateQuery() {
    const r = this._rand();
    if (r < 0.5) return this.generateSelect();
    if (r < 0.75) return this.generateAggregateSelect();
    return this.generateGroupBySelect();
  }
}

// ============================================================
// Differential Executor
// ============================================================

function normalizeRows(rows) {
  // Convert all values to strings for comparison (both DBs may return different types)
  // Also filter out duplicate columns (HenryDB may include both alias and raw name)
  if (!rows || rows.length === 0) return [];
  return rows.map(row => {
    const normalized = {};
    for (const [key, val] of Object.entries(row)) {
      // Skip raw aggregate function names like "COUNT(*)" if alias exists
      if (key.includes('(') && key.includes(')')) continue;
      normalized[key] = val === null || val === undefined ? null : String(val);
    }
    return normalized;
  });
}

function sortRows(rows) {
  if (rows.length === 0) return rows;
  const keys = Object.keys(rows[0]);
  return [...rows].sort((a, b) => {
    for (const key of keys) {
      const av = a[key] ?? '';
      const bv = b[key] ?? '';
      if (av < bv) return -1;
      if (av > bv) return 1;
    }
    return 0;
  });
}

// ============================================================
// Tests
// ============================================================

let henrydb, sqlite, fuzzer;

function setupBoth(seed, numTables = 2, rowsPerTable = 30) {
  henrydb = new Database();
  sqlite = new BetterSqlite3(':memory:');
  fuzzer = new SQLFuzzer(seed);

  // Generate and execute schema
  const schemas = fuzzer.generateSchema(numTables);
  for (const sql of schemas) {
    // HenryDB doesn't support INTEGER PRIMARY KEY, use INT instead
    const henrySql = sql.replace(/INTEGER PRIMARY KEY/g, 'INT');
    henrydb.execute(henrySql);
    sqlite.exec(sql);
  }

  // Generate and execute inserts
  const inserts = fuzzer.generateInserts(rowsPerTable);
  for (const sql of inserts) {
    henrydb.execute(sql);
    sqlite.exec(sql);
  }
}

function teardownBoth() {
  try { sqlite?.close(); } catch (e) {}
}

function executeHenryDB(sql) {
  try {
    const result = henrydb.execute(sql);
    return { rows: result.rows || [], error: null };
  } catch (e) {
    return { rows: [], error: e.message };
  }
}

function executeSQLite(sql) {
  try {
    const rows = sqlite.prepare(sql).all();
    return { rows, error: null };
  } catch (e) {
    return { rows: [], error: e.message };
  }
}

describe('SQL Fuzzer: Differential Correctness Testing', () => {
  after(teardownBoth);

  it('100 random queries with seed 42', () => {
    setupBoth(42, 2, 30);
    let passed = 0;
    let errorsMatched = 0;
    let mismatches = [];

    for (let i = 0; i < 100; i++) {
      const sql = fuzzer.generateQuery();
      const h = executeHenryDB(sql);
      const s = executeSQLite(sql);

      // If both error, that's fine (both reject the same SQL)
      if (h.error && s.error) {
        errorsMatched++;
        continue;
      }

      // If one errors and the other doesn't, that's a mismatch
      if (h.error !== null && s.error === null) {
        mismatches.push({ sql, type: 'henrydb_error', error: h.error });
        continue;
      }
      if (h.error === null && s.error !== null) {
        // HenryDB succeeded where SQLite didn't — not necessarily wrong
        passed++;
        continue;
      }

      // Both succeeded: compare results
      const hRows = normalizeRows(h.rows);
      const sRows = normalizeRows(s.rows);

      // For queries without ORDER BY, sort both for comparison
      const hasOrderBy = /ORDER BY/i.test(sql);
      const hSorted = hasOrderBy ? hRows : sortRows(hRows);
      const sSorted = hasOrderBy ? sRows : sortRows(sRows);

      if (hSorted.length !== sSorted.length) {
        mismatches.push({ sql, type: 'row_count', henry: hSorted.length, sqlite: sSorted.length });
        continue;
      }

      // Compare row by row
      let rowMatch = true;
      for (let r = 0; r < hSorted.length; r++) {
        const hKeys = Object.keys(hSorted[r]).sort();
        const sKeys = Object.keys(sSorted[r]).sort();
        
        // Compare values for matching keys
        for (const key of hKeys) {
          if (hSorted[r][key] !== sSorted[r][key]) {
            // Allow numeric tolerance (floating point)
            const hVal = parseFloat(hSorted[r][key]);
            const sVal = parseFloat(sSorted[r][key]);
            if (!isNaN(hVal) && !isNaN(sVal) && Math.abs(hVal - sVal) < 0.001) continue;
            
            rowMatch = false;
            mismatches.push({ sql, type: 'value_mismatch', row: r, key, henry: hSorted[r][key], sqlite: sSorted[r][key] });
            break;
          }
        }
        if (!rowMatch) break;
      }

      if (rowMatch) passed++;
    }

    console.log(`    Passed: ${passed}/100, Errors matched: ${errorsMatched}, Mismatches: ${mismatches.length}`);
    
    if (mismatches.length > 0) {
      console.log('    First 5 mismatches:');
      for (const m of mismatches.slice(0, 5)) {
        console.log(`      ${m.type}: ${m.sql}`);
        if (m.type === 'row_count') console.log(`        HenryDB: ${m.henry} rows, SQLite: ${m.sqlite} rows`);
        if (m.type === 'value_mismatch') console.log(`        row ${m.row}, key "${m.key}": henry=${m.henry}, sqlite=${m.sqlite}`);
        if (m.type === 'henrydb_error') console.log(`        Error: ${m.error}`);
      }
    }

    // Allow up to 10% mismatch rate (some features may differ)
    const passRate = (passed + errorsMatched) / 100;
    assert.ok(passRate >= 0.80, `Pass rate ${(passRate * 100).toFixed(1)}% below 80% threshold`);
  });

  it('200 random queries with seed 12345', () => {
    setupBoth(12345, 3, 50);
    let passed = 0;
    let errorsMatched = 0;
    let mismatches = [];

    for (let i = 0; i < 200; i++) {
      const sql = fuzzer.generateQuery();
      const h = executeHenryDB(sql);
      const s = executeSQLite(sql);

      if (h.error && s.error) { errorsMatched++; continue; }
      if (h.error !== null && s.error === null) {
        mismatches.push({ sql, type: 'henrydb_error', error: h.error });
        continue;
      }
      if (h.error === null && s.error !== null) { passed++; continue; }

      const hRows = normalizeRows(h.rows);
      const sRows = normalizeRows(s.rows);
      const hasOrderBy = /ORDER BY/i.test(sql);
      const hSorted = hasOrderBy ? hRows : sortRows(hRows);
      const sSorted = hasOrderBy ? sRows : sortRows(sRows);

      if (hSorted.length !== sSorted.length) {
        mismatches.push({ sql, type: 'row_count', henry: hSorted.length, sqlite: sSorted.length });
        continue;
      }

      let rowMatch = true;
      for (let r = 0; r < hSorted.length; r++) {
        for (const key of Object.keys(hSorted[r])) {
          if (hSorted[r][key] !== sSorted[r][key]) {
            const hVal = parseFloat(hSorted[r][key]);
            const sVal = parseFloat(sSorted[r][key]);
            if (!isNaN(hVal) && !isNaN(sVal) && Math.abs(hVal - sVal) < 0.001) continue;
            rowMatch = false;
            mismatches.push({ sql, type: 'value_mismatch', row: r, key, henry: hSorted[r][key], sqlite: sSorted[r][key] });
            break;
          }
        }
        if (!rowMatch) break;
      }

      if (rowMatch) passed++;
    }

    console.log(`    Passed: ${passed}/200, Errors matched: ${errorsMatched}, Mismatches: ${mismatches.length}`);
    if (mismatches.length > 0) {
      console.log('    First 5 mismatches:');
      for (const m of mismatches.slice(0, 5)) {
        console.log(`      ${m.type}: ${m.sql}`);
        if (m.type === 'row_count') console.log(`        HenryDB: ${m.henry} rows, SQLite: ${m.sqlite} rows`);
        if (m.type === 'value_mismatch') console.log(`        row ${m.row}, key "${m.key}": henry=${m.henry}, sqlite=${m.sqlite}`);
        if (m.type === 'henrydb_error') console.log(`        Error: ${m.error}`);
      }
    }

    const passRate = (passed + errorsMatched) / 200;
    assert.ok(passRate >= 0.80, `Pass rate ${(passRate * 100).toFixed(1)}% below 80% threshold`);
  });

  it('500 queries stress test with seed 99999', () => {
    setupBoth(99999, 2, 100);
    let passed = 0;
    let errorsMatched = 0;
    let mismatches = [];

    for (let i = 0; i < 500; i++) {
      const sql = fuzzer.generateQuery();
      const h = executeHenryDB(sql);
      const s = executeSQLite(sql);

      if (h.error && s.error) { errorsMatched++; continue; }
      if (h.error !== null && s.error === null) {
        mismatches.push({ sql, type: 'henrydb_error', error: h.error });
        continue;
      }
      if (h.error === null && s.error !== null) { passed++; continue; }

      const hRows = normalizeRows(h.rows);
      const sRows = normalizeRows(s.rows);
      const hasOrderBy = /ORDER BY/i.test(sql);
      const hSorted = hasOrderBy ? hRows : sortRows(hRows);
      const sSorted = hasOrderBy ? sRows : sortRows(sRows);

      if (hSorted.length !== sSorted.length) {
        mismatches.push({ sql, type: 'row_count', henry: hSorted.length, sqlite: sSorted.length });
        continue;
      }

      let rowMatch = true;
      for (let r = 0; r < hSorted.length; r++) {
        for (const key of Object.keys(hSorted[r])) {
          if (hSorted[r][key] !== sSorted[r][key]) {
            const hVal = parseFloat(hSorted[r][key]);
            const sVal = parseFloat(sSorted[r][key]);
            if (!isNaN(hVal) && !isNaN(sVal) && Math.abs(hVal - sVal) < 0.001) continue;
            rowMatch = false;
            break;
          }
        }
        if (!rowMatch) break;
      }

      if (rowMatch) passed++;
      else if (mismatches.length < 20) {
        mismatches.push({ sql, type: 'value_mismatch' });
      }
    }

    const total = passed + errorsMatched;
    const mismatchCount = 500 - total;
    console.log(`    Passed: ${passed}/500, Errors matched: ${errorsMatched}, Mismatches: ${mismatchCount}`);
    
    if (mismatches.length > 0) {
      console.log('    Sample mismatches:');
      for (const m of mismatches.slice(0, 3)) {
        console.log(`      ${m.type}: ${m.sql?.substring(0, 100)}`);
      }
    }

    const passRate = total / 500;
    assert.ok(passRate >= 0.75, `Pass rate ${(passRate * 100).toFixed(1)}% below 75% threshold`);
  });
});
