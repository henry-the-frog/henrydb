// diff-fuzzer-sqlite.test.js — Differential Fuzzer: HenryDB vs SQLite
// Generates random SQL queries, runs them against both HenryDB and SQLite,
// and compares results. Any divergence is a bug (in one or the other).

import { describe, it, test } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { execSync } from 'child_process';

function sqlite(sql) {
  try {
    const result = execSync(`sqlite3 :memory: "${sql.replace(/"/g, '\\"')}"`, {
      encoding: 'utf-8',
      timeout: 5000,
    }).trim();
    return result;
  } catch (e) {
    return `ERROR: ${e.stderr?.trim() || e.message}`;
  }
}

function henrydb(db, sql) {
  try {
    const r = db.execute(sql);
    if (!r.rows || r.rows.length === 0) return '';
    // Format like SQLite: values separated by | per row
    return r.rows.map(row => Object.values(row).map(v => v === null ? '' : v).join('|')).join('\n');
  } catch (e) {
    return `ERROR: ${e.message}`;
  }
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function choice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

const SETUP = [
  'CREATE TABLE t (id INTEGER, name TEXT, val REAL, flag INTEGER)',
  "INSERT INTO t VALUES (1, 'alice', 1.5, 1)",
  "INSERT INTO t VALUES (2, 'bob', 2.7, 0)",
  "INSERT INTO t VALUES (3, 'carol', 3.14, 1)",
  "INSERT INTO t VALUES (4, 'dave', NULL, 0)",
  "INSERT INTO t VALUES (5, 'eve', 5.0, 1)",
  "INSERT INTO t VALUES (NULL, NULL, NULL, NULL)",
];

function setupHenryDB() {
  const db = new Database();
  for (const sql of SETUP) db.execute(sql);
  return db;
}

function setupSQLite() {
  return SETUP.map(s => s.replace(/"/g, '\\"')).join('; ');
}

describe('Differential Fuzzer: HenryDB vs SQLite', () => {
  const db = setupHenryDB();
  const sqliteSetup = setupSQLite();

  function runSqlite(query) {
    return sqlite(`${sqliteSetup}; ${query}`);
  }

  // Deterministic queries for comparison
  const queries = [
    'SELECT COUNT(*) FROM t',
    'SELECT SUM(val) FROM t',
    'SELECT AVG(val) FROM t WHERE val IS NOT NULL',
    'SELECT MIN(val), MAX(val) FROM t',
    "SELECT name FROM t WHERE val > 2 ORDER BY name",
    'SELECT flag, COUNT(*) FROM t WHERE flag IS NOT NULL GROUP BY flag ORDER BY flag',
    "SELECT name FROM t WHERE name LIKE 'a%'",
    'SELECT id, val FROM t WHERE val BETWEEN 2 AND 4 ORDER BY id',
    "SELECT COALESCE(name, 'unknown') FROM t WHERE id IS NULL",
    'SELECT id + 10 FROM t WHERE id IS NOT NULL ORDER BY id',
    'SELECT CASE WHEN flag = 1 THEN name ELSE NULL END FROM t WHERE id IS NOT NULL ORDER BY id',
    'SELECT UPPER(name) FROM t WHERE name IS NOT NULL ORDER BY name',
    'SELECT LENGTH(name) FROM t WHERE name IS NOT NULL ORDER BY name',
    "SELECT REPLACE(name, 'a', 'X') FROM t WHERE name IS NOT NULL ORDER BY name",
    'SELECT ABS(-5)',
    'SELECT 10 / 3',
    'SELECT 10 % 3',
    'SELECT TYPEOF(1) AS t1, TYPEOF(1.5) AS t2, TYPEOF(NULL) AS t3',
    'SELECT NULLIF(1, 1) AS n1, NULLIF(1, 2) AS n2',
    'SELECT IIF(1 > 0, 10, 20)',
  ];

  for (const sql of queries) {
    test(`${sql.slice(0, 60)}`, () => {
      const hResult = henrydb(db, sql);
      const sResult = runSqlite(sql);

      // Normalize: trim whitespace, handle float precision
      const hNorm = hResult.split('\n').map(s => s.trim()).filter(Boolean);
      const sNorm = sResult.split('\n').map(s => s.trim()).filter(Boolean);

      // Compare row count
      if (hNorm.length !== sNorm.length) {
        // Check if it's just precision differences
        assert.equal(hNorm.length, sNorm.length,
          `Row count mismatch:\n  HenryDB (${hNorm.length}): ${hNorm.join(', ')}\n  SQLite  (${sNorm.length}): ${sNorm.join(', ')}`);
      }

      // Compare values (allow float tolerance)
      for (let i = 0; i < hNorm.length; i++) {
        const hVals = hNorm[i].split('|');
        const sVals = sNorm[i].split('|');
        
        for (let j = 0; j < Math.max(hVals.length, sVals.length); j++) {
          const h = (hVals[j] || '').trim();
          const s = (sVals[j] || '').trim();
          
          // Try numeric comparison with tolerance
          const hNum = parseFloat(h);
          const sNum = parseFloat(s);
          if (!isNaN(hNum) && !isNaN(sNum)) {
            assert.ok(Math.abs(hNum - sNum) < 0.01,
              `Numeric mismatch in '${sql}': HenryDB=${h}, SQLite=${s}`);
          } else {
            // Case-insensitive string comparison
            assert.equal(h.toLowerCase(), s.toLowerCase(),
              `Value mismatch in '${sql}' row ${i}: HenryDB='${h}', SQLite='${s}'`);
          }
        }
      }
    });
  }
});
