// sqlite-comparison.test.js — Benchmark HenryDB vs SQLite on identical queries
// Shows where a hand-built database competes with, beats, or loses to SQLite

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const BetterSqlite3 = require('better-sqlite3');

let henrydb;
let sqlite;

function rows(result) {
  if (Array.isArray(result)) return result;
  if (result && result.rows) return result.rows;
  return [];
}

function setupBoth(size = 10000) {
  henrydb = new Database();
  sqlite = new BetterSqlite3(':memory:');
  
  // Create identical schemas
  const createTable = 'CREATE TABLE data (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, category TEXT, value REAL)';
  henrydb.execute(createTable.replace('REAL', 'INT').replace('INTEGER PRIMARY KEY', 'INT'));
  sqlite.exec(createTable);
  
  // Insert identical data
  const categories = ['alpha', 'beta', 'gamma', 'delta', 'epsilon'];
  
  const sqliteInsert = sqlite.prepare('INSERT INTO data VALUES (?, ?, ?, ?, ?)');
  const insertMany = sqlite.transaction((rows) => {
    for (const row of rows) sqliteInsert.run(...row);
  });
  
  const rowData = [];
  for (let i = 1; i <= size; i++) {
    const a = i * 7 % 1000;
    const b = i * 13 % 500;
    const cat = categories[(i - 1) % 5];
    const val = Math.round(i * 3.14 * 100) / 100;
    rowData.push([i, a, b, cat, val]);
    henrydb.execute(`INSERT INTO data VALUES (${i}, ${a}, ${b}, '${cat}', ${Math.round(val)})`);
  }
  insertMany(rowData);
  
  henrydb.execute('ANALYZE data');
}

function teardownBoth() {
  henrydb = null;
  if (sqlite) { sqlite.close(); sqlite = null; }
}

function bench(label, fn, iterations = 10) {
  fn(); fn(); // Warm up
  const start = process.hrtime.bigint();
  let result;
  for (let i = 0; i < iterations; i++) result = fn();
  const elapsed = Number(process.hrtime.bigint() - start) / 1e6;
  return { label, totalMs: elapsed, avgMs: elapsed / iterations, result };
}

describe('HenryDB vs SQLite: 10K rows', () => {
  beforeEach(() => setupBoth(10000));
  afterEach(teardownBoth);

  it('COUNT(*) — full table count', () => {
    const iter = 20;
    
    const h = bench('HenryDB', () => {
      return rows(henrydb.execute('SELECT COUNT(*) AS c FROM data'))[0].c;
    }, iter);
    
    const s = bench('SQLite', () => {
      return sqlite.prepare('SELECT COUNT(*) AS c FROM data').get().c;
    }, iter);
    
    assert.equal(h.result, 10000);
    assert.equal(s.result, 10000);
    
    const ratio = h.avgMs / s.avgMs;
    console.log(`    COUNT(*) 10K rows (${iter} iter):`);
    console.log(`      HenryDB: ${h.avgMs.toFixed(3)}ms`);
    console.log(`      SQLite:  ${s.avgMs.toFixed(3)}ms`);
    console.log(`      Ratio:   ${ratio.toFixed(1)}x (${ratio > 1 ? 'SQLite faster' : 'HenryDB faster'})`);
  });

  it('Selective filter (10% selectivity)', () => {
    const iter = 20;
    
    const h = bench('HenryDB', () => {
      return rows(henrydb.execute('SELECT * FROM data WHERE a > 900'));
    }, iter);
    
    const s = bench('SQLite', () => {
      return sqlite.prepare('SELECT * FROM data WHERE a > 900').all();
    }, iter);
    
    assert.equal(h.result.length, s.result.length);
    
    const ratio = h.avgMs / s.avgMs;
    console.log(`    Filter 10% (${iter} iter):`);
    console.log(`      HenryDB: ${h.avgMs.toFixed(3)}ms (${h.result.length} rows)`);
    console.log(`      SQLite:  ${s.avgMs.toFixed(3)}ms`);
    console.log(`      Ratio:   ${ratio.toFixed(1)}x`);
  });

  it('GROUP BY with aggregation', () => {
    const iter = 20;
    
    const h = bench('HenryDB', () => {
      return rows(henrydb.execute('SELECT category, COUNT(*) AS cnt, SUM(a) AS total FROM data GROUP BY category'));
    }, iter);
    
    const s = bench('SQLite', () => {
      return sqlite.prepare('SELECT category, COUNT(*) AS cnt, SUM(a) AS total FROM data GROUP BY category').all();
    }, iter);
    
    assert.equal(h.result.length, 5);
    assert.equal(s.result.length, 5);
    
    const ratio = h.avgMs / s.avgMs;
    console.log(`    GROUP BY + agg (${iter} iter):`);
    console.log(`      HenryDB: ${h.avgMs.toFixed(3)}ms`);
    console.log(`      SQLite:  ${s.avgMs.toFixed(3)}ms`);
    console.log(`      Ratio:   ${ratio.toFixed(1)}x`);
  });

  it('ORDER BY + LIMIT (top-N)', () => {
    const iter = 20;
    
    const h = bench('HenryDB', () => {
      return rows(henrydb.execute('SELECT * FROM data ORDER BY a DESC LIMIT 10'));
    }, iter);
    
    const s = bench('SQLite', () => {
      return sqlite.prepare('SELECT * FROM data ORDER BY a DESC LIMIT 10').all();
    }, iter);
    
    assert.equal(h.result.length, 10);
    assert.equal(s.result.length, 10);
    
    const ratio = h.avgMs / s.avgMs;
    console.log(`    ORDER BY + LIMIT 10 (${iter} iter):`);
    console.log(`      HenryDB: ${h.avgMs.toFixed(3)}ms`);
    console.log(`      SQLite:  ${s.avgMs.toFixed(3)}ms`);
    console.log(`      Ratio:   ${ratio.toFixed(1)}x`);
  });

  it('String matching (LIKE equivalent via filter)', () => {
    const iter = 20;
    
    const h = bench('HenryDB', () => {
      return rows(henrydb.execute("SELECT COUNT(*) AS c FROM data WHERE category = 'alpha'"));
    }, iter);
    
    const s = bench('SQLite', () => {
      return sqlite.prepare("SELECT COUNT(*) AS c FROM data WHERE category = 'alpha'").get();
    }, iter);
    
    assert.equal(h.result[0].c, s.c);
    
    const ratio = h.avgMs / s.avgMs;
    console.log(`    String equality filter (${iter} iter):`);
    console.log(`      HenryDB: ${h.avgMs.toFixed(3)}ms`);
    console.log(`      SQLite:  ${s.avgMs.toFixed(3)}ms`);
    console.log(`      Ratio:   ${ratio.toFixed(1)}x`);
  });

  it('Complex: filter + group + order', () => {
    const iter = 10;
    
    const h = bench('HenryDB', () => {
      return rows(henrydb.execute(`
        SELECT category, COUNT(*) AS cnt, SUM(a) AS total
        FROM data 
        WHERE b > 250 
        GROUP BY category 
        ORDER BY total DESC
      `));
    }, iter);
    
    const s = bench('SQLite', () => {
      return sqlite.prepare(`
        SELECT category, COUNT(*) AS cnt, SUM(a) AS total
        FROM data 
        WHERE b > 250 
        GROUP BY category 
        ORDER BY total DESC
      `).all();
    }, iter);
    
    assert.equal(h.result.length, s.result.length);
    
    const ratio = h.avgMs / s.avgMs;
    console.log(`    Complex (filter+group+order) (${iter} iter):`);
    console.log(`      HenryDB: ${h.avgMs.toFixed(3)}ms`);
    console.log(`      SQLite:  ${s.avgMs.toFixed(3)}ms`);
    console.log(`      Ratio:   ${ratio.toFixed(1)}x`);
  });

  it('INSERT performance (1000 rows)', () => {
    const iter = 5;
    
    const h = bench('HenryDB', () => {
      const db = new Database();
      db.execute('CREATE TABLE bench (id INT, val INT)');
      for (let i = 0; i < 1000; i++) {
        db.execute(`INSERT INTO bench VALUES (${i}, ${i * 7})`);
      }
      return rows(db.execute('SELECT COUNT(*) AS c FROM bench'))[0].c;
    }, iter);
    
    const s = bench('SQLite', () => {
      const db = new BetterSqlite3(':memory:');
      db.exec('CREATE TABLE bench (id INTEGER, val INTEGER)');
      const stmt = db.prepare('INSERT INTO bench VALUES (?, ?)');
      for (let i = 0; i < 1000; i++) {
        stmt.run(i, i * 7);
      }
      const count = db.prepare('SELECT COUNT(*) AS c FROM bench').get().c;
      db.close();
      return count;
    }, iter);
    
    assert.equal(h.result, 1000);
    assert.equal(s.result, 1000);
    
    const ratio = h.avgMs / s.avgMs;
    console.log(`    INSERT 1000 rows (${iter} iter):`);
    console.log(`      HenryDB: ${h.avgMs.toFixed(1)}ms`);
    console.log(`      SQLite:  ${s.avgMs.toFixed(1)}ms`);
    console.log(`      Ratio:   ${ratio.toFixed(1)}x`);
  });
});
