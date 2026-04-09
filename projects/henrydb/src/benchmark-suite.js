// benchmark-suite.js — HenryDB performance benchmarks
// Run with: node src/benchmark-suite.js
import { Database } from './db.js';

function bench(name, fn, iterations = 1) {
  const times = [];
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    const result = fn();
    const t1 = performance.now();
    times.push(t1 - t0);
  }
  const avg = times.reduce((a, b) => a + b, 0) / times.length;
  const min = Math.min(...times);
  const max = Math.max(...times);
  return { name, avg: avg.toFixed(2), min: min.toFixed(2), max: max.toFixed(2), iterations };
}

function runBenchmarks() {
  console.log('🐘 HenryDB Performance Benchmarks');
  console.log('='.repeat(60));
  console.log();
  
  const results = [];
  
  // --- Setup ---
  const db = new Database();
  
  // 1. Bulk INSERT
  db.execute('CREATE TABLE bench_insert (id INTEGER PRIMARY KEY, name TEXT, value REAL)');
  db.execute('CREATE INDEX idx_bench_val ON bench_insert(value)');
  
  const r1 = bench('INSERT 10,000 rows', () => {
    const d = new Database();
    d.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)');
    for (let i = 0; i < 10000; i++) {
      d.execute(`INSERT INTO t (id, name, value) VALUES (${i}, 'row_${i}', ${Math.random() * 1000})`);
    }
    return d;
  }, 3);
  results.push(r1);
  
  // Setup for subsequent benchmarks
  const benchDb = new Database();
  benchDb.execute('CREATE TABLE data (id INTEGER PRIMARY KEY, name TEXT, category TEXT, value REAL)');
  benchDb.execute('CREATE INDEX idx_data_category ON data(category)');
  benchDb.execute('CREATE INDEX idx_data_value ON data(value)');
  const categories = ['A', 'B', 'C', 'D', 'E'];
  for (let i = 0; i < 10000; i++) {
    const cat = categories[i % 5];
    benchDb.execute(`INSERT INTO data (id, name, category, value) VALUES (${i}, 'item_${i}', '${cat}', ${(Math.random() * 10000).toFixed(2)})`);
  }
  
  // 2. Point query (indexed)
  const r2 = bench('Point SELECT (indexed, 10K rows)', () => {
    for (let i = 0; i < 100; i++) {
      benchDb.execute(`SELECT * FROM data WHERE id = ${Math.floor(Math.random() * 10000)}`);
    }
  }, 5);
  r2.note = '100 random lookups per iteration';
  results.push(r2);
  
  // 3. Range scan
  const r3 = bench('Range scan (value BETWEEN)', () => {
    benchDb.execute('SELECT * FROM data WHERE value BETWEEN 1000 AND 2000');
  }, 5);
  results.push(r3);
  
  // 4. Full table scan with filter
  const r4 = bench('Full scan + filter (10K rows)', () => {
    benchDb.execute("SELECT * FROM data WHERE name LIKE 'item_99%'");
  }, 5);
  results.push(r4);
  
  // 5. GROUP BY + aggregates
  const r5 = bench('GROUP BY + 5 aggregates (10K rows)', () => {
    benchDb.execute(`
      SELECT category, COUNT(*) as cnt, SUM(value) as total,
             AVG(value) as avg_val, MIN(value) as min_val, MAX(value) as max_val
      FROM data
      GROUP BY category
    `);
  }, 5);
  results.push(r5);
  
  // 6. ORDER BY + LIMIT
  const r6 = bench('ORDER BY + LIMIT (10K rows)', () => {
    benchDb.execute('SELECT * FROM data ORDER BY value DESC LIMIT 100');
  }, 5);
  results.push(r6);
  
  // 7. Self-JOIN
  const r7 = bench('Self-JOIN (100x100 = 10K combos)', () => {
    const d = new Database();
    d.execute('CREATE TABLE small (id INTEGER PRIMARY KEY, grp TEXT)');
    for (let i = 0; i < 100; i++) d.execute(`INSERT INTO small VALUES (${i}, '${i % 5}')`);
    d.execute('SELECT a.id, b.id FROM small a JOIN small b ON a.grp = b.grp LIMIT 1000');
  }, 3);
  results.push(r7);
  
  // 8. Window functions
  const r8 = bench('Window: ROW_NUMBER (1K rows)', () => {
    const d = new Database();
    d.execute('CREATE TABLE win (id INTEGER PRIMARY KEY, grp TEXT, val REAL)');
    for (let i = 0; i < 1000; i++) d.execute(`INSERT INTO win VALUES (${i}, '${i % 10}', ${i})`);
    d.execute(`
      SELECT grp, val,
        ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) as rn
      FROM win
    `);
  }, 3);
  results.push(r8);
  
  // 9. CTE
  const r9 = bench('CTE + aggregate (10K rows)', () => {
    benchDb.execute(`
      WITH stats AS (
        SELECT category, SUM(value) as total
        FROM data
        GROUP BY category
      )
      SELECT category, total FROM stats ORDER BY total DESC
    `);
  }, 5);
  results.push(r9);
  
  // 10. Subquery
  const r10 = bench('Correlated subquery (100 rows)', () => {
    benchDb.execute(`
      SELECT d.id, d.name,
        (SELECT AVG(d2.value) FROM data d2 WHERE d2.category = d.category) as cat_avg
      FROM data d
      WHERE d.id < 100
    `);
  }, 3);
  results.push(r10);
  
  // 11. UPDATE
  const r11 = bench('UPDATE 1000 rows', () => {
    const d = new Database();
    d.execute('CREATE TABLE upd (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 0; i < 1000; i++) d.execute(`INSERT INTO upd VALUES (${i}, 0)`);
    for (let i = 0; i < 1000; i++) d.execute(`UPDATE upd SET val = val + 1 WHERE id = ${i}`);
  }, 3);
  results.push(r11);
  
  // 12. DELETE
  const r12 = bench('DELETE 500 of 1000 rows', () => {
    const d = new Database();
    d.execute('CREATE TABLE del (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 0; i < 1000; i++) d.execute(`INSERT INTO del VALUES (${i}, ${i})`);
    d.execute('DELETE FROM del WHERE val < 500');
  }, 3);
  results.push(r12);
  
  // 13. Transaction throughput
  const r13 = bench('100 transactions (BEGIN/INSERT/COMMIT)', () => {
    const d = new Database();
    d.execute('CREATE TABLE txn (id INTEGER PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 100; i++) {
      d.execute('BEGIN');
      d.execute(`INSERT INTO txn VALUES (${i}, 'tx_${i}')`);
      d.execute('COMMIT');
    }
  }, 3);
  results.push(r13);
  
  // 14. CREATE TABLE AS (materialization)
  const r14 = bench('CREATE TABLE AS (aggregate 10K rows)', () => {
    const d = new Database();
    d.execute('CREATE TABLE src (id INTEGER PRIMARY KEY, grp TEXT, val REAL)');
    for (let i = 0; i < 1000; i++) d.execute(`INSERT INTO src VALUES (${i}, '${i%10}', ${i*1.5})`);
    d.execute('CREATE TABLE report AS SELECT grp, SUM(val) as total, AVG(val) as avg FROM src GROUP BY grp');
  }, 3);
  results.push(r14);
  
  // Print results
  console.log('Benchmark Results:');
  console.log('-'.repeat(60));
  console.log(`${'Benchmark'.padEnd(45)} ${'Avg (ms)'.padStart(10)}`);
  console.log('-'.repeat(60));
  for (const r of results) {
    console.log(`${r.name.padEnd(45)} ${r.avg.padStart(10)}`);
    if (r.note) console.log(`  ${r.note}`);
  }
  console.log('-'.repeat(60));
  
  // Summary
  const insertRate = (10000 / parseFloat(r1.avg) * 1000).toFixed(0);
  const pointQueryRate = (100 / parseFloat(r2.avg) * 1000).toFixed(0);
  console.log(`\nKey metrics:`);
  console.log(`  Insert throughput: ~${insertRate} rows/sec`);
  console.log(`  Point query throughput: ~${pointQueryRate} queries/sec`);
  console.log(`  Dataset: 10,000 rows with indexes`);
  
  return results;
}

runBenchmarks();
