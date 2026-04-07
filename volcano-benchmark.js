#!/usr/bin/env node
// volcano-benchmark.js — Compare volcano vs standard execution engine
// Usage: node volcano-benchmark.js

import { Database } from './src/db.js';
import { buildPlan } from './src/volcano-planner.js';
import { parse } from './src/sql.js';

const ROWS = 5000;
const ITERATIONS = 20;

function bench(label, fn, iters = ITERATIONS) {
  // Warmup
  for (let i = 0; i < 3; i++) fn();
  // Measure
  const start = performance.now();
  for (let i = 0; i < iters; i++) fn();
  const elapsed = performance.now() - start;
  const opsPerSec = Math.round(iters / (elapsed / 1000));
  const avgMs = (elapsed / iters).toFixed(2);
  return { label, opsPerSec, avgMs, totalMs: elapsed.toFixed(0) };
}

// Setup database with 10K rows
console.log(`Setting up database with ${ROWS} rows...`);
const db = new Database();
db.execute('CREATE TABLE employees (id INT, name TEXT, age INT, dept TEXT, salary INT)');
db.execute('CREATE TABLE departments (id INT, name TEXT, budget INT)');

const depts = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'];
for (const [i, d] of depts.entries()) {
  db.execute(`INSERT INTO departments VALUES (${i + 1}, '${d}', ${(i + 1) * 100000})`);
}

for (let i = 0; i < ROWS; i++) {
  const age = 20 + (i % 40);
  const dept = depts[i % depts.length];
  const salary = 40000 + (i % 60) * 1000;
  db.execute(`INSERT INTO employees VALUES (${i}, 'emp_${i}', ${age}, '${dept}', ${salary})`);
}

console.log('Running benchmarks...\n');

const queries = [
  { name: 'Full scan', sql: 'SELECT * FROM employees' },
  { name: 'Filter (age > 50)', sql: 'SELECT * FROM employees WHERE age > 50' },
  { name: 'Project 2 cols', sql: 'SELECT name, salary FROM employees' },
  { name: 'ORDER BY', sql: 'SELECT name, salary FROM employees ORDER BY salary DESC' },
  { name: 'LIMIT 10', sql: 'SELECT name FROM employees LIMIT 10' },
  { name: 'ORDER BY + LIMIT 10', sql: 'SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10' },
  { name: 'COUNT(*)', sql: 'SELECT COUNT(*) as cnt FROM employees' },
  { name: 'GROUP BY + COUNT', sql: 'SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept' },
  { name: 'GROUP BY + SUM', sql: 'SELECT dept, SUM(salary) as total FROM employees GROUP BY dept' },
  { name: 'DISTINCT', sql: 'SELECT DISTINCT dept FROM employees' },
  { name: 'JOIN', sql: 'SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept = d.name' },
  { name: 'JOIN + filter', sql: "SELECT e.name, d.budget FROM employees e JOIN departments d ON e.dept = d.name WHERE e.age > 50" },
  { name: 'Complex pipeline', sql: 'SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal FROM employees WHERE age > 30 GROUP BY dept ORDER BY avg_sal DESC LIMIT 3' },
];

const results = [];

for (const { name, sql } of queries) {
  const ast = parse(sql);
  
  let stdResult, volResult;
  
  // Standard engine
  const std = bench(`[STD] ${name}`, () => {
    stdResult = db.execute(sql);
  });
  
  // Volcano engine
  const vol = bench(`[VOL] ${name}`, () => {
    const plan = buildPlan(ast, db.tables);
    volResult = plan.toArray();
  });

  const speedup = (std.opsPerSec / vol.opsPerSec).toFixed(2);
  const volWins = vol.opsPerSec > std.opsPerSec;
  
  results.push({
    query: name,
    stdOps: std.opsPerSec,
    volOps: vol.opsPerSec,
    ratio: speedup,
    winner: volWins ? 'VOLCANO' : 'STANDARD',
    stdRows: stdResult?.rows?.length || 0,
    volRows: volResult?.length || 0,
  });
}

// Print results
console.log('═'.repeat(90));
console.log(`${'Query'.padEnd(25)} ${'Std ops/s'.padStart(10)} ${'Vol ops/s'.padStart(10)} ${'Ratio'.padStart(8)} ${'Winner'.padStart(10)} ${'Rows'.padStart(8)}`);
console.log('─'.repeat(90));

for (const r of results) {
  const correct = r.stdRows === r.volRows ? '✓' : `✗ (${r.stdRows}/${r.volRows})`;
  console.log(
    `${r.query.padEnd(25)} ${String(r.stdOps).padStart(10)} ${String(r.volOps).padStart(10)} ${r.ratio.padStart(8)} ${r.winner.padStart(10)} ${correct.padStart(8)}`
  );
}

console.log('═'.repeat(90));
console.log('\nRatio = std/vol (>1 means standard is faster, <1 means volcano is faster)');
console.log(`Database: ${ROWS} rows, ${ITERATIONS} iterations per benchmark\n`);

// Summary
const volWins = results.filter(r => r.winner === 'VOLCANO').length;
const stdWins = results.filter(r => r.winner === 'STANDARD').length;
console.log(`Volcano wins: ${volWins}/${results.length} queries`);
console.log(`Standard wins: ${stdWins}/${results.length} queries`);

// Row count correctness
const correct = results.filter(r => r.stdRows === r.volRows).length;
console.log(`Row count match: ${correct}/${results.length} queries`);
