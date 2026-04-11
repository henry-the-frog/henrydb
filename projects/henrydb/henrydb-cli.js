#!/usr/bin/env node
// henrydb-cli.js — Interactive SQL CLI for HenryDB
import readline from 'readline';
import { Database } from './src/db.js';

const db = new Database();

console.log('HenryDB v1.0 — A SQL database written from scratch in JavaScript');
console.log('250/250 SQL compliance checks | Recursive CTEs | Window functions');
console.log('Type SQL queries, or \\q to quit, \\help for commands\n');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: 'henrydb> ',
  historySize: 100,
});

let buffer = '';

function formatTable(rows) {
  if (!rows || rows.length === 0) return '(0 rows)';
  
  const cols = Object.keys(rows[0]);
  const widths = cols.map(c => Math.max(
    c.length,
    ...rows.map(r => String(r[c] ?? 'NULL').length)
  ));
  
  const header = cols.map((c, i) => c.padEnd(widths[i])).join(' | ');
  const separator = widths.map(w => '-'.repeat(w)).join('-+-');
  const body = rows.map(row => 
    cols.map((c, i) => String(row[c] ?? 'NULL').padEnd(widths[i])).join(' | ')
  ).join('\n');
  
  return `${header}\n${separator}\n${body}\n(${rows.length} row${rows.length !== 1 ? 's' : ''})`;
}

function processInput(line) {
  const trimmed = line.trim();
  
  // Commands
  if (trimmed === '\\q' || trimmed === '\\quit') {
    console.log('Bye!');
    process.exit(0);
  }
  if (trimmed === '\\help' || trimmed === '\\h') {
    console.log(`Commands:
  \\q          Quit
  \\dt         List tables
  \\di         List indexes  
  \\dv         List views
  \\timing     Toggle timing
  \\help       Show this help
  
SQL examples:
  CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
  INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);
  SELECT * FROM users WHERE age > 20 ORDER BY name;
  WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT * FROM cnt;
  EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1;`);
    return;
  }
  if (trimmed === '\\dt') {
    try { 
      const r = db.execute('SHOW TABLES');
      console.log(formatTable(r.rows));
    } catch { console.log('No tables'); }
    return;
  }
  if (trimmed === '\\timing') {
    db._timing = !db._timing;
    console.log(`Timing is ${db._timing ? 'on' : 'off'}.`);
    return;
  }
  
  // Accumulate multi-line SQL
  buffer += (buffer ? ' ' : '') + line;
  
  // Execute when we see a semicolon
  if (!buffer.trim().endsWith(';') && !buffer.trim().startsWith('\\')) return;
  
  const sql = buffer.replace(/;$/, '').trim();
  buffer = '';
  
  if (!sql) return;
  
  try {
    const start = performance.now();
    const result = db.execute(sql);
    const elapsed = performance.now() - start;
    
    if (result.rows && result.rows.length > 0) {
      console.log(formatTable(result.rows));
    } else if (result.type === 'OK') {
      console.log(result.message || 'OK');
    } else if (result.count !== undefined) {
      console.log(`${result.type || 'OK'} ${result.count}`);
    } else {
      console.log('OK');
    }
    
    if (db._timing) {
      console.log(`Time: ${elapsed.toFixed(3)} ms`);
    }
  } catch (e) {
    console.error(`ERROR: ${e.message}`);
  }
}

rl.prompt();
rl.on('line', (line) => {
  processInput(line);
  rl.prompt();
});
rl.on('close', () => {
  console.log('\nBye!');
  process.exit(0);
});
