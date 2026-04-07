#!/usr/bin/env node
// henrydb-cli.js — Interactive REPL for HenryDB
// Usage: node henrydb-cli.js [dbfile]

import { Database } from './src/db.js';
import * as readline from 'readline';

const db = new Database();
let buffer = '';

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: 'henrydb> ',
});

console.log('HenryDB v0.1 — Type SQL commands, .help for info, .quit to exit');
console.log('');
rl.prompt();

rl.on('line', (line) => {
  const trimmed = line.trim();
  
  // Meta commands
  if (trimmed === '.quit' || trimmed === '.exit') {
    console.log('Bye!');
    process.exit(0);
  }
  if (trimmed === '.help') {
    console.log('  .tables    List all tables');
    console.log('  .schema    Show table schemas');
    console.log('  .stats     Show database statistics');
    console.log('  .quit      Exit');
    rl.prompt();
    return;
  }
  if (trimmed === '.tables') {
    console.log([...db.tables.keys()].join('\n') || '(no tables)');
    rl.prompt();
    return;
  }
  if (trimmed === '.schema') {
    for (const [name, table] of db.tables) {
      console.log(`${name}: ${table.schema.map(c => `${c.name} ${c.type}${c.primaryKey ? ' PK' : ''}`).join(', ')}`);
    }
    rl.prompt();
    return;
  }
  if (trimmed === '.stats') {
    let totalRows = 0;
    for (const [name, table] of db.tables) {
      let count = 0;
      for (const _ of table.heap.scan()) count++;
      totalRows += count;
      console.log(`${name}: ${count} rows, ${table.schema.length} columns, ${table.indexes.size} indexes`);
    }
    console.log(`Total: ${db.tables.size} tables, ${totalRows} rows`);
    rl.prompt();
    return;
  }

  // Accumulate SQL (support multi-line with ; terminator)
  buffer += (buffer ? ' ' : '') + trimmed;
  if (!buffer.endsWith(';') && trimmed !== '') {
    rl.setPrompt('     ...> ');
    rl.prompt();
    return;
  }
  
  const sql = buffer.replace(/;$/, '').trim();
  buffer = '';
  rl.setPrompt('henrydb> ');

  if (!sql) { rl.prompt(); return; }

  try {
    const start = performance.now();
    const result = db.execute(sql);
    const elapsed = performance.now() - start;

    if (result.type === 'ROWS' && result.rows) {
      if (result.rows.length === 0) {
        console.log('(0 rows)');
      } else {
        // Print as table
        const keys = Object.keys(result.rows[0]);
        const widths = keys.map(k => Math.max(k.length, ...result.rows.map(r => String(r[k] ?? 'NULL').length)));
        
        // Header
        console.log(keys.map((k, i) => k.padEnd(widths[i])).join(' | '));
        console.log(widths.map(w => '-'.repeat(w)).join('-+-'));
        
        // Rows
        for (const row of result.rows) {
          console.log(keys.map((k, i) => String(row[k] ?? 'NULL').padEnd(widths[i])).join(' | '));
        }
        console.log(`(${result.rows.length} row${result.rows.length !== 1 ? 's' : ''}) [${elapsed.toFixed(1)}ms]`);
      }
    } else if (result.type === 'PLAN') {
      for (const step of result.plan) {
        console.log(JSON.stringify(step));
      }
    } else if (result.type === 'ANALYZE') {
      console.log(result.message);
      for (const t of result.tables || []) {
        console.log(`  ${t.table}: ${t.rows} rows, ${t.pages} pages`);
      }
    } else {
      console.log(result.message || 'OK');
    }
    console.log(`Time: ${elapsed.toFixed(1)}ms`);
  } catch (e) {
    console.log(`ERROR: ${e.message}`);
  }

  rl.prompt();
});

rl.on('close', () => {
  console.log('\nBye!');
  process.exit(0);
});
