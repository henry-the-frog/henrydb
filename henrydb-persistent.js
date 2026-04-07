#!/usr/bin/env node
// henrydb-persistent.js — Interactive REPL for HenryDB with file-backed persistence
// Usage: node henrydb-persistent.js [dbdir]
//
// Data persists to disk. Close with .quit for a clean shutdown.

import { PersistentDatabase } from './src/persistent-db.js';
import * as readline from 'readline';

const dbDir = process.argv[2] || './henrydb-data';
console.log(`HenryDB Persistent v0.1 — Data directory: ${dbDir}`);
console.log('Type SQL commands, .help for info, .quit to exit\n');

const db = PersistentDatabase.open(dbDir);
let buffer = '';

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: 'henrydb> ',
});

rl.prompt();

rl.on('line', (line) => {
  const trimmed = line.trim();
  
  // Meta commands
  if (trimmed === '.quit' || trimmed === '.exit') {
    db.close();
    console.log('Database saved. Bye!');
    process.exit(0);
  }
  if (trimmed === '.help') {
    console.log('  .tables    List all tables');
    console.log('  .stats     Buffer pool statistics');
    console.log('  .flush     Flush all dirty pages to disk');
    console.log('  .quit      Save and exit');
    console.log('');
    console.log('  SQL commands end with ;');
    rl.prompt();
    return;
  }
  if (trimmed === '.tables') {
    const tables = [...db._db.tables.keys()];
    if (tables.length === 0) {
      console.log('(no tables)');
    } else {
      for (const name of tables) {
        const table = db._db.tables.get(name);
        const cols = table.schema.map(c => c.name).join(', ');
        console.log(`  ${name} (${cols})`);
      }
    }
    rl.prompt();
    return;
  }
  if (trimmed === '.stats') {
    const stats = db.stats();
    console.log('Buffer Pool:');
    console.log(`  Pool size: ${stats.poolSize}`);
    console.log(`  Used:      ${stats.used}`);
    console.log(`  Pinned:    ${stats.pinned}`);
    console.log(`  Dirty:     ${stats.dirty}`);
    console.log(`  Hits:      ${stats.hits}`);
    console.log(`  Misses:    ${stats.misses}`);
    console.log(`  Hit rate:  ${(stats.hitRate * 100).toFixed(1)}%`);
    console.log(`  Evictions: ${stats.evictions}`);
    console.log(`  Flushes:   ${stats.flushes}`);
    rl.prompt();
    return;
  }
  if (trimmed === '.flush') {
    db.flush();
    console.log('Flushed all dirty pages.');
    rl.prompt();
    return;
  }
  
  // SQL input — accumulate until semicolon
  buffer += (buffer ? '\n' : '') + line;
  
  if (!buffer.trim().endsWith(';')) {
    rl.setPrompt('     -> ');
    rl.prompt();
    return;
  }
  
  // Remove trailing semicolon and execute
  const sql = buffer.trim().replace(/;$/, '').trim();
  buffer = '';
  rl.setPrompt('henrydb> ');
  
  if (!sql) {
    rl.prompt();
    return;
  }
  
  try {
    const start = performance.now();
    const result = db.execute(sql);
    const elapsed = performance.now() - start;
    
    if (result && result.rows && result.rows.length > 0) {
      // Table formatting
      const cols = Object.keys(result.rows[0]);
      const widths = cols.map(c => {
        let max = c.length;
        for (const row of result.rows) {
          const val = row[c] == null ? 'NULL' : String(row[c]);
          max = Math.max(max, val.length);
        }
        return Math.min(max, 40); // cap width
      });
      
      // Header
      const header = cols.map((c, i) => c.padEnd(widths[i])).join(' | ');
      const sep = widths.map(w => '-'.repeat(w)).join('-+-');
      console.log(header);
      console.log(sep);
      
      // Rows
      for (const row of result.rows) {
        const line = cols.map((c, i) => {
          const val = row[c] == null ? 'NULL' : String(row[c]);
          return val.slice(0, 40).padEnd(widths[i]);
        }).join(' | ');
        console.log(line);
      }
      
      console.log(`(${result.rows.length} row${result.rows.length > 1 ? 's' : ''}) — ${elapsed.toFixed(1)}ms`);
    } else if (result && result.type === 'PLAN') {
      // EXPLAIN output
      console.log(JSON.stringify(result.plan, null, 2));
    } else {
      console.log(`OK — ${elapsed.toFixed(1)}ms`);
    }
  } catch (e) {
    console.log(`ERROR: ${e.message}`);
  }
  
  console.log('');
  rl.prompt();
});

rl.on('close', () => {
  db.close();
  console.log('\nDatabase saved. Bye!');
  process.exit(0);
});

// Handle SIGINT gracefully
process.on('SIGINT', () => {
  console.log('\nSaving database...');
  db.close();
  console.log('Saved. Bye!');
  process.exit(0);
});
