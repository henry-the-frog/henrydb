#!/usr/bin/env node
// repl.js — Interactive SQL shell for HenryDB
// Run: node examples/repl.js
// Type SQL statements, press Enter to execute

import { Database } from '../src/db.js';
import { createInterface } from 'readline';

const db = new Database();
const rl = createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: 'henrydb> ',
  terminal: process.stdin.isTTY,
});

console.log('🗄️  HenryDB Interactive Shell');
console.log('Type SQL statements, end with ;');
console.log('Type .help for help, .quit to exit\n');

let buffer = '';

rl.prompt();
rl.on('line', (line) => {
  const trimmed = line.trim();
  
  if (trimmed === '.quit' || trimmed === '.exit') {
    console.log('Bye!');
    process.exit(0);
  }
  
  if (trimmed === '.help') {
    console.log(`
Commands:
  .quit     Exit
  .tables   List all tables
  .schema   Show column info
  .help     Show this help

SQL: Any valid SQL statement (CREATE, INSERT, SELECT, etc.)
`);
    rl.prompt();
    return;
  }
  
  if (trimmed === '.tables') {
    try {
      const result = db.execute("SELECT table_name, table_type FROM information_schema.tables");
      console.table(result.rows);
    } catch (e) { console.error('Error:', e.message); }
    rl.prompt();
    return;
  }
  
  if (trimmed === '.schema') {
    try {
      const result = db.execute("SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns ORDER BY table_name, ordinal_position");
      console.table(result.rows);
    } catch (e) { console.error('Error:', e.message); }
    rl.prompt();
    return;
  }
  
  buffer += line + ' ';
  
  if (buffer.includes(';')) {
    const statements = buffer.split(';').filter(s => s.trim());
    for (const stmt of statements) {
      try {
        const start = performance.now();
        const result = db.execute(stmt.trim());
        const elapsed = performance.now() - start;
        
        if (result.rows && result.rows.length > 0) {
          console.table(result.rows);
          console.log(`(${result.rows.length} rows, ${elapsed.toFixed(1)}ms)\n`);
        } else if (result.type === 'ROWS') {
          console.log(`(0 rows, ${elapsed.toFixed(1)}ms)\n`);
        } else {
          console.log(`OK (${elapsed.toFixed(1)}ms)\n`);
        }
      } catch (e) {
        console.error('Error:', e.message, '\n');
      }
    }
    buffer = '';
  }
  
  rl.prompt();
});

rl.on('close', () => {
  console.log('\nBye!');
  process.exit(0);
});
