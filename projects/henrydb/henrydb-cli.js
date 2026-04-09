#!/usr/bin/env node
// henrydb-cli.js — Interactive SQL shell for HenryDB
//
// Usage: node henrydb-cli.js [--persist path]
//
// Commands:
//   .help        — Show help
//   .tables      — List all tables
//   .schema TABLE — Show table schema
//   .indexes     — List all indexes
//   .stats       — Show database statistics
//   .profile SQL — Profile a query with timing
//   .timer on|off — Toggle query timing
//   .mode table|csv|json — Output format
//   .quit        — Exit

import { createInterface } from 'node:readline';
import { Database } from './src/db.js';

const args = process.argv.slice(2);
const persistPath = args.includes('--persist') ? args[args.indexOf('--persist') + 1] : null;

const db = new Database();
let showTimer = true;
let outputMode = 'table'; // table, csv, json

console.log(`
🐸 HenryDB Interactive Shell
Version 1.0 | Type .help for commands | .quit to exit
${persistPath ? `Persistent: ${persistPath}` : 'In-memory mode'}
`);

const rl = createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: 'henrydb> ',
  historySize: 1000,
});

function formatTable(rows, columns) {
  if (!rows || rows.length === 0) return '(0 rows)';
  
  const cols = columns || Object.keys(rows[0]);
  const widths = cols.map(c => Math.max(
    c.length,
    ...rows.map(r => String(r[c] ?? 'NULL').length)
  ));
  
  const sep = widths.map(w => '─'.repeat(w + 2)).join('┼');
  const header = cols.map((c, i) => ` ${c.padEnd(widths[i])} `).join('│');
  
  const lines = [
    '┌' + widths.map(w => '─'.repeat(w + 2)).join('┬') + '┐',
    '│' + header + '│',
    '├' + sep + '┤',
    ...rows.map(r => 
      '│' + cols.map((c, i) => ` ${String(r[c] ?? 'NULL').padEnd(widths[i])} `).join('│') + '│'
    ),
    '└' + widths.map(w => '─'.repeat(w + 2)).join('┴') + '┘',
  ];
  
  return lines.join('\n');
}

function formatCSV(rows) {
  if (!rows || rows.length === 0) return '';
  const cols = Object.keys(rows[0]);
  return [cols.join(','), ...rows.map(r => cols.map(c => r[c] ?? '').join(','))].join('\n');
}

function handleDotCommand(input) {
  const [cmd, ...rest] = input.trim().split(/\s+/);
  
  switch (cmd) {
    case '.help':
      console.log(`
Commands:
  .help          Show this help
  .tables        List all tables
  .schema TABLE  Show table schema
  .indexes       List all indexes
  .stats         Show database statistics
  .profile SQL   Profile a query with timing breakdown
  .timer on|off  Toggle query timing (default: on)
  .mode FORMAT   Output format: table, csv, json
  .quit          Exit
  
SQL:
  Enter any SQL statement (SELECT, INSERT, CREATE TABLE, etc.)
  Multi-line SQL: end with semicolon
`);
      break;
    
    case '.tables': {
      const tables = [...db.tables.keys()];
      if (tables.length === 0) {
        console.log('(no tables)');
      } else {
        for (const name of tables.sort()) {
          const info = db.tables.get(name);
          const engine = info.heap.constructor.name === 'BTreeTable' ? 'btree' : 'heap';
          console.log(`  ${name} (${info.heap.rowCount || 0} rows, ${engine})`);
        }
      }
      break;
    }
    
    case '.schema': {
      const tableName = rest[0];
      if (!tableName) { console.log('Usage: .schema TABLE'); break; }
      const table = db.tables.get(tableName);
      if (!table) { console.log(`Table '${tableName}' not found`); break; }
      console.log(`CREATE TABLE ${tableName} (`);
      for (const col of table.schema) {
        const parts = [`  ${col.name} ${col.type || 'TEXT'}`];
        if (col.primaryKey) parts.push('PRIMARY KEY');
        if (col.notNull) parts.push('NOT NULL');
        if (col.defaultValue !== undefined) parts.push(`DEFAULT ${col.defaultValue}`);
        console.log(parts.join(' '));
      }
      console.log(');');
      break;
    }
    
    case '.indexes': {
      let count = 0;
      for (const [tableName, table] of db.tables) {
        if (table.indexMeta && table.indexMeta.size > 0) {
          for (const [colName, meta] of table.indexMeta) {
            const type = meta.indexType || 'BTREE';
            console.log(`  ${meta.name} ON ${tableName}(${meta.columns.join(', ')}) USING ${type}${meta.unique ? ' UNIQUE' : ''}`);
            count++;
          }
        }
      }
      if (count === 0) console.log('(no indexes)');
      break;
    }
    
    case '.stats': {
      const tables = [...db.tables.entries()];
      let totalRows = 0;
      console.log(`Tables: ${tables.length}`);
      for (const [name, info] of tables) {
        const rows = info.heap.rowCount || 0;
        totalRows += rows;
      }
      console.log(`Total rows: ${totalRows}`);
      console.log(`Prepared statements: ${db._prepared.size}`);
      break;
    }
    
    case '.profile': {
      const sql = rest.join(' ');
      if (!sql) { console.log('Usage: .profile SQL'); break; }
      try {
        const { result, profile } = db.profile(sql);
        console.log(profile.formatted);
        if (result?.rows?.length > 0) {
          console.log('');
          displayResult(result);
        }
      } catch (e) {
        console.log(`Error: ${e.message}`);
      }
      break;
    }
    
    case '.timer': {
      const val = rest[0];
      if (val === 'on') { showTimer = true; console.log('Timer: on'); }
      else if (val === 'off') { showTimer = false; console.log('Timer: off'); }
      else console.log(`Timer: ${showTimer ? 'on' : 'off'}`);
      break;
    }
    
    case '.mode': {
      const mode = rest[0];
      if (['table', 'csv', 'json'].includes(mode)) {
        outputMode = mode;
        console.log(`Output mode: ${mode}`);
      } else {
        console.log(`Current: ${outputMode}. Options: table, csv, json`);
      }
      break;
    }
    
    case '.quit':
    case '.exit':
      console.log('Bye! 🐸');
      process.exit(0);
      break;
    
    default:
      console.log(`Unknown command: ${cmd}. Type .help for commands.`);
  }
}

function displayResult(result) {
  if (result.rows && result.rows.length > 0) {
    switch (outputMode) {
      case 'table':
        console.log(formatTable(result.rows, result.columns?.map(c => c.name)));
        break;
      case 'csv':
        console.log(formatCSV(result.rows));
        break;
      case 'json':
        console.log(JSON.stringify(result.rows, null, 2));
        break;
    }
    console.log(`(${result.rows.length} row${result.rows.length > 1 ? 's' : ''})`);
  } else if (result.message) {
    console.log(result.message);
  } else if (result.changes !== undefined) {
    console.log(`${result.changes} row${result.changes !== 1 ? 's' : ''} affected`);
  } else {
    console.log('OK');
  }
}

let buffer = '';

rl.prompt();

rl.on('line', (line) => {
  const input = line.trim();
  
  if (!input) { rl.prompt(); return; }
  
  // Dot commands
  if (input.startsWith('.')) {
    handleDotCommand(input);
    rl.prompt();
    return;
  }
  
  // Multi-line SQL: accumulate until semicolon
  buffer += (buffer ? ' ' : '') + input;
  if (!buffer.endsWith(';')) {
    rl.setPrompt('     -> ');
    rl.prompt();
    return;
  }
  
  // Remove trailing semicolon and execute
  const sql = buffer.slice(0, -1).trim();
  buffer = '';
  rl.setPrompt('henrydb> ');
  
  try {
    const t0 = performance.now();
    const result = db.execute(sql);
    const elapsed = performance.now() - t0;
    
    displayResult(result);
    
    if (showTimer) {
      console.log(`Time: ${elapsed.toFixed(3)}ms`);
    }
  } catch (e) {
    console.log(`Error: ${e.message}`);
  }
  
  rl.prompt();
});

rl.on('close', () => {
  console.log('\nBye! 🐸');
  process.exit(0);
});
