// repl.js — Interactive HenryDB SQL shell
// Usage: node src/repl.js

import readline from 'node:readline';
import { Database } from './db.js';

const COLORS = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  red: '\x1b[31m',
  cyan: '\x1b[36m',
  gray: '\x1b[90m',
};

/**
 * Format query results as a table.
 */
function formatTable(rows) {
  if (!rows || rows.length === 0) return '(empty)';
  
  const columns = Object.keys(rows[0]);
  const widths = columns.map(col => col.length);
  
  // Calculate column widths
  for (const row of rows) {
    for (let i = 0; i < columns.length; i++) {
      const val = String(row[columns[i]] ?? 'NULL');
      widths[i] = Math.max(widths[i], val.length);
    }
  }
  
  // Build table
  const lines = [];
  const sep = widths.map(w => '─'.repeat(w + 2)).join('┼');
  
  // Header
  const header = columns.map((col, i) => ` ${COLORS.bold}${col.padEnd(widths[i])}${COLORS.reset} `).join('│');
  lines.push(header);
  lines.push(sep);
  
  // Rows
  for (const row of rows) {
    const vals = columns.map((col, i) => {
      const val = row[col];
      const str = val === null || val === undefined ? `${COLORS.dim}NULL${COLORS.reset}` : String(val);
      const padded = String(val ?? 'NULL').padEnd(widths[i]);
      return ` ${val === null || val === undefined ? COLORS.dim + padded + COLORS.reset : padded} `;
    });
    lines.push(vals.join('│'));
  }
  
  return lines.join('\n');
}

/**
 * HenryDB REPL — interactive SQL shell.
 */
export class HenryDBRepl {
  constructor(options = {}) {
    this._db = options.database || new Database();
    this._output = options.output || process.stdout;
    this._input = options.input || process.stdin;
    this._timing = true;
    this._history = [];
  }

  /** Write to output. */
  _write(text) {
    this._output.write(text + '\n');
  }

  /** Process a command or SQL statement. */
  execute(input) {
    const trimmed = input.trim();
    if (!trimmed) return;
    
    this._history.push(trimmed);
    
    // Dot commands
    if (trimmed.startsWith('.')) {
      return this._dotCommand(trimmed);
    }
    
    // SQL
    const start = performance.now();
    try {
      const result = this._db.execute(trimmed);
      const elapsed = performance.now() - start;
      
      if (result.rows && result.rows.length > 0) {
        this._write(formatTable(result.rows));
        this._write(`${COLORS.green}${result.rows.length} row(s)${COLORS.reset}` + 
          (this._timing ? ` ${COLORS.gray}(${elapsed.toFixed(1)}ms)${COLORS.reset}` : ''));
      } else if (result.count !== undefined) {
        this._write(`${COLORS.green}${result.count} row(s) affected${COLORS.reset}` +
          (this._timing ? ` ${COLORS.gray}(${elapsed.toFixed(1)}ms)${COLORS.reset}` : ''));
      } else {
        this._write(`${COLORS.green}${result.message || 'OK'}${COLORS.reset}` +
          (this._timing ? ` ${COLORS.gray}(${elapsed.toFixed(1)}ms)${COLORS.reset}` : ''));
      }
    } catch (e) {
      this._write(`${COLORS.red}Error: ${e.message}${COLORS.reset}`);
    }
  }

  /** Handle dot commands (.tables, .schema, etc.) */
  _dotCommand(cmd) {
    const parts = cmd.split(/\s+/);
    const command = parts[0].toLowerCase();
    
    switch (command) {
      case '.help':
        this._write(`${COLORS.bold}HenryDB REPL Commands:${COLORS.reset}`);
        this._write(`  .tables          — List all tables`);
        this._write(`  .schema <table>  — Show table schema`);
        this._write(`  .indexes         — List all indexes`);
        this._write(`  .timing on|off   — Toggle query timing`);
        this._write(`  .history         — Show command history`);
        this._write(`  .clear           — Clear screen`);
        this._write(`  .quit            — Exit REPL`);
        this._write(`  ${COLORS.dim}Any other input is executed as SQL${COLORS.reset}`);
        break;
        
      case '.tables':
        for (const [name, table] of this._db.tables) {
          const cols = table.schema.map(c => c.name).join(', ');
          this._write(`  ${COLORS.cyan}${name}${COLORS.reset} (${cols})`);
        }
        if (this._db.tables.size === 0) this._write('  (no tables)');
        break;
        
      case '.schema': {
        const tableName = parts[1];
        if (!tableName) {
          this._write(`${COLORS.red}Usage: .schema <table_name>${COLORS.reset}`);
          break;
        }
        const table = this._db.tables.get(tableName);
        if (!table) {
          this._write(`${COLORS.red}Table "${tableName}" not found${COLORS.reset}`);
          break;
        }
        this._write(`${COLORS.bold}CREATE TABLE ${tableName} (${COLORS.reset}`);
        for (const col of table.schema) {
          const constraints = [];
          if (col.primaryKey) constraints.push('PRIMARY KEY');
          if (col.notNull) constraints.push('NOT NULL');
          if (col.unique) constraints.push('UNIQUE');
          if (col.default !== undefined) constraints.push(`DEFAULT ${col.default}`);
          this._write(`  ${COLORS.cyan}${col.name}${COLORS.reset} ${col.type}${constraints.length ? ' ' + constraints.join(' ') : ''}`);
        }
        this._write(`${COLORS.bold})${COLORS.reset}`);
        break;
      }
        
      case '.indexes':
        for (const [name, idx] of this._db.indexCatalog) {
          this._write(`  ${COLORS.cyan}${name}${COLORS.reset} on ${idx.table}(${idx.columns.join(', ')})${idx.unique ? ' UNIQUE' : ''}`);
        }
        if (this._db.indexCatalog.size === 0) this._write('  (no indexes)');
        break;
        
      case '.timing':
        if (parts[1] === 'off') {
          this._timing = false;
          this._write('Timing off');
        } else {
          this._timing = true;
          this._write('Timing on');
        }
        break;
        
      case '.history':
        for (let i = 0; i < this._history.length; i++) {
          this._write(`  ${COLORS.gray}${i + 1}${COLORS.reset} ${this._history[i]}`);
        }
        break;
        
      case '.clear':
        this._output.write('\x1b[2J\x1b[H');
        break;
        
      case '.quit':
      case '.exit':
        return 'quit';
        
      default:
        this._write(`${COLORS.red}Unknown command: ${command}. Type .help for help.${COLORS.reset}`);
    }
  }

  /** Start the interactive REPL. */
  start() {
    const rl = readline.createInterface({
      input: this._input,
      output: this._output,
      prompt: `${COLORS.blue}henrydb>${COLORS.reset} `,
      terminal: this._input.isTTY ?? true,
    });

    this._write(`${COLORS.bold}HenryDB Interactive Shell${COLORS.reset}`);
    this._write(`${COLORS.dim}Type .help for commands, SQL statements end with ;${COLORS.reset}\n`);
    
    rl.prompt();

    let multiline = '';

    rl.on('line', (line) => {
      const trimmed = line.trim();
      
      // Multi-line SQL support
      if (multiline || (trimmed && !trimmed.startsWith('.') && !trimmed.endsWith(';') && trimmed.length > 0)) {
        multiline += (multiline ? ' ' : '') + trimmed;
        if (trimmed.endsWith(';')) {
          const result = this.execute(multiline);
          multiline = '';
          if (result === 'quit') { rl.close(); return; }
        } else {
          this._output.write(`${COLORS.gray}   ...>${COLORS.reset} `);
          return;
        }
      } else {
        const result = this.execute(trimmed.endsWith(';') ? trimmed : trimmed);
        if (result === 'quit') { rl.close(); return; }
      }
      
      rl.prompt();
    });

    rl.on('close', () => {
      this._write(`\n${COLORS.dim}Goodbye!${COLORS.reset}`);
    });
  }
}

// CLI entry point
const isMain = process.argv[1] && process.argv[1].endsWith('repl.js');
if (isMain) {
  const repl = new HenryDBRepl();
  repl.start();
}
