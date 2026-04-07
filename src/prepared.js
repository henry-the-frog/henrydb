// prepared.js — Prepared statement support for HenryDB
// Parses SQL with $1, $2 parameter placeholders, binds values, and executes.

import { parse } from './sql.js';

/**
 * PreparedStatement — a parsed SQL statement with parameter placeholders.
 */
export class PreparedStatement {
  /**
   * @param {string} name — statement name (empty string = unnamed)
   * @param {string} sql — SQL with $1, $2, etc. placeholders
   */
  constructor(name, sql) {
    this.name = name;
    this.sql = sql;
    this._paramCount = 0;
    
    // Count parameters
    const matches = sql.match(/\$(\d+)/g);
    if (matches) {
      const nums = matches.map(m => parseInt(m.substring(1)));
      this._paramCount = Math.max(...nums);
    }
    
    // Pre-parse the SQL (replace $N with placeholder literals)
    this._template = sql;
  }

  get paramCount() { return this._paramCount; }

  /**
   * Bind parameter values and produce executable SQL.
   * @param {any[]} params — parameter values
   * @returns {string} — SQL with parameters substituted
   */
  bind(params) {
    if (params.length < this._paramCount) {
      throw new Error(`Expected ${this._paramCount} parameters, got ${params.length}`);
    }
    
    let sql = this._template;
    // Replace $N with literal values (from highest to lowest to avoid $1 replacing part of $10)
    for (let i = this._paramCount; i >= 1; i--) {
      const value = params[i - 1];
      const literal = formatLiteral(value);
      sql = sql.replace(new RegExp('\\$' + i, 'g'), literal);
    }
    
    return sql;
  }
}

/**
 * Format a JavaScript value as a SQL literal.
 */
function formatLiteral(value) {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'number') return String(value);
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
  // String — escape single quotes
  const escaped = String(value).replace(/'/g, "''");
  return `'${escaped}'`;
}

/**
 * PreparedStatementCache — manages named prepared statements.
 */
export class PreparedStatementCache {
  constructor() {
    this._statements = new Map();
  }

  /**
   * Parse and cache a prepared statement.
   */
  prepare(name, sql) {
    const stmt = new PreparedStatement(name, sql);
    this._statements.set(name, stmt);
    return stmt;
  }

  /**
   * Get a cached prepared statement.
   */
  get(name) {
    return this._statements.get(name);
  }

  /**
   * Remove a cached prepared statement.
   */
  close(name) {
    this._statements.delete(name);
  }

  /**
   * Bind parameters to a named statement and return executable SQL.
   */
  bind(name, params) {
    const stmt = this._statements.get(name);
    if (!stmt) throw new Error(`Prepared statement '${name}' not found`);
    return stmt.bind(params);
  }
}

export { formatLiteral };
