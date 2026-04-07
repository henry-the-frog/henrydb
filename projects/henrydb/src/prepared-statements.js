// prepared-statements.js — Prepared statement support for HenryDB
// Implements PREPARE/EXECUTE/DEALLOCATE SQL commands
// 
// Prepared statements cache the parsed AST and optionally the volcano plan,
// then substitute parameters on EXECUTE.

import { parse } from './sql.js';
import { buildPlan } from './volcano-planner.js';

/**
 * PreparedStatementManager — manages named prepared statements per session.
 */
export class PreparedStatementManager {
  constructor() {
    this._stmts = new Map(); // name → { sql, ast, paramTypes, paramCount }
  }

  /**
   * PREPARE name (type1, type2, ...) AS query
   * Parameters are referenced as $1, $2, ... in the query.
   */
  prepare(name, sql, paramTypes = []) {
    if (this._stmts.has(name)) {
      throw new Error(`Prepared statement "${name}" already exists`);
    }
    
    // Count parameters ($1, $2, etc.)
    const paramMatches = sql.match(/\$\d+/g) || [];
    const paramNums = paramMatches.map(p => parseInt(p.slice(1)));
    const paramCount = paramNums.length > 0 ? Math.max(...paramNums) : 0;
    
    // Store the template SQL — don't parse yet (parameters need substitution first)
    this._stmts.set(name, {
      sql,
      paramTypes,
      paramCount,
    });
    
    return { type: 'OK', message: 'PREPARE' };
  }

  /**
   * EXECUTE name (val1, val2, ...)
   * Returns the SQL with parameters substituted.
   */
  execute(name, params = []) {
    const stmt = this._stmts.get(name);
    if (!stmt) {
      throw new Error(`Prepared statement "${name}" does not exist`);
    }
    
    if (params.length < stmt.paramCount) {
      throw new Error(`Expected ${stmt.paramCount} parameters, got ${params.length}`);
    }
    
    // Substitute parameters
    let sql = stmt.sql;
    for (let i = params.length; i >= 1; i--) {
      const param = params[i - 1];
      const replacement = typeof param === 'string' ? `'${param.replace(/'/g, "''")}'` : String(param);
      sql = sql.replace(new RegExp(`\\$${i}`, 'g'), replacement);
    }
    
    return sql;
  }

  /**
   * DEALLOCATE name — remove a prepared statement.
   */
  deallocate(name) {
    if (name === 'ALL') {
      this._stmts.clear();
      return { type: 'OK', message: 'DEALLOCATE ALL' };
    }
    if (!this._stmts.has(name)) {
      throw new Error(`Prepared statement "${name}" does not exist`);
    }
    this._stmts.delete(name);
    return { type: 'OK', message: 'DEALLOCATE' };
  }

  has(name) { return this._stmts.has(name); }
  get(name) { return this._stmts.get(name); }
  
  /**
   * Parse SQL and check if it's a PREPARE/EXECUTE/DEALLOCATE command.
   * Returns { type, ... } or null if not a prepared statement command.
   */
  static parseCommand(sql) {
    const trimmed = sql.trim();
    const upper = trimmed.toUpperCase();
    
    // PREPARE name [(type, ...)] AS query
    const prepareMatch = trimmed.match(/^PREPARE\s+(\w+)\s*(?:\(([^)]*)\))?\s+AS\s+(.+)$/is);
    if (prepareMatch) {
      const name = prepareMatch[1].toLowerCase();
      const types = prepareMatch[2] ? prepareMatch[2].split(',').map(t => t.trim()) : [];
      const query = prepareMatch[3];
      return { type: 'PREPARE', name, paramTypes: types, sql: query };
    }
    
    // EXECUTE name [(val, ...)]
    const execMatch = trimmed.match(/^EXECUTE\s+(\w+)\s*(?:\((.+)\))?$/is);
    if (execMatch) {
      const name = execMatch[1].toLowerCase();
      const paramsStr = execMatch[2];
      let params = [];
      if (paramsStr) {
        // Parse parameter values (handle strings, numbers)
        params = parseParamValues(paramsStr);
      }
      return { type: 'EXECUTE', name, params };
    }
    
    // DEALLOCATE [PREPARE] name | ALL
    const deallocMatch = trimmed.match(/^DEALLOCATE\s+(?:PREPARE\s+)?(\w+)$/is);
    if (deallocMatch) {
      return { type: 'DEALLOCATE', name: deallocMatch[1].toUpperCase() === 'ALL' ? 'ALL' : deallocMatch[1].toLowerCase() };
    }
    
    return null;
  }
}

/**
 * Parse a comma-separated list of parameter values.
 * Handles: numbers, quoted strings, NULL, booleans.
 */
function parseParamValues(str) {
  const values = [];
  let i = 0;
  
  while (i < str.length) {
    // Skip whitespace and commas
    while (i < str.length && (str[i] === ' ' || str[i] === ',')) i++;
    if (i >= str.length) break;
    
    if (str[i] === "'") {
      // Quoted string
      i++;
      let val = '';
      while (i < str.length) {
        if (str[i] === "'" && str[i + 1] === "'") {
          val += "'";
          i += 2;
        } else if (str[i] === "'") {
          i++;
          break;
        } else {
          val += str[i++];
        }
      }
      values.push(val);
    } else {
      // Unquoted value
      let val = '';
      while (i < str.length && str[i] !== ',') {
        val += str[i++];
      }
      val = val.trim();
      if (val.toUpperCase() === 'NULL') values.push(null);
      else if (val.toUpperCase() === 'TRUE') values.push(true);
      else if (val.toUpperCase() === 'FALSE') values.push(false);
      else if (!isNaN(val) && val !== '') values.push(Number(val));
      else values.push(val);
    }
  }
  
  return values;
}
