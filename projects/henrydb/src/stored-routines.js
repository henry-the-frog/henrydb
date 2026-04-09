// stored-routines.js — CREATE FUNCTION/PROCEDURE, CALL, and stored routine catalog
// Integrates PL/HenryDB with the database engine.

import { PLParser, PLInterpreter, PLRaise } from './plsql.js';

/**
 * StoredRoutineCatalog — stores and manages user-defined functions and procedures.
 */
export class StoredRoutineCatalog {
  constructor() {
    this._functions = new Map(); // name → FunctionDef
    this._procedures = new Map(); // name → ProcedureDef
  }

  /**
   * Register a function.
   */
  createFunction(name, params, returnType, body, options = {}) {
    const lowerName = name.toLowerCase();
    if (this._functions.has(lowerName) && !options.orReplace) {
      throw new Error(`Function '${name}' already exists`);
    }

    const parser = new PLParser(body);
    const ast = parser.parse();

    this._functions.set(lowerName, {
      name,
      params, // [{ name, type, default? }]
      returnType,
      body,
      ast,
      language: options.language || 'plhenrydb',
      volatile: options.volatile || 'VOLATILE', // VOLATILE | STABLE | IMMUTABLE
      createdAt: Date.now(),
    });
  }

  /**
   * Register a procedure.
   */
  createProcedure(name, params, body, options = {}) {
    const lowerName = name.toLowerCase();
    if (this._procedures.has(lowerName) && !options.orReplace) {
      throw new Error(`Procedure '${name}' already exists`);
    }

    const parser = new PLParser(body);
    const ast = parser.parse();

    this._procedures.set(lowerName, {
      name,
      params,
      body,
      ast,
      language: options.language || 'plhenrydb',
      createdAt: Date.now(),
    });
  }

  /**
   * Execute a function with the given arguments.
   */
  callFunction(name, args, db) {
    const lowerName = name.toLowerCase();
    const fn = this._functions.get(lowerName);
    if (!fn) throw new Error(`Function '${name}' does not exist`);

    // Bind arguments to parameter names
    const params = {};
    for (let i = 0; i < fn.params.length; i++) {
      params[fn.params[i].name] = i < args.length ? args[i] : (fn.params[i].default ?? null);
    }

    const interp = new PLInterpreter(db);
    const result = interp.execute(fn.ast, params);
    return { result, notices: interp.notices };
  }

  /**
   * Execute a procedure with the given arguments.
   */
  callProcedure(name, args, db) {
    const lowerName = name.toLowerCase();
    const proc = this._procedures.get(lowerName);
    if (!proc) throw new Error(`Procedure '${name}' does not exist`);

    const params = {};
    for (let i = 0; i < proc.params.length; i++) {
      params[proc.params[i].name] = i < args.length ? args[i] : (proc.params[i].default ?? null);
    }

    const interp = new PLInterpreter(db);
    interp.execute(proc.ast, params);
    return { notices: interp.notices };
  }

  /**
   * Drop a function.
   */
  dropFunction(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    if (!this._functions.has(lowerName)) {
      if (ifExists) return false;
      throw new Error(`Function '${name}' does not exist`);
    }
    this._functions.delete(lowerName);
    return true;
  }

  /**
   * Drop a procedure.
   */
  dropProcedure(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    if (!this._procedures.has(lowerName)) {
      if (ifExists) return false;
      throw new Error(`Procedure '${name}' does not exist`);
    }
    this._procedures.delete(lowerName);
    return true;
  }

  hasFunction(name) {
    return this._functions.has(name.toLowerCase());
  }

  hasProcedure(name) {
    return this._procedures.has(name.toLowerCase());
  }

  getFunction(name) {
    return this._functions.get(name.toLowerCase());
  }

  getProcedure(name) {
    return this._procedures.get(name.toLowerCase());
  }

  /**
   * List all routines (for information_schema).
   */
  listRoutines() {
    const routines = [];
    for (const [name, fn] of this._functions) {
      routines.push({
        name: fn.name,
        type: 'FUNCTION',
        params: fn.params.map(p => `${p.name} ${p.type}`).join(', '),
        returnType: fn.returnType,
        language: fn.language,
        volatile: fn.volatile,
      });
    }
    for (const [name, proc] of this._procedures) {
      routines.push({
        name: proc.name,
        type: 'PROCEDURE',
        params: proc.params.map(p => `${p.name} ${p.type}`).join(', '),
        returnType: null,
        language: proc.language,
        volatile: null,
      });
    }
    return routines;
  }
}

/**
 * Parse a CREATE FUNCTION or CREATE PROCEDURE statement.
 * 
 * CREATE [OR REPLACE] FUNCTION name(param1 type, param2 type DEFAULT val)
 *   RETURNS returnType
 *   LANGUAGE plhenrydb
 *   AS $$ ... $$;
 * 
 * CREATE [OR REPLACE] PROCEDURE name(param1 type)
 *   LANGUAGE plhenrydb
 *   AS $$ ... $$;
 */
export function parseCreateRoutine(sql) {
  const upper = sql.toUpperCase();
  const orReplace = /\bOR\s+REPLACE\b/i.test(sql);
  const isFunction = /\bFUNCTION\b/i.test(sql);
  const isProcedure = /\bPROCEDURE\b/i.test(sql);

  if (!isFunction && !isProcedure) {
    throw new Error('Expected CREATE FUNCTION or CREATE PROCEDURE');
  }

  const kind = isFunction ? 'FUNCTION' : 'PROCEDURE';

  // Extract name
  const nameMatch = sql.match(new RegExp(`\\b${kind}\\s+(\\w+)\\s*\\(`, 'i'));
  if (!nameMatch) throw new Error(`Cannot parse ${kind} name`);
  const name = nameMatch[1];

  // Extract parameters
  const paramStart = sql.indexOf('(', sql.search(new RegExp(kind, 'i')));
  const paramEnd = findMatchingParen(sql, paramStart);
  const paramStr = sql.substring(paramStart + 1, paramEnd).trim();
  const params = paramStr ? parseParams(paramStr) : [];

  // Extract return type (functions only)
  let returnType = null;
  if (isFunction) {
    const returnsMatch = sql.match(/\bRETURNS\s+(\w+)/i);
    if (returnsMatch) returnType = returnsMatch[1].toUpperCase();
  }

  // Extract body (between $$ ... $$ or ' ... ')
  let body;
  const dollarMatch = sql.match(/\$\$\s*([\s\S]*?)\s*\$\$/);
  if (dollarMatch) {
    body = dollarMatch[1];
  } else {
    const asMatch = sql.match(/\bAS\s+'([\s\S]*?)'/i);
    if (asMatch) body = asMatch[1];
    else throw new Error(`Cannot find ${kind} body`);
  }

  // Extract language
  const langMatch = sql.match(/\bLANGUAGE\s+(\w+)/i);
  const language = langMatch ? langMatch[1].toLowerCase() : 'plhenrydb';

  // Extract volatility
  let volatile = 'VOLATILE';
  if (/\bIMMUTABLE\b/i.test(sql)) volatile = 'IMMUTABLE';
  else if (/\bSTABLE\b/i.test(sql)) volatile = 'STABLE';

  return {
    kind,
    name,
    params,
    returnType,
    body,
    language,
    volatile,
    orReplace,
  };
}

/**
 * Parse a CALL statement: CALL procedure_name(arg1, arg2, ...);
 */
export function parseCall(sql) {
  const match = sql.match(/^\s*CALL\s+(\w+)\s*\(([\s\S]*?)\)\s*;?\s*$/i);
  if (!match) throw new Error('Cannot parse CALL statement');
  
  const name = match[1];
  const argsStr = match[2].trim();
  const args = argsStr ? parseArgList(argsStr) : [];
  
  return { name, args };
}

function findMatchingParen(sql, start) {
  let depth = 0;
  for (let i = start; i < sql.length; i++) {
    if (sql[i] === '(') depth++;
    if (sql[i] === ')') { depth--; if (depth === 0) return i; }
  }
  return sql.length;
}

function parseParams(str) {
  const params = [];
  const parts = splitOutsideParens(str, ',');
  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    const tokens = trimmed.split(/\s+/);
    const param = { name: tokens[0], type: tokens.length > 1 ? tokens[1].toUpperCase() : 'TEXT' };
    // Check for DEFAULT
    const defIdx = tokens.findIndex(t => t.toUpperCase() === 'DEFAULT');
    if (defIdx >= 0 && defIdx + 1 < tokens.length) {
      param.default = parseArgValue(tokens.slice(defIdx + 1).join(' '));
    }
    params.push(param);
  }
  return params;
}

function parseArgList(str) {
  const parts = splitOutsideParens(str, ',');
  return parts.map(p => parseArgValue(p.trim()));
}

function parseArgValue(str) {
  if (str.startsWith("'") && str.endsWith("'")) return str.slice(1, -1);
  if (/^-?\d+$/.test(str)) return parseInt(str);
  if (/^-?\d+\.\d+$/.test(str)) return parseFloat(str);
  if (str.toUpperCase() === 'NULL') return null;
  if (str.toUpperCase() === 'TRUE') return true;
  if (str.toUpperCase() === 'FALSE') return false;
  return str;
}

function splitOutsideParens(str, delimiter) {
  const parts = [];
  let depth = 0;
  let current = '';
  let inString = false;
  for (const ch of str) {
    if (ch === "'" && !inString) inString = true;
    else if (ch === "'" && inString) inString = false;
    if (!inString) {
      if (ch === '(') depth++;
      if (ch === ')') depth--;
      if (ch === delimiter && depth === 0) {
        parts.push(current);
        current = '';
        continue;
      }
    }
    current += ch;
  }
  if (current) parts.push(current);
  return parts;
}
