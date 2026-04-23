// prepared-stmts-ast.js — Extracted from db.js (2026-04-23)
// AST-based prepared statement handling (PREPARE/EXECUTE/DEALLOCATE via parsed AST)

import { parse } from './sql.js';

/**
 * PREPARE name AS query — store AST for later execution
 * @param {object} db - Database instance
 * @param {object} ast - Parsed PREPARE AST
 * @returns {object} Result
 */
export function prepareSql(db, ast) {
  const name = ast.name;
  if (db._prepared.has(name)) {
    throw new Error(`Prepared statement '${name}' already exists`);
  }
  db._prepared.set(name, { ast: ast.query, name });
  return { message: `PREPARE ${name}` };
}

/**
 * EXECUTE name(params) — execute a prepared statement with bound parameters
 * @param {object} db - Database instance
 * @param {object} ast - Parsed EXECUTE AST
 * @returns {object} Query result
 */
export function executePrepared(db, ast) {
  const name = ast.name;
  if (!db._prepared.has(name)) {
    throw new Error(`Prepared statement '${name}' not found`);
  }
  const stmt = db._prepared.get(name);
  
  const paramValues = ast.params.map(p => {
    if (p.type === 'literal') return p.value;
    if (p.type === 'PARAM') throw new Error('Cannot use parameters in EXECUTE parameter list');
    return p.value;
  });
  
  const boundAst = bindParams(JSON.parse(JSON.stringify(stmt.ast)), paramValues);
  return db.execute_ast(boundAst);
}

/**
 * DEALLOCATE name / DEALLOCATE ALL — remove prepared statements
 * @param {object} db - Database instance
 * @param {object} ast - Parsed DEALLOCATE AST
 * @returns {object} Result
 */
export function deallocate(db, ast) {
  if (ast.all) {
    const count = db._prepared.size;
    db._prepared.clear();
    return { message: `DEALLOCATE ALL (${count} statements)` };
  }
  if (!db._prepared.has(ast.name)) {
    throw new Error(`Prepared statement '${ast.name}' not found`);
  }
  db._prepared.delete(ast.name);
  return { message: `DEALLOCATE ${ast.name}` };
}

/**
 * Bind parameter values into an AST by replacing PARAM nodes with literals.
 * @param {object} node - AST node to process
 * @param {Array} params - Parameter values
 * @returns {object} Modified AST node
 */
export function bindParams(node, params) {
  if (!node || typeof node !== 'object') return node;
  
  if (node.type === 'PARAM') {
    const idx = node.index - 1;
    if (idx < 0 || idx >= params.length) {
      throw new Error(`Parameter $${node.index} not provided (got ${params.length} params)`);
    }
    return { type: 'literal', value: params[idx] };
  }
  
  for (const key of Object.keys(node)) {
    if (Array.isArray(node[key])) {
      node[key] = node[key].map(item => bindParams(item, params));
    } else if (typeof node[key] === 'object' && node[key] !== null) {
      node[key] = bindParams(node[key], params);
    }
  }
  
  return node;
}

/**
 * Programmatic API: prepare a statement for repeated execution.
 * @param {object} db - Database instance
 * @param {string} sql - SQL to prepare
 * @returns {object} PreparedStatement with execute() and close()
 */
export function prepare(db, sql) {
  const ast = parse(sql);
  const name = `__stmt_${db._prepared.size}`;
  db._prepared.set(name, { ast, name });
  
  return {
    name,
    execute(...params) {
      const flatParams = params.length === 1 && Array.isArray(params[0]) ? params[0] : params;
      const bound = bindParams(JSON.parse(JSON.stringify(ast)), flatParams);
      return db.execute_ast(bound);
    },
    close() {
      db._prepared.delete(name);
    },
  };
}
