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
 * Collect all PARAM node locations in an AST for fast binding.
 * Returns array of { parent, key, arrayIndex?, paramIndex }.
 * @param {object} node - AST root
 * @returns {Array} param slots
 */
function collectParamSlots(node) {
  const slots = [];
  _walkForParams(node, null, null, null, slots);
  return slots;
}

function _walkForParams(node, parent, key, arrayIndex, slots) {
  if (!node || typeof node !== 'object') return;
  
  if (node.type === 'PARAM') {
    slots.push({ parent, key, arrayIndex, paramIndex: node.index - 1 });
    return;
  }
  
  for (const k of Object.keys(node)) {
    const val = node[k];
    if (Array.isArray(val)) {
      for (let i = 0; i < val.length; i++) {
        if (val[i] && typeof val[i] === 'object') {
          _walkForParams(val[i], node, k, i, slots);
        }
      }
    } else if (val && typeof val === 'object') {
      _walkForParams(val, node, k, null, slots);
    }
  }
}

/**
 * Bind params into AST using pre-collected slots (no deep clone).
 * Saves original nodes for restore after execution.
 */
function fastBind(slots, params) {
  const originals = new Array(slots.length);
  for (let i = 0; i < slots.length; i++) {
    const s = slots[i];
    if (s.paramIndex >= params.length) {
      throw new Error(`Parameter $${s.paramIndex + 1} not provided (got ${params.length} params)`);
    }
    const literal = { type: 'literal', value: params[s.paramIndex] };
    if (s.arrayIndex !== null && s.arrayIndex !== undefined) {
      originals[i] = s.parent[s.key][s.arrayIndex];
      s.parent[s.key][s.arrayIndex] = literal;
    } else {
      originals[i] = s.parent[s.key];
      s.parent[s.key] = literal;
    }
  }
  return originals;
}

/**
 * Restore original PARAM nodes after execution.
 */
function fastUnbind(slots, originals) {
  for (let i = 0; i < slots.length; i++) {
    const s = slots[i];
    if (s.arrayIndex !== null && s.arrayIndex !== undefined) {
      s.parent[s.key][s.arrayIndex] = originals[i];
    } else {
      s.parent[s.key] = originals[i];
    }
  }
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
  const slots = collectParamSlots(ast);
  db._prepared.set(name, { ast, name, slots });
  
  return {
    name,
    execute(...params) {
      const flatParams = params.length === 1 && Array.isArray(params[0]) ? params[0] : params;
      if (slots.length === 0) {
        // No params — execute directly
        return db.execute_ast(ast);
      }
      // Fast bind: mutate AST in-place, execute, restore
      const originals = fastBind(slots, flatParams);
      try {
        return db.execute_ast(ast);
      } finally {
        fastUnbind(slots, originals);
      }
    },
    close() {
      db._prepared.delete(name);
    },
    /**
     * Execute the statement for each row of params.
     * @param {Array<Array>} rows — array of param arrays
     * @returns {object} — { count, results? }
     */
    executeMany(rows) {
      let count = 0;
      const results = [];
      for (const row of rows) {
        const flatParams = Array.isArray(row) ? row : [row];
        if (slots.length === 0) {
          results.push(db.execute_ast(ast));
        } else {
          const originals = fastBind(slots, flatParams);
          try {
            results.push(db.execute_ast(ast));
          } finally {
            fastUnbind(slots, originals);
          }
        }
        count++;
      }
      return { count, results };
    },
  };
}
