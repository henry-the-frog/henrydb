// procedure-handler.js — Extracted from db.js (2026-04-23)
// CALL procedure and user-defined function execution

/**
 * Execute a stored procedure by substituting parameters and running the body.
 * @param {object} db - Database instance
 * @param {object} ast - Parsed CALL AST
 * @returns {object} Execution result
 */
export function callProcedure(db, ast) {
  const funcDef = db._functions.get(ast.name.toLowerCase());
  if (!funcDef) throw new Error(`Procedure ${ast.name} not found`);
  
  const args = ast.args.map(a => db._evalValue(a, {}));
  
  let body = funcDef.body;
  for (let i = 0; i < funcDef.params.length; i++) {
    const param = funcDef.params[i];
    const val = args[i];
    const regex = new RegExp('\\b' + param.name + '\\b', 'gi');
    if (val === null) {
      body = body.replace(regex, 'NULL');
    } else if (typeof val === 'number') {
      body = body.replace(regex, String(val));
    } else {
      body = body.replace(regex, `'${String(val).replace(/'/g, "''")}'`);
    }
  }
  
  return db.execute(body);
}

/**
 * Evaluate a user-defined SQL or JS function call.
 * @param {object} db - Database instance
 * @param {object} funcDef - Function definition
 * @param {Array} args - Argument values
 * @returns {*} Function result
 */
export function callUserFunction(db, funcDef, args) {
  if (funcDef.language === 'sql') {
    let body = funcDef.body;
    
    if (body.toUpperCase().startsWith('RETURN ')) {
      body = 'SELECT ' + body.substring(7);
    }
    
    if (body.toUpperCase().startsWith('SELECT')) {
      for (let i = 0; i < funcDef.params.length; i++) {
        const param = funcDef.params[i];
        const val = args[i];
        const regex = new RegExp('\\b' + param.name + '\\b', 'gi');
        if (val === null) {
          body = body.replace(regex, 'NULL');
        } else if (typeof val === 'number') {
          body = body.replace(regex, String(val));
        } else {
          body = body.replace(regex, `'${String(val).replace(/'/g, "''")}'`);
        }
      }
      const result = db.execute(body);
      const rows = result.rows || result;

      if (funcDef.returnType === 'TABLE') {
        return { type: 'TABLE_RESULT', rows: rows || [] };
      }

      if (!rows || rows.length === 0) return null;
      const firstRow = rows[0];
      const keys = Object.keys(firstRow);
      return firstRow[keys[0]];
    }
    throw new Error(`Function body must start with SELECT: ${body}`);
  } else if (funcDef.language === 'js') {
    const paramNames = funcDef.params.map(p => p.name);
    try {
      const fn = new Function(...paramNames, `return ${funcDef.body}`);
      return fn(...args);
    } catch (e) {
      throw new Error(`Error in JS function: ${e.message}`);
    }
  }
  throw new Error(`Unsupported function language: ${funcDef.language}`);
}
