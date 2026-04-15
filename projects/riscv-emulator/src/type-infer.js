// type-infer.js — Simple type inference for monkey-lang AST
//
// Walks the AST and infers types for variables and function parameters.
// Used by the RISC-V codegen to generate type-correct code (e.g., puts for strings vs integers).
//
// This is a simple forward analysis, not full Hindley-Milner.
// It handles the common cases: literal types, variable assignments, and call-site inference.

/**
 * Infer types for all variables and function parameters in a program.
 * @param {Program} program - The monkey-lang AST
 * @returns {Map<string, { params: Map<string, string>, returnType: string }>} Function type info
 *          + Map<string, string> for variable types
 */
export function inferTypes(program) {
  const varTypes = new Map();     // variable name → type
  const funcTypes = new Map();    // function name → { params: Map<paramName, type>, returnType }
  const funcDefs = new Map();     // function name → FunctionLiteral

  // Pass 1: Collect function definitions and simple variable types
  for (const stmt of program.statements) {
    if (stmt.constructor.name === 'LetStatement') {
      const name = stmt.name.value;
      const type = exprType(stmt.value, varTypes);
      varTypes.set(name, type);
      
      if (stmt.value?.constructor.name === 'FunctionLiteral') {
        funcDefs.set(name, stmt.value);
      }
    }
  }

  // Pass 2: Infer function parameter types from call sites
  walkCallSites(program, funcDefs, funcTypes, varTypes);

  return { varTypes, funcTypes };
}

/**
 * Determine the type of an expression.
 */
function exprType(expr, varTypes) {
  if (!expr) return 'unknown';
  const name = expr.constructor.name;
  switch (name) {
    case 'IntegerLiteral': return 'int';
    case 'BooleanLiteral': return 'int'; // booleans are ints in our codegen
    case 'StringLiteral': return 'string';
    case 'ArrayLiteral': return 'array';
    case 'Identifier':
      return varTypes.get(expr.value) || 'unknown';
    case 'PrefixExpression':
      return 'int'; // -x, !x are always int
    case 'InfixExpression':
      return 'int'; // arithmetic/comparison always int
    case 'CallExpression': {
      const funcName = expr.function?.value;
      if (funcName === 'len' || funcName === 'first' || funcName === 'last') return 'int';
      if (funcName === 'push') return 'array';
      return 'unknown';
    }
    case 'IfExpression':
      return 'unknown'; // Could be either branch
    default:
      return 'unknown';
  }
}

/**
 * Walk the AST to find call sites and infer parameter types.
 */
function walkCallSites(node, funcDefs, funcTypes, varTypes) {
  if (!node) return;
  
  if (Array.isArray(node)) {
    for (const item of node) walkCallSites(item, funcDefs, funcTypes, varTypes);
    return;
  }
  
  if (typeof node !== 'object') return;
  
  const name = node.constructor.name;
  
  if (name === 'CallExpression') {
    const funcName = node.function?.value;
    if (funcName && funcDefs.has(funcName)) {
      const funcLit = funcDefs.get(funcName);
      const params = funcLit.parameters || [];
      const args = node.arguments || [];
      
      if (!funcTypes.has(funcName)) {
        funcTypes.set(funcName, { params: new Map(), returnType: 'unknown' });
      }
      
      const info = funcTypes.get(funcName);
      for (let i = 0; i < Math.min(params.length, args.length); i++) {
        const paramName = params[i].value;
        const argType = exprType(args[i], varTypes);
        if (argType !== 'unknown') {
          // Only overwrite if we have concrete info
          const existing = info.params.get(paramName);
          if (!existing || existing === 'unknown') {
            info.params.set(paramName, argType);
          }
        }
      }
    }
  }
  
  // Recurse into child nodes
  if (node.statements) walkCallSites(node.statements, funcDefs, funcTypes, varTypes);
  if (node.body) walkCallSites(node.body, funcDefs, funcTypes, varTypes);
  if (node.consequence) walkCallSites(node.consequence, funcDefs, funcTypes, varTypes);
  if (node.alternative) walkCallSites(node.alternative, funcDefs, funcTypes, varTypes);
  if (node.condition) walkCallSites(node.condition, funcDefs, funcTypes, varTypes);
  if (node.left) walkCallSites(node.left, funcDefs, funcTypes, varTypes);
  if (node.right) walkCallSites(node.right, funcDefs, funcTypes, varTypes);
  if (node.value) walkCallSites(node.value, funcDefs, funcTypes, varTypes);
  if (node.expression) walkCallSites(node.expression, funcDefs, funcTypes, varTypes);
  if (node.function) walkCallSites(node.function, funcDefs, funcTypes, varTypes);
  if (node.arguments) walkCallSites(node.arguments, funcDefs, funcTypes, varTypes);
  if (node.elements) walkCallSites(node.elements, funcDefs, funcTypes, varTypes);
  if (node.iterable) walkCallSites(node.iterable, funcDefs, funcTypes, varTypes);
  if (node.index) walkCallSites(node.index, funcDefs, funcTypes, varTypes);
  if (node.returnValue) walkCallSites(node.returnValue, funcDefs, funcTypes, varTypes);
}
