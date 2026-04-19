/**
 * expr-walker.js — Generic expression tree walker for HenryDB
 * 
 * Provides a unified way to traverse SQL expression AST nodes,
 * replacing the 6+ ad-hoc expression walkers in db.js.
 */

/**
 * Get all child expression nodes from an AST node.
 * Handles all known node types in the HenryDB expression AST.
 */
export function getExprChildren(node) {
  if (!node || typeof node !== 'object') return [];
  const children = [];
  
  // Binary operators: arith, COMPARE, AND, OR
  if (node.left) children.push(node.left);
  if (node.right) children.push(node.right);
  
  // Unary: NOT, unary_minus, cast, IS_NULL, IS_NOT_NULL
  if (node.expr && node.type !== 'aggregate_expr') children.push(node.expr);
  if (node.operand) children.push(node.operand);
  
  // Function calls: function_call, function
  if (node.args && Array.isArray(node.args)) {
    for (const arg of node.args) {
      if (arg && typeof arg === 'object') children.push(arg);
    }
  }
  
  // CASE expressions: case, case_expr
  if (node.whens && Array.isArray(node.whens)) {
    for (const w of node.whens) {
      const cond = w.condition || w.when;
      const result = w.result || w.then;
      if (cond) children.push(cond);
      if (result) children.push(result);
    }
  }
  const elseNode = node.elseResult || node.else;
  if (elseNode) children.push(elseNode);
  
  // Aggregate expressions: check .arg (but not as a child walk for aggregate detection)
  if (node.type === 'aggregate_expr' && node.arg && typeof node.arg === 'object') {
    children.push(node.arg);
  }
  
  // Window function: arg
  if (node.type === 'window' && node.arg && typeof node.arg === 'object') {
    children.push(node.arg);
  }
  
  // IN list
  if (node.values && Array.isArray(node.values)) {
    for (const v of node.values) {
      if (v && typeof v === 'object') children.push(v);
    }
  }
  
  // BETWEEN
  if (node.low) children.push(node.low);
  if (node.high) children.push(node.high);
  
  return children;
}

/**
 * Walk an expression tree, calling visitor for each node.
 * 
 * @param {object} node - AST expression node
 * @param {function} visitor - Called with (node). Return truthy to short-circuit (for "contains" checks).
 * @returns {boolean} true if any visitor call returned truthy
 */
export function exprContains(node, predicate) {
  if (!node || typeof node !== 'object') return false;
  if (predicate(node)) return true;
  return getExprChildren(node).some(child => exprContains(child, predicate));
}

/**
 * Collect all nodes matching a predicate from an expression tree.
 * 
 * @param {object} node - AST expression node
 * @param {function} predicate - Return truthy for nodes to collect
 * @param {array} results - Accumulator (optional)
 * @returns {array} Collected nodes
 */
export function exprCollect(node, predicate, results = []) {
  if (!node || typeof node !== 'object') return results;
  if (predicate(node)) results.push(node);
  // Continue walking children even if this node matched (for nested cases)
  for (const child of getExprChildren(node)) {
    exprCollect(child, predicate, results);
  }
  return results;
}

/**
 * Transform an expression tree by applying a function to each node.
 * Returns a new tree (doesn't modify original).
 * 
 * @param {object} node - AST expression node
 * @param {function} transform - Called with (node). Return new node or null to keep original.
 * @returns {object} Transformed tree
 */
export function exprMap(node, transform) {
  if (!node || typeof node !== 'object') return node;
  const result = transform(node);
  if (result !== undefined && result !== null) return result;
  // Deep clone and recurse on children
  const clone = { ...node };
  if (clone.left) clone.left = exprMap(clone.left, transform);
  if (clone.right) clone.right = exprMap(clone.right, transform);
  if (clone.expr) clone.expr = exprMap(clone.expr, transform);
  if (clone.operand) clone.operand = exprMap(clone.operand, transform);
  if (clone.args) clone.args = clone.args.map(a => typeof a === 'object' ? exprMap(a, transform) : a);
  if (clone.whens) clone.whens = clone.whens.map(w => ({
    ...w,
    condition: w.condition ? exprMap(w.condition, transform) : w.condition,
    when: w.when ? exprMap(w.when, transform) : w.when,
    result: w.result ? exprMap(w.result, transform) : w.result,
    then: w.then ? exprMap(w.then, transform) : w.then,
  }));
  return clone;
}
