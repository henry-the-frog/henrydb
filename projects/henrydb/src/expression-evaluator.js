// expression-evaluator.js — Evaluate SQL-like expressions
export function evaluate(expr, row) {
  if (typeof expr === 'number' || typeof expr === 'string') return expr;
  if (expr.type === 'column') return row[expr.name];
  if (expr.type === 'literal') return expr.value;
  if (expr.type === 'binary') {
    const l = evaluate(expr.left, row), r = evaluate(expr.right, row);
    switch (expr.op) {
      case '+': return l + r; case '-': return l - r;
      case '*': return l * r; case '/': return l / r;
      case '=': return l === r; case '!=': return l !== r;
      case '<': return l < r; case '>': return l > r;
      case '<=': return l <= r; case '>=': return l >= r;
      case 'AND': return l && r; case 'OR': return l || r;
    }
  }
  if (expr.type === 'function') {
    const args = expr.args.map(a => evaluate(a, row));
    switch (expr.name) {
      case 'UPPER': return String(args[0]).toUpperCase();
      case 'LOWER': return String(args[0]).toLowerCase();
      case 'LENGTH': return String(args[0]).length;
      case 'ABS': return Math.abs(args[0]);
      case 'ROUND': return Math.round(args[0]);
      case 'COALESCE': return args.find(a => a != null);
    }
  }
  return null;
}
