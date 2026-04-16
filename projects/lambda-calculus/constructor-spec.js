/**
 * Constructor Specialization
 * 
 * When a function always scrutinizes its argument,
 * create specialized versions for each constructor.
 * 
 * f (Just x) = x + 1; f Nothing = 0
 * →
 * f_Just x = x + 1  (no pattern match overhead!)
 * f_Nothing = 0
 */

class Fun { constructor(name, param, cases) { this.name = name; this.param = param; this.cases = cases; } }
class Case { constructor(con, vars, body) { this.con = con; this.vars = vars; this.body = body; } }
class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class Con { constructor(name, args) { this.tag = 'Con'; this.name = name; this.args = args; } toString() { return `${this.name}(${this.args.join(',')})`; } }
class Call { constructor(fn, arg) { this.tag = 'Call'; this.fn = fn; this.arg = arg; } toString() { return `${this.fn}(${this.arg})`; } }
class BinOp { constructor(op, l, r) { this.tag = 'BinOp'; this.op = op; this.left = l; this.right = r; } }

function specialize(fn) {
  const specialized = [];
  
  for (const c of fn.cases) {
    const specName = `${fn.name}_${c.con}`;
    specialized.push({
      name: specName,
      params: c.vars,
      body: c.body,
      constructor: c.con
    });
  }
  
  // Wrapper that dispatches to specialized versions
  const wrapper = {
    name: fn.name,
    param: fn.param,
    dispatch: specialized.map(s => ({
      con: s.constructor,
      call: { fn: s.name, vars: s.params }
    }))
  };
  
  return { specialized, wrapper };
}

function callSiteTransform(callExpr, specializedMap) {
  if (callExpr.tag !== 'Call') return callExpr;
  
  // If argument is a known constructor, call specialized version directly
  if (callExpr.arg.tag === 'Con') {
    const specName = `${callExpr.fn}_${callExpr.arg.name}`;
    if (specializedMap.has(specName)) {
      return { tag: 'Call', fn: specName, args: callExpr.arg.args };
    }
  }
  
  return callExpr;
}

function estimateSaving(fn) {
  const savings = fn.cases.map(c => ({
    constructor: c.con,
    eliminatedPatternMatch: true,
    bodySize: countNodes(c.body)
  }));
  return { totalCases: fn.cases.length, savings };
}

function countNodes(expr) {
  if (!expr || typeof expr !== 'object') return 0;
  switch (expr.tag) {
    case 'Var': case 'Num': return 1;
    case 'BinOp': return 1 + countNodes(expr.left) + countNodes(expr.right);
    case 'Call': return 1;
    case 'Con': return 1 + expr.args.length;
    default: return 1;
  }
}

export { Fun, Case, Var, Num, Con, Call, BinOp, specialize, callSiteTransform, estimateSaving };
