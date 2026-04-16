/**
 * Worker-Wrapper Transformation
 * 
 * Split a function into:
 * - Wrapper: handles boxing/unboxing at the interface
 * - Worker: uses unboxed values internally (faster!)
 * 
 * Before: f x = case x of { Just n → n + 1; Nothing → 0 }
 * After:  f x = case x of { Just n → f_worker n; Nothing → 0 }
 *         f_worker n# = n# + 1  (unboxed!)
 */

class Fun { constructor(name, params, body) { this.name = name; this.params = params; this.body = body; } }
class Var { constructor(name) { this.tag = 'Var'; this.name = name; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } }
class Call { constructor(fn, args) { this.tag = 'Call'; this.fn = fn; this.args = args; } }
class BinOp { constructor(op, l, r) { this.tag = 'BinOp'; this.op = op; this.left = l; this.right = r; } }
class Box { constructor(val) { this.tag = 'Box'; this.val = val; } }
class Unbox { constructor(val) { this.tag = 'Unbox'; this.val = val; } }

function workerWrapper(fn, strictParams = []) {
  // strictParams: indices of parameters that are always strict (can be unboxed)
  if (strictParams.length === 0) return { wrapper: fn, worker: null };
  
  const workerName = `${fn.name}_worker`;
  const workerParams = fn.params.map((p, i) => strictParams.includes(i) ? `${p}#` : p);
  
  // Worker: operates on unboxed values
  let workerBody = fn.body;
  for (const idx of strictParams) {
    workerBody = replaceVar(workerBody, fn.params[idx], new Var(workerParams[idx]));
  }
  const worker = new Fun(workerName, workerParams, workerBody);
  
  // Wrapper: unbox strict args, call worker
  const wrapperArgs = fn.params.map((p, i) => 
    strictParams.includes(i) ? new Unbox(new Var(p)) : new Var(p)
  );
  const wrapper = new Fun(fn.name, fn.params, new Call(workerName, wrapperArgs));
  
  return { wrapper, worker };
}

function replaceVar(expr, oldName, newExpr) {
  switch (expr.tag) {
    case 'Var': return expr.name === oldName ? newExpr : expr;
    case 'Num': return expr;
    case 'Call': return new Call(expr.fn, expr.args.map(a => replaceVar(a, oldName, newExpr)));
    case 'BinOp': return new BinOp(expr.op, replaceVar(expr.left, oldName, newExpr), replaceVar(expr.right, oldName, newExpr));
    case 'Box': return new Box(replaceVar(expr.val, oldName, newExpr));
    case 'Unbox': return new Unbox(replaceVar(expr.val, oldName, newExpr));
    default: return expr;
  }
}

function countBoxOps(expr) {
  let boxes = 0, unboxes = 0;
  function walk(e) {
    if (!e || !e.tag) return;
    if (e.tag === 'Box') { boxes++; walk(e.val); }
    else if (e.tag === 'Unbox') { unboxes++; walk(e.val); }
    else {
      for (const k of Object.keys(e)) {
        if (k !== 'tag' && e[k] && typeof e[k] === 'object') {
          if (Array.isArray(e[k])) e[k].forEach(walk);
          else if (e[k].tag) walk(e[k]);
        }
      }
    }
  }
  walk(expr);
  return { boxes, unboxes };
}

export { Fun, Var, Num, Call, BinOp, Box, Unbox, workerWrapper, countBoxOps };
