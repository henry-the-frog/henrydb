/**
 * Parametric HOAS: Higher-Order Abstract Syntax with type safety
 */
class HOAS {
  static lam(f) { return { tag: 'Lam', body: f }; }
  static app(fn, arg) { return { tag: 'App', fn, arg }; }
  static num(n) { return { tag: 'Num', n }; }
  static add(l, r) { return { tag: 'Add', left: l, right: r }; }
  static let_(init, body) { return { tag: 'Let', init, body }; }
}

function evalHOAS(expr) {
  switch(expr.tag) {
    case 'Num': return expr.n;
    case 'Add': return evalHOAS(expr.left) + evalHOAS(expr.right);
    case 'Lam': return arg => evalHOAS(expr.body({ tag: 'Num', n: arg }));
    case 'App': return evalHOAS(expr.fn)(evalHOAS(expr.arg));
    case 'Let': return evalHOAS(expr.body({ tag: 'Num', n: evalHOAS(expr.init) }));
  }
}

function sizeHOAS(expr, depth = 0) {
  switch(expr.tag) {
    case 'Num': return 1;
    case 'Add': return 1 + sizeHOAS(expr.left) + sizeHOAS(expr.right);
    case 'Lam': return 1 + sizeHOAS(expr.body({ tag: 'Num', n: 0 }));
    case 'App': return 1 + sizeHOAS(expr.fn) + sizeHOAS(expr.arg);
    case 'Let': return 1 + sizeHOAS(expr.init) + sizeHOAS(expr.body({ tag: 'Num', n: 0 }));
  }
}

export { HOAS, evalHOAS, sizeHOAS };
