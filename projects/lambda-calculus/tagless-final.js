/**
 * Tagless Final: Type-safe embedded DSLs without AST
 * 
 * Instead of building an AST and interpreting it, define your
 * language as a type class. Different "interpreters" are different instances.
 */

// The "language" is defined by functions, not constructors
function evalInterp() {
  return {
    num: n => n,
    add: (a, b) => a + b,
    mul: (a, b) => a * b,
    neg: a => -a,
    bool: b => b,
    if_: (c, t, f) => c ? t() : f(),
    lam: f => f,
    app: (f, x) => f(x),
  };
}

function prettyInterp() {
  return {
    num: n => `${n}`,
    add: (a, b) => `(${a} + ${b})`,
    mul: (a, b) => `(${a} * ${b})`,
    neg: a => `(-${a})`,
    bool: b => `${b}`,
    if_: (c, t, f) => `(if ${c} then ${t()} else ${f()})`,
    lam: f => { const x = `x${_pp++}`; return `(λ${x}. ${f(x)})`; },
    app: (f, x) => `(${f} ${x})`,
  };
}
let _pp = 0;
function resetPP() { _pp = 0; }

function sizeInterp() {
  return {
    num: _ => 1,
    add: (a, b) => 1 + a + b,
    mul: (a, b) => 1 + a + b,
    neg: a => 1 + a,
    bool: _ => 1,
    if_: (c, t, f) => 1 + c + t() + f(),
    lam: f => 1 + f(0),
    app: (f, x) => 1 + f + x,
  };
}

// Programs are written ONCE, interpreted MANY ways
function example1(i) { return i.add(i.num(2), i.num(3)); }
function example2(i) { return i.mul(i.add(i.num(1), i.num(2)), i.num(4)); }
function example3(i) { return i.if_(i.bool(true), () => i.num(1), () => i.num(2)); }
function example4(i) { return i.app(i.lam(x => i.add(x, i.num(1))), i.num(41)); }

export { evalInterp, prettyInterp, sizeInterp, example1, example2, example3, example4, resetPP };
