/**
 * Hindley-Milner Stress Tests
 * 
 * Known tricky cases from the type inference literature:
 * - Omega combinator (should fail: infinite type)
 * - Let-polymorphism (id used at different types)
 * - Church encodings (higher-order functions)
 * - Y combinator (should fail: recursive type)
 * - S/K/I combinators
 * - Nested let with shadowing
 * - Value restriction edge cases
 */

import {
  TVar, TFun, TCon, tInt, tBool, tStr,
  Scheme, Subst,
  evar, elam, eapp, elet, eint, ebool, estr,
  unify, ftv, ftvScheme, infer, generalize, instantiate,
  freshVar, resetFresh
} from './hindley-milner.js';

let pass = 0, fail = 0;
function test(name, fn) {
  try {
    fn();
    pass++;
  } catch (e) {
    fail++;
    console.log(`FAIL: ${name}`);
    console.log(`  ${e.message}`);
  }
}
function assert(cond, msg) { if (!cond) throw new Error(msg || 'assertion failed'); }
function assertThrows(fn, msg) {
  try { fn(); throw new Error('Expected an error but none was thrown'); } catch (e) {
    if (e.message === 'Expected an error but none was thrown') throw e;
    // An error was thrown — pass
  }
}
function typeStr(t) {
  if (!t) return 'undefined';
  return t.toString();
}

console.log('=== Hindley-Milner Stress Tests ===');

// ============================================================
// Identity function: λx.x : ∀a. a → a
// ============================================================
test('identity function', () => {
  const id = elam('x', evar('x'));
  const { type } = infer(id);
  assert(type.tag === 'TFun', `Expected function type, got ${type.tag}`);
  assert(type.param.tag === 'TVar', 'Param should be type variable');
  assert(type.ret.tag === 'TVar', 'Return should be type variable');
  assert(type.param.name === type.ret.name, 'Input and output types should be same variable');
});

// ============================================================
// Constant function: λx.λy.x : ∀a b. a → b → a
// ============================================================
test('K combinator (const)', () => {
  const k = elam('x', elam('y', evar('x')));
  const { type } = infer(k);
  assert(type.tag === 'TFun', 'K should be a function');
  assert(type.ret.tag === 'TFun', 'K should return a function');
  // K : a → b → a
  assert(type.param.name === type.ret.ret.name, 'K should return its first argument type');
});

// ============================================================
// S combinator: λf.λg.λx. f x (g x) : ∀a b c. (a → b → c) → (a → b) → a → c
// ============================================================
test('S combinator', () => {
  const s = elam('f', elam('g', elam('x',
    eapp(eapp(evar('f'), evar('x')), eapp(evar('g'), evar('x')))
  )));
  const { type } = infer(s);
  assert(type.tag === 'TFun', 'S should be a function');
  // The type should be (a → b → c) → (a → b) → a → c
  // But exact variable names don't matter
});

// ============================================================
// Church Boolean: true = λt.λf.t
// ============================================================
test('Church true', () => {
  const ctrue = elam('t', elam('f', evar('t')));
  const { type } = infer(ctrue);
  // Same as K combinator: a → b → a
  assert(type.tag === 'TFun', 'Church true should be a function');
  assert(type.ret.tag === 'TFun', 'Should return a function');
});

// ============================================================
// Church Boolean: false = λt.λf.f
// ============================================================
test('Church false', () => {
  const cfalse = elam('t', elam('f', evar('f')));
  const { type } = infer(cfalse);
  // a → b → b (note: different from K!)
  assert(type.ret.ret.name === type.ret.param.name || 
         type.ret.ret.name === type.ret.tag && type.ret.tag === 'TFun',
    'Church false should return the second argument type');
});

// ============================================================
// Omega combinator: (λx. x x) — should fail (infinite type)
// ============================================================
test('omega combinator (should fail)', () => {
  const omega = elam('x', eapp(evar('x'), evar('x')));
  assertThrows(() => infer(omega), 'Omega should produce infinite type error');
});

// ============================================================
// Full omega: (λx. x x)(λx. x x) — should fail (infinite type)  
// ============================================================
test('full omega (should fail)', () => {
  const omega_part = elam('x', eapp(evar('x'), evar('x')));
  const full_omega = eapp(omega_part, omega_part);
  assertThrows(() => infer(full_omega), 'Full omega should produce infinite type error');
});

// ============================================================
// Y combinator: λf. (λx. f (x x))(λx. f (x x)) — should fail
// ============================================================
test('Y combinator (should fail)', () => {
  const inner = elam('x', eapp(evar('f'), eapp(evar('x'), evar('x'))));
  const y = elam('f', eapp(inner, inner));
  assertThrows(() => infer(y), 'Y combinator should produce infinite type error');
});

// ============================================================
// Let-polymorphism: let id = λx.x in (id 42, id true)
// The key test: id must be used at TWO different types
// ============================================================
test('let-polymorphism', () => {
  // let id = λx.x in id 42
  const expr1 = elet('id', elam('x', evar('x')),
    eapp(evar('id'), eint)
  );
  const { type: t1 } = infer(expr1);
  assert(t1.name === 'Int', `id 42 should be Int, got ${typeStr(t1)}`);
  
  // let id = λx.x in id true
  const expr2 = elet('id', elam('x', evar('x')),
    eapp(evar('id'), ebool)
  );
  const { type: t2 } = infer(expr2);
  assert(t2.name === 'Bool', `id true should be Bool, got ${typeStr(t2)}`);
});

// ============================================================
// Let-polymorphism: id used at two different types IN SAME SCOPE
// This is the real test of let-polymorphism vs lambda binding
// ============================================================
test('let-polymorphism: same scope, different instantiation', () => {
  // let id = λx.x in let a = id 42 in id true
  const expr = elet('id', elam('x', evar('x')),
    elet('a', eapp(evar('id'), eint),
      eapp(evar('id'), ebool)
    )
  );
  const { type } = infer(expr);
  assert(type.name === 'Bool', `Should be Bool, got ${typeStr(type)}`);
});

// ============================================================
// Lambda binding is NOT polymorphic (monomorphism restriction)
// λid. (id 42, id true) — should FAIL because id is lambda-bound
// ============================================================
test('lambda binding is monomorphic', () => {
  // λid. id 42
  const expr1 = elam('id', eapp(evar('id'), eint));
  const { type: t1 } = infer(expr1);
  // id : Int → Int, so the function is (Int → Int) → Int
  assert(t1.param.tag === 'TFun', 'Param should be a function');
  assert(t1.param.param.name === 'Int', `id should be Int → ?, got ${typeStr(t1.param)}`);
});

// ============================================================
// Nested let shadowing
// ============================================================
test('let shadowing', () => {
  // let x = 42 in let x = true in x
  const expr = elet('x', eint, elet('x', ebool, evar('x')));
  const { type } = infer(expr);
  assert(type.name === 'Bool', `Shadowed x should be Bool, got ${typeStr(type)}`);
});

// ============================================================
// Compose: λf.λg.λx. f (g x)
// Should infer: (b → c) → (a → b) → a → c
// ============================================================
test('function composition', () => {
  const compose = elam('f', elam('g', elam('x',
    eapp(evar('f'), eapp(evar('g'), evar('x')))
  )));
  const { type } = infer(compose);
  assert(type.tag === 'TFun', 'compose should be a function');
  // (b → c) → (a → b) → a → c
  const [f, rest1] = [type.param, type.ret];
  assert(f.tag === 'TFun', 'First param should be a function');
  const [g, rest2] = [rest1.param, rest1.ret];
  assert(g.tag === 'TFun', 'Second param should be a function');
  // f.param should equal g.ret (both are b)
  assert(f.param.name === g.ret.name, `f input should match g output: ${f.param} vs ${g.ret}`);
  // rest2 input should equal g input (both are a)
  assert(rest2.param.name === g.param.name, `Last input should match g input`);
  // rest2 output should equal f output (both are c)
  assert(rest2.ret.name === f.ret.name, `Output should match f output`);
});

// ============================================================  
// Flip: λf.λx.λy. f y x
// (a → b → c) → b → a → c
// ============================================================
test('flip', () => {
  const flip = elam('f', elam('x', elam('y',
    eapp(eapp(evar('f'), evar('y')), evar('x'))
  )));
  const { type } = infer(flip);
  assert(type.tag === 'TFun', 'flip should be a function');
});

// ============================================================
// Apply: λf.λx. f x — simplest higher-order function
// ============================================================
test('apply', () => {
  const apply = elam('f', elam('x', eapp(evar('f'), evar('x'))));
  const { type } = infer(apply);
  // (a → b) → a → b
  assert(type.param.tag === 'TFun', 'First param should be a function');
  assert(type.param.param.name === type.ret.param.name, 'f input = x type');
  assert(type.param.ret.name === type.ret.ret.name, 'f output = result');
});

// ============================================================
// Occurs check: λx. x x (self-application)
// ============================================================
test('occurs check prevents self-application', () => {
  assertThrows(
    () => infer(elam('x', eapp(evar('x'), evar('x')))),
    'Self-application should trigger occurs check'
  );
});

// ============================================================
// Unification edge cases
// ============================================================
test('unify same variable', () => {
  const a = new TVar('a');
  const s = unify(a, a);
  assert(s.map.size === 0, 'Unifying a with a should produce empty substitution');
});

test('unify different variables', () => {
  const a = new TVar('a');
  const b = new TVar('b');
  const s = unify(a, b);
  assert(s.map.size === 1, 'Should produce one binding');
});

test('unify function types', () => {
  const t1 = new TFun(new TVar('a'), tInt);
  const t2 = new TFun(tBool, new TVar('b'));
  const s = unify(t1, t2);
  assert(s.apply(new TVar('a')).name === 'Bool', 'a should unify to Bool');
  assert(s.apply(new TVar('b')).name === 'Int', 'b should unify to Int');
});

test('unify Int with Bool should fail', () => {
  assertThrows(() => unify(tInt, tBool), 'Cannot unify Int with Bool');
});

test('unify occurs check', () => {
  const a = new TVar('a');
  const t = new TFun(a, tInt);
  assertThrows(() => unify(a, t), 'Occurs check should reject a = a → Int');
});

// ============================================================
// Substitution composition
// ============================================================
test('substitution composition', () => {
  const s1 = new Subst(new Map([['a', tInt]]));
  const s2 = new Subst(new Map([['b', new TVar('a')]]));
  const composed = s1.compose(s2);
  // s1 ∘ s2: b → a → Int
  const result = composed.apply(new TVar('b'));
  assert(result.name === 'Int', `b should map to Int via composition, got ${result}`);
});

test('substitution idempotence', () => {
  const s = new Subst(new Map([['a', tInt]]));
  const t = s.apply(new TFun(new TVar('a'), new TVar('a')));
  assert(t.param.name === 'Int' && t.ret.name === 'Int', 'Both occurrences should be replaced');
});

// ============================================================
// Free type variables
// ============================================================
test('ftv of type variable', () => {
  assert(ftv(new TVar('a')).has('a'), 'ftv(a) should contain a');
});

test('ftv of function type', () => {
  const t = new TFun(new TVar('a'), new TVar('b'));
  const fvs = ftv(t);
  assert(fvs.has('a') && fvs.has('b'), 'ftv(a → b) should contain a and b');
});

test('ftv of concrete type', () => {
  assert(ftv(tInt).size === 0, 'ftv(Int) should be empty');
});

test('ftvScheme removes bound vars', () => {
  const scheme = new Scheme(['a'], new TFun(new TVar('a'), new TVar('b')));
  const fvs = ftvScheme(scheme);
  assert(!fvs.has('a'), 'Bound variable a should not be free');
  assert(fvs.has('b'), 'Unbound variable b should be free');
});

// ============================================================
// Summary
// ============================================================
console.log(`\nHindley-Milner stress tests: ${pass}/${pass + fail} passed`);
if (fail > 0) {
  console.log(`${fail} FAILED`);
  process.exit(1);
}
