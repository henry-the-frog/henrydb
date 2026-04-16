import { strict as assert } from 'assert';
import {
  TVar, TFun, TCon, tInt, tBool, tStr,
  Scheme, Subst,
  evar, elam, eapp, elet, eint, ebool, estr,
  unify, ftv, occursIn,
  infer, generalize, instantiate,
  resetFresh
} from './hindley-milner.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// ============================================================
// Unification
// ============================================================

test('unify: same type', () => {
  const s = unify(tInt, tInt);
  assert.equal(s.map.size, 0);
});

test('unify: var with concrete', () => {
  const s = unify(new TVar('a'), tInt);
  assert.equal(s.apply(new TVar('a')).toString(), 'Int');
});

test('unify: function types', () => {
  const s = unify(new TFun(new TVar('a'), tBool), new TFun(tInt, new TVar('b')));
  assert.equal(s.apply(new TVar('a')).toString(), 'Int');
  assert.equal(s.apply(new TVar('b')).toString(), 'Bool');
});

test('unify: fails for different base types', () => {
  assert.throws(() => unify(tInt, tBool), /Cannot unify/);
});

test('unify: occurs check', () => {
  assert.throws(() => unify(new TVar('a'), new TFun(new TVar('a'), tInt)), /Infinite/);
});

// ============================================================
// Free type variables
// ============================================================

test('ftv: base type has no free vars', () => {
  assert.equal(ftv(tInt).size, 0);
});

test('ftv: type var has itself', () => {
  assert.ok(ftv(new TVar('a')).has('a'));
});

test('ftv: function type', () => {
  const ftvs = ftv(new TFun(new TVar('a'), new TVar('b')));
  assert.ok(ftvs.has('a'));
  assert.ok(ftvs.has('b'));
});

// ============================================================
// Type inference: literals
// ============================================================

test('infer: integer literal', () => {
  const { type } = infer(eint);
  assert.equal(type.toString(), 'Int');
});

test('infer: boolean literal', () => {
  const { type } = infer(ebool);
  assert.equal(type.toString(), 'Bool');
});

// ============================================================
// Type inference: lambda
// ============================================================

test('infer: identity function λx.x', () => {
  const { subst, type } = infer(elam('x', evar('x')));
  const resolved = subst.apply(type);
  // Should be a → a (same type var for param and return)
  assert.equal(resolved.tag, 'TFun');
  assert.equal(resolved.param.toString(), resolved.ret.toString());
});

test('infer: constant function λx.42', () => {
  const { subst, type } = infer(elam('x', eint));
  const resolved = subst.apply(type);
  assert.equal(resolved.tag, 'TFun');
  assert.equal(resolved.ret.toString(), 'Int');
});

// ============================================================
// Type inference: application
// ============================================================

test('infer: (λx.x) 42', () => {
  const { subst, type } = infer(eapp(elam('x', evar('x')), eint));
  assert.equal(subst.apply(type).toString(), 'Int');
});

test('infer: (λx.x) true', () => {
  const { subst, type } = infer(eapp(elam('x', evar('x')), ebool));
  assert.equal(subst.apply(type).toString(), 'Bool');
});

// ============================================================
// Type inference: let polymorphism
// ============================================================

test('let-polymorphism: let id = λx.x in (id 42, id true)', () => {
  // let id = λx.x in id 42
  const { subst, type } = infer(elet('id', elam('x', evar('x')), eapp(evar('id'), eint)));
  assert.equal(subst.apply(type).toString(), 'Int');
});

test('let-polymorphism: id used at different types', () => {
  // let id = λx.x in let a = id 42 in id true
  const { subst, type } = infer(
    elet('id', elam('x', evar('x')),
      elet('a', eapp(evar('id'), eint),
        eapp(evar('id'), ebool))));
  assert.equal(subst.apply(type).toString(), 'Bool');
});

test('let-polymorphism: const function', () => {
  // let const = λx.λy.x in const 42 true = Int
  const { subst, type } = infer(
    elet('const', elam('x', elam('y', evar('x'))),
      eapp(eapp(evar('const'), eint), ebool)));
  assert.equal(subst.apply(type).toString(), 'Int');
});

// ============================================================
// Type inference: errors
// ============================================================

test('unbound variable', () => {
  assert.throws(() => infer(evar('undefined')), /Unbound/);
});

// ============================================================
// Generalize and instantiate
// ============================================================

test('generalize: no env ftv → all generalized', () => {
  const env = new Map();
  const scheme = generalize(env, new TFun(new TVar('a'), new TVar('a')));
  assert.ok(scheme.vars.includes('a'));
});

test('instantiate: creates fresh vars', () => {
  resetFresh();
  const scheme = new Scheme(['a'], new TFun(new TVar('a'), new TVar('a')));
  const t1 = instantiate(scheme);
  const t2 = instantiate(scheme);
  // Two instantiations should produce different variables
  assert.notEqual(t1.param.name, t2.param.name);
});

// ============================================================
// Substitution composition
// ============================================================

test('substitution composition', () => {
  const s1 = new Subst(new Map([['a', tInt]]));
  const s2 = new Subst(new Map([['b', new TVar('a')]]));
  const composed = s1.compose(s2);
  assert.equal(composed.apply(new TVar('b')).toString(), 'Int');
});

// ============================================================
// Stress tests: complex programs
// ============================================================

test('compose: (λf.λg.λx. f(g(x)))', () => {
  // compose : (b → c) → (a → b) → a → c
  const compose = elam('f', elam('g', elam('x', eapp(evar('f'), eapp(evar('g'), evar('x'))))));
  const { subst, type } = infer(compose);
  const resolved = subst.apply(type);
  assert.equal(resolved.tag, 'TFun');
  // Should be (t1 → t2) → (t3 → t1) → t3 → t2
});

test('flip: λf.λx.λy. f y x', () => {
  const flip = elam('f', elam('x', elam('y', eapp(eapp(evar('f'), evar('y')), evar('x')))));
  const { subst, type } = infer(flip);
  const resolved = subst.apply(type);
  assert.equal(resolved.tag, 'TFun');
});

test('church numerals type: λf.λx. f(f(x))', () => {
  const two = elam('f', elam('x', eapp(evar('f'), eapp(evar('f'), evar('x')))));
  const { subst, type } = infer(two);
  const resolved = subst.apply(type);
  // (a → a) → a → a
  assert.equal(resolved.tag, 'TFun');
  assert.equal(resolved.param.tag, 'TFun');
  // param and return of the inner function should match
});

test('nested let: let x = 1 in let y = 2 in x', () => {
  const { subst, type } = infer(elet('x', eint, elet('y', ebool, evar('x'))));
  assert.equal(subst.apply(type).toString(), 'Int');
});

test('let with function: let f = λx.x in let g = λy. f y in g 42', () => {
  const prog = elet('f', elam('x', evar('x')),
    elet('g', elam('y', eapp(evar('f'), evar('y'))),
      eapp(evar('g'), eint)));
  const { subst, type } = infer(prog);
  assert.equal(subst.apply(type).toString(), 'Int');
});

test('apply: λf.λx. f x', () => {
  const apply = elam('f', elam('x', eapp(evar('f'), evar('x'))));
  const { subst, type } = infer(apply);
  const resolved = subst.apply(type);
  // (a → b) → a → b
  assert.equal(resolved.tag, 'TFun');
  assert.equal(resolved.param.tag, 'TFun');
});

test('S combinator: λf.λg.λx. f x (g x)', () => {
  const S = elam('f', elam('g', elam('x',
    eapp(eapp(evar('f'), evar('x')), eapp(evar('g'), evar('x'))))));
  const { subst, type } = infer(S);
  const resolved = subst.apply(type);
  assert.equal(resolved.tag, 'TFun');
});

test('K combinator: λx.λy. x', () => {
  const K = elam('x', elam('y', evar('x')));
  const { subst, type } = infer(K);
  const resolved = subst.apply(type);
  assert.equal(resolved.tag, 'TFun');
  // a → b → a
});

test('omega: cannot type λx. x x (infinite type)', () => {
  const omega = elam('x', eapp(evar('x'), evar('x')));
  assert.throws(() => infer(omega), /Infinite|occurs/);
});

// ============================================================
// Report
// ============================================================

console.log(`\nHindley-Milner tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
