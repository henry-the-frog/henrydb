// type-inference/test.js — Comprehensive test suite
'use strict';

const { typeOf, TVar, TCon, TFun, TList, TPair, tInt, tBool, tString,
        Scheme, Subst, TypeEnv, unify, generalize, instantiate, 
        freeTypeVars, occurs, freshVar, resetFresh, infer, Parser } = require('./types.js');

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try {
    fn();
    passed++;
  } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

function eq(a, b, msg = '') {
  if (a !== b) throw new Error(`Expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}${msg ? ' — ' + msg : ''}`);
}

function throws(fn, msg) {
  try { fn(); throw new Error(`Expected error but succeeded`); }
  catch (e) { if (e.message === 'Expected error but succeeded') throw e; }
}

// ═══════════════════════════════════════════
// Type System Basics
// ═══════════════════════════════════════════
console.log('── Type System ──');

test('TVar toString', () => eq(new TVar('a').toString(), 'a'));
test('TCon toString', () => eq(new TCon('Int').toString(), 'Int'));
test('TFun toString', () => eq(new TFun(tInt, tBool).toString(), 'Int -> Bool'));
test('TFun nested toString', () => eq(new TFun(new TFun(tInt, tInt), tBool).toString(), '(Int -> Int) -> Bool'));
test('TFun right assoc', () => eq(new TFun(tInt, new TFun(tInt, tInt)).toString(), 'Int -> Int -> Int'));
test('TList toString', () => eq(new TList(tInt).toString(), '[Int]'));
test('TPair toString', () => eq(new TPair(tInt, tBool).toString(), '(Int, Bool)'));

// ═══════════════════════════════════════════
// Substitution
// ═══════════════════════════════════════════
console.log('── Substitution ──');

test('empty subst', () => {
  const s = Subst.empty();
  const t = new TVar('a');
  eq(s.apply(t).toString(), 'a');
});

test('single subst', () => {
  const s = Subst.single('a', tInt);
  eq(s.apply(new TVar('a')).toString(), 'Int');
});

test('subst through TFun', () => {
  const s = Subst.single('a', tInt);
  const t = new TFun(new TVar('a'), new TVar('b'));
  eq(s.apply(t).toString(), 'Int -> b');
});

test('subst composition', () => {
  const s1 = Subst.single('b', tBool);
  const s2 = Subst.single('a', new TVar('b'));
  const composed = s1.compose(s2);
  // Apply s2 first (a → b), then s1 (b → Bool)
  eq(composed.apply(new TVar('a')).toString(), 'Bool');
});

test('subst preserves scheme quantified vars', () => {
  const s = Subst.single('a', tInt);
  const scheme = new Scheme(['a'], new TFun(new TVar('a'), new TVar('a')));
  const result = s.applyScheme(scheme);
  eq(result.type.toString(), 'a -> a', 'quantified vars should not be substituted');
});

// ═══════════════════════════════════════════
// Unification
// ═══════════════════════════════════════════
console.log('── Unification ──');

test('unify same type', () => {
  const s = unify(tInt, tInt);
  eq(s.map.size, 0);
});

test('unify var with type', () => {
  const s = unify(new TVar('a'), tInt);
  eq(s.apply(new TVar('a')).toString(), 'Int');
});

test('unify functions', () => {
  const t1 = new TFun(new TVar('a'), tBool);
  const t2 = new TFun(tInt, new TVar('b'));
  const s = unify(t1, t2);
  eq(s.apply(new TVar('a')).toString(), 'Int');
  eq(s.apply(new TVar('b')).toString(), 'Bool');
});

test('unify lists', () => {
  const s = unify(new TList(new TVar('a')), new TList(tInt));
  eq(s.apply(new TVar('a')).toString(), 'Int');
});

test('unify pairs', () => {
  const s = unify(new TPair(new TVar('a'), new TVar('b')), new TPair(tInt, tBool));
  eq(s.apply(new TVar('a')).toString(), 'Int');
  eq(s.apply(new TVar('b')).toString(), 'Bool');
});

test('unify fails: Int vs Bool', () => {
  throws(() => unify(tInt, tBool));
});

test('unify fails: occurs check', () => {
  throws(() => unify(new TVar('a'), new TFun(new TVar('a'), tInt)));
});

test('occurs check detects', () => {
  eq(occurs('a', new TFun(new TVar('a'), tInt)), true);
  eq(occurs('a', new TFun(tInt, tInt)), false);
  eq(occurs('a', new TVar('a')), true);
  eq(occurs('a', new TVar('b')), false);
});

// ═══════════════════════════════════════════
// Free Type Variables
// ═══════════════════════════════════════════
console.log('── Free Variables ──');

test('freeTypeVars TVar', () => {
  const fv = freeTypeVars(new TVar('a'));
  eq(fv.has('a'), true);
  eq(fv.size, 1);
});

test('freeTypeVars TCon', () => {
  eq(freeTypeVars(tInt).size, 0);
});

test('freeTypeVars TFun', () => {
  const fv = freeTypeVars(new TFun(new TVar('a'), new TVar('b')));
  eq(fv.size, 2);
  eq(fv.has('a'), true);
  eq(fv.has('b'), true);
});

// ═══════════════════════════════════════════
// Generalize & Instantiate
// ═══════════════════════════════════════════
console.log('── Generalize & Instantiate ──');

test('generalize quantifies free vars', () => {
  const env = new TypeEnv();
  const scheme = generalize(env, new TFun(new TVar('a'), new TVar('a')));
  eq(scheme.vars.length, 1);
  eq(scheme.vars[0], 'a');
});

test('generalize respects env', () => {
  const env = new TypeEnv().extend('x', new Scheme([], new TVar('a')));
  const scheme = generalize(env, new TFun(new TVar('a'), new TVar('b')));
  // 'a' is free in env, so only 'b' is generalized
  eq(scheme.vars.length, 1);
  eq(scheme.vars[0], 'b');
});

test('instantiate creates fresh vars', () => {
  resetFresh();
  const scheme = new Scheme(['x'], new TFun(new TVar('x'), new TVar('x')));
  const t = instantiate(scheme);
  // Should be a -> a with a fresh variable
  eq(t instanceof TFun, true);
  eq(t.param.name, t.result.name);
});

// ═══════════════════════════════════════════
// Parser
// ═══════════════════════════════════════════
console.log('── Parser ──');

test('parse integer', () => {
  const ast = new Parser('42').parse();
  eq(ast.type, 'int');
  eq(ast.value, 42);
});

test('parse boolean', () => {
  eq(new Parser('true').parse().type, 'bool');
  eq(new Parser('false').parse().value, false);
});

test('parse variable', () => {
  eq(new Parser('x').parse().name, 'x');
});

test('parse lambda', () => {
  const ast = new Parser('\\x -> x').parse();
  eq(ast.type, 'lambda');
  eq(ast.param, 'x');
  eq(ast.body.name, 'x');
});

test('parse application', () => {
  const ast = new Parser('f x').parse();
  eq(ast.type, 'app');
  eq(ast.fn.name, 'f');
  eq(ast.arg.name, 'x');
});

test('parse let', () => {
  const ast = new Parser('let x = 1 in x').parse();
  eq(ast.type, 'let');
  eq(ast.name, 'x');
});

test('parse let rec', () => {
  const ast = new Parser('let rec f = \\x -> f x in f').parse();
  eq(ast.type, 'letrec');
  eq(ast.name, 'f');
});

test('parse if', () => {
  const ast = new Parser('if true then 1 else 2').parse();
  eq(ast.type, 'if');
});

test('parse arithmetic', () => {
  const ast = new Parser('1 + 2').parse();
  eq(ast.type, 'binop');
  eq(ast.op, '+');
});

test('parse list', () => {
  const ast = new Parser('[1, 2, 3]').parse();
  eq(ast.type, 'list');
  eq(ast.elems.length, 3);
});

test('parse empty list', () => {
  const ast = new Parser('[]').parse();
  eq(ast.type, 'list');
  eq(ast.elems.length, 0);
});

test('parse pair', () => {
  const ast = new Parser('(1, true)').parse();
  eq(ast.type, 'pair');
});

test('parse precedence', () => {
  const ast = new Parser('1 + 2 * 3').parse();
  eq(ast.type, 'binop');
  eq(ast.op, '+');
  eq(ast.right.op, '*');
});

// ═══════════════════════════════════════════
// Type Inference — Literals
// ═══════════════════════════════════════════
console.log('── Inference: Literals ──');

test('infer integer', () => eq(typeOf('42'), 'Int'));
test('infer boolean true', () => eq(typeOf('true'), 'Bool'));
test('infer boolean false', () => eq(typeOf('false'), 'Bool'));
test('infer string', () => eq(typeOf('"hello"'), 'String'));

// ═══════════════════════════════════════════
// Type Inference — Arithmetic
// ═══════════════════════════════════════════
console.log('── Inference: Arithmetic ──');

test('infer addition', () => eq(typeOf('1 + 2'), 'Int'));
test('infer subtraction', () => eq(typeOf('5 - 3'), 'Int'));
test('infer multiplication', () => eq(typeOf('2 * 3'), 'Int'));
test('infer division', () => eq(typeOf('10 / 3'), 'Int'));
test('infer modulo', () => eq(typeOf('10 % 3'), 'Int'));
test('infer comparison', () => eq(typeOf('1 < 2'), 'Bool'));
test('infer equality', () => eq(typeOf('1 == 2'), 'Bool'));
test('infer negation', () => eq(typeOf('let x = 5 in -x'), 'Int'));
test('infer not', () => eq(typeOf('not true'), 'Bool'));
test('infer string concat', () => eq(typeOf('"a" ++ "b"'), 'String'));

// ═══════════════════════════════════════════
// Type Inference — Lambda & Application
// ═══════════════════════════════════════════
console.log('── Inference: Lambda ──');

test('infer identity lambda', () => {
  const t = typeOf('\\x -> x');
  // Should be a -> a (with some fresh variable name)
  const m = t.match(/^(\w+) -> \1$/);
  eq(!!m, true, `Expected α -> α, got ${t}`);
});

test('infer const lambda', () => {
  const t = typeOf('\\x -> \\y -> x');
  // Should be a -> b -> a
  const parts = t.split(' -> ');
  eq(parts.length, 3);
  eq(parts[0], parts[2], `Expected α -> β -> α, got ${t}`);
});

test('infer application', () => {
  eq(typeOf('(\\x -> x + 1) 5'), 'Int');
});

test('infer higher-order function', () => {
  const t = typeOf('\\f -> \\x -> f x');
  // (a -> b) -> a -> b
  eq(t.includes('->'), true);
});

test('infer function applied to int', () => {
  eq(typeOf('(\\x -> x + 1) 42'), 'Int');
});

// ═══════════════════════════════════════════
// Type Inference — Let & Polymorphism
// ═══════════════════════════════════════════
console.log('── Inference: Let-Polymorphism ──');

test('infer simple let', () => {
  eq(typeOf('let x = 5 in x'), 'Int');
});

test('infer let with computation', () => {
  eq(typeOf('let x = 5 in x + 1'), 'Int');
});

test('infer let polymorphism: id', () => {
  // The classic test: id applied to both Int and Bool
  const t = typeOf('let id = \\x -> x in if id true then id 1 else 0');
  eq(t, 'Int');
});

test('infer let polymorphism: compose', () => {
  // compose should work with different types
  const t = typeOf('let compose = \\f -> \\g -> \\x -> f (g x) in compose (\\x -> x + 1) (\\x -> x * 2) 3');
  eq(t, 'Int');
});

test('infer nested let', () => {
  eq(typeOf('let x = 1 in let y = 2 in x + y'), 'Int');
});

test('let shadowing', () => {
  eq(typeOf('let x = 1 in let x = true in x'), 'Bool');
});

// ═══════════════════════════════════════════
// Type Inference — Recursion
// ═══════════════════════════════════════════
console.log('── Inference: Recursion ──');

test('infer recursive factorial', () => {
  eq(typeOf('let rec fact = \\n -> if n == 0 then 1 else n * fact (n - 1) in fact 5'), 'Int');
});

test('infer recursive fibonacci', () => {
  eq(typeOf('let rec fib = \\n -> if n < 2 then n else fib (n - 1) + fib (n - 2) in fib 10'), 'Int');
});

test('infer recursive sum', () => {
  eq(typeOf('let rec sum = \\n -> if n == 0 then 0 else n + sum (n - 1) in sum'), 'Int -> Int');
});

// ═══════════════════════════════════════════
// Type Inference — If
// ═══════════════════════════════════════════
console.log('── Inference: If ──');

test('infer if expression', () => {
  eq(typeOf('if true then 1 else 2'), 'Int');
});

test('infer if with bool result', () => {
  eq(typeOf('if true then false else true'), 'Bool');
});

test('infer if with comparison', () => {
  eq(typeOf('if 1 < 2 then 42 else 0'), 'Int');
});

// ═══════════════════════════════════════════
// Type Inference — Lists
// ═══════════════════════════════════════════
console.log('── Inference: Lists ──');

test('infer int list', () => eq(typeOf('[1, 2, 3]'), '[Int]'));
test('infer bool list', () => eq(typeOf('[true, false]'), '[Bool]'));
test('infer empty list', () => {
  const t = typeOf('[]');
  eq(t.startsWith('['), true);
  eq(t.endsWith(']'), true);
});

test('infer head', () => eq(typeOf('head [1, 2, 3]'), 'Int'));
test('infer tail', () => eq(typeOf('tail [1, 2, 3]'), '[Int]'));
test('infer cons', () => eq(typeOf('cons 1 [2, 3]'), '[Int]'));
test('infer null check', () => eq(typeOf('null []'), 'Bool'));
test('infer length', () => eq(typeOf('length [1, 2, 3]'), 'Int'));

test('infer list in let', () => {
  eq(typeOf('let xs = [1, 2, 3] in head xs'), 'Int');
});

// ═══════════════════════════════════════════
// Type Inference — Pairs
// ═══════════════════════════════════════════
console.log('── Inference: Pairs ──');

test('infer pair', () => eq(typeOf('(1, true)'), '(Int, Bool)'));
test('infer fst', () => eq(typeOf('fst (1, true)'), 'Int'));
test('infer snd', () => eq(typeOf('snd (1, true)'), 'Bool'));
test('infer nested pair', () => eq(typeOf('(1, (2, 3))'), '(Int, (Int, Int))'));

test('infer pair in let', () => {
  eq(typeOf('let p = (42, true) in fst p'), 'Int');
});

// ═══════════════════════════════════════════
// Type Errors
// ═══════════════════════════════════════════
console.log('── Type Errors ──');

test('error: if branches disagree', () => {
  throws(() => typeOf('if true then 1 else false'));
});

test('error: applying non-function', () => {
  throws(() => typeOf('1 2'));
});

test('error: adding bool', () => {
  throws(() => typeOf('true + 1'));
});

test('error: unbound variable', () => {
  throws(() => typeOf('x'));
});

test('error: if condition not bool', () => {
  throws(() => typeOf('if 42 then 1 else 2'));
});

test('error: heterogeneous list', () => {
  throws(() => typeOf('[1, true]'));
});

test('error: infinite type', () => {
  // \x -> x x causes infinite type
  throws(() => typeOf('\\x -> x x'));
});

test('error: not on int', () => {
  throws(() => typeOf('not 42'));
});

// ═══════════════════════════════════════════
// Complex Programs
// ═══════════════════════════════════════════
console.log('── Complex Programs ──');

test('map function type', () => {
  const t = typeOf('let rec map = \\f -> \\xs -> if null xs then [] else cons (f (head xs)) (map f (tail xs)) in map');
  // Should be (a -> b) -> [a] -> [b]
  eq(t.includes('->'), true);
  eq(t.includes('['), true);
});

test('map applied', () => {
  eq(typeOf('let rec map = \\f -> \\xs -> if null xs then [] else cons (f (head xs)) (map f (tail xs)) in map (\\x -> x + 1) [1, 2, 3]'), '[Int]');
});

test('filter type', () => {
  const t = typeOf('let rec filter = \\f -> \\xs -> if null xs then [] else if f (head xs) then cons (head xs) (filter f (tail xs)) else filter f (tail xs) in filter');
  eq(t.includes('->'), true);
  eq(t.includes('Bool'), true);
});

test('fold type', () => {
  const t = typeOf('let rec fold = \\f -> \\acc -> \\xs -> if null xs then acc else fold f (f acc (head xs)) (tail xs) in fold');
  eq(t.includes('->'), true);
});

test('fold sum', () => {
  eq(typeOf('let rec fold = \\f -> \\acc -> \\xs -> if null xs then acc else fold f (f acc (head xs)) (tail xs) in fold (\\a -> \\b -> a + b) 0 [1, 2, 3]'), 'Int');
});

test('church numerals', () => {
  // Church zero: \f -> \x -> x
  // Church succ: \n -> \f -> \x -> f (n f x)
  const t = typeOf('let zero = \\f -> \\x -> x in let succ = \\n -> \\f -> \\x -> f (n f x) in succ zero');
  eq(t.includes('->'), true);
});

test('polymorphic identity used twice', () => {
  // The KEY test for let-polymorphism:
  // Without polymorphism, id would be monomorphic and this would fail
  eq(typeOf('let id = \\x -> x in (id 42, id true)'), '(Int, Bool)');
});

test('K combinator', () => {
  const t = typeOf('\\x -> \\y -> x');
  const parts = t.split(' -> ');
  eq(parts.length, 3);
  eq(parts[0], parts[2]);
});

test('S combinator type', () => {
  // S = \f -> \g -> \x -> f x (g x)
  const t = typeOf('\\f -> \\g -> \\x -> f x (g x)');
  eq(t.includes('->'), true);
});

// ═══════════════════════════════════════════

console.log(`\n══════════════════════════════`);
console.log(`  ${passed}/${total} passed, ${failed} failed`);
console.log(`══════════════════════════════`);
process.exit(failed > 0 ? 1 : 0);
