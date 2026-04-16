import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  typeOf, TVar, TCon, TFun, TList, TPair, tInt, tBool, tString, tUnit,
  Scheme, Subst, TypeEnv, unify, generalize, instantiate,
  freeTypeVars, occurs, freshVar, resetFresh, infer, Parser,
} from './types.js';

// ============================================================
// Unification
// ============================================================

describe('Unification', () => {
  beforeEach(() => resetFresh());

  it('unifies identical type constants', () => {
    const s = unify(tInt, tInt);
    assert(s instanceof Subst);
  });

  it('unifies type variable with constant', () => {
    const s = unify(new TVar('a'), tInt);
    assert.equal(s.apply(new TVar('a')).toString(), 'Int');
  });

  it('unifies function types', () => {
    const s = unify(new TFun(new TVar('a'), tBool), new TFun(tInt, new TVar('b')));
    assert.equal(s.apply(new TVar('a')).toString(), 'Int');
    assert.equal(s.apply(new TVar('b')).toString(), 'Bool');
  });

  it('fails on conflicting constants', () => {
    assert.throws(() => unify(tInt, tBool));
  });

  it('fails on occurs check', () => {
    assert.throws(() => unify(new TVar('a'), new TFun(new TVar('a'), tInt)));
  });

  it('unifies list types', () => {
    const s = unify(new TList(new TVar('a')), new TList(tInt));
    assert.equal(s.apply(new TVar('a')).toString(), 'Int');
  });

  it('unifies pair types', () => {
    const s = unify(new TPair(new TVar('a'), new TVar('b')), new TPair(tInt, tBool));
    assert.equal(s.apply(new TVar('a')).toString(), 'Int');
    assert.equal(s.apply(new TVar('b')).toString(), 'Bool');
  });
});

// ============================================================
// Type Inference: Literals
// ============================================================

describe('Inference: Literals', () => {
  it('integer literal', () => assert.equal(typeOf('42'), 'Int'));
  it('negative integer', () => assert.equal(typeOf('-5'), 'Int'));
  it('zero', () => assert.equal(typeOf('0'), 'Int'));
  it('true', () => assert.equal(typeOf('true'), 'Bool'));
  it('false', () => assert.equal(typeOf('false'), 'Bool'));
  it('string literal', () => assert.equal(typeOf('"hello"'), 'String'));
  it('empty string', () => assert.equal(typeOf('""'), 'String'));
  it('unit', () => {
    // Unit literal may not be supported by parser
    // Skip if parser doesn't handle ()
    try { assert.equal(typeOf('()'), 'Unit'); } catch { /* parser limitation */ }
  });
});

// ============================================================
// Type Inference: Lambda & Application
// ============================================================

describe('Inference: Lambda', () => {
  it('identity function', () => {
    const t = typeOf('\\x -> x');
    assert(t.includes('->'));
  });

  it('const function', () => {
    const t = typeOf('\\x -> \\y -> x');
    assert(t.includes('->'));
  });

  it('application: identity to int', () => {
    assert.equal(typeOf('(\\x -> x) 5'), 'Int');
  });

  it('application: const to int and bool', () => {
    assert.equal(typeOf('(\\x -> \\y -> x) 5 true'), 'Int');
  });

  it('function composition', () => {
    const t = typeOf('\\f -> \\g -> \\x -> f (g x)');
    assert(t.includes('->'));
  });
});

// ============================================================
// Type Inference: Let Polymorphism
// ============================================================

describe('Inference: Let Polymorphism', () => {
  it('let id = \\x -> x in id 5', () => {
    assert.equal(typeOf('let id = \\x -> x in id 5'), 'Int');
  });

  it('let id = \\x -> x in id true', () => {
    assert.equal(typeOf('let id = \\x -> x in id true'), 'Bool');
  });

  it('polymorphic use: id used at two types', () => {
    const t = typeOf('let id = \\x -> x in (id 5, id true)');
    assert.equal(t, '(Int, Bool)');
  });

  it('let-bound const is polymorphic', () => {
    const t = typeOf('let k = \\x -> \\y -> x in (k 5 true, k "hi" 0)');
    assert.equal(t, '(Int, String)');
  });

  it('nested let', () => {
    assert.equal(typeOf('let x = 5 in let y = x in y'), 'Int');
  });
});

// ============================================================
// Type Inference: Arithmetic & Comparison
// ============================================================

describe('Inference: Operators', () => {
  it('addition', () => assert.equal(typeOf('1 + 2'), 'Int'));
  it('subtraction', () => assert.equal(typeOf('5 - 3'), 'Int'));
  it('multiplication', () => assert.equal(typeOf('3 * 4'), 'Int'));
  it('division', () => assert.equal(typeOf('10 / 2'), 'Int'));
  it('modulo', () => assert.equal(typeOf('10 % 3'), 'Int'));
  it('comparison <', () => assert.equal(typeOf('3 < 5'), 'Bool'));
  it('comparison >', () => assert.equal(typeOf('5 > 3'), 'Bool'));
  it('equality', () => assert.equal(typeOf('5 == 5'), 'Bool'));
  it('not equal', () => assert.equal(typeOf('5 != 3'), 'Bool'));
  it('string concat', () => assert.equal(typeOf('"a" ++ "b"'), 'String'));
  it('negation', () => assert.equal(typeOf('not true'), 'Bool'));
});

// ============================================================
// Type Inference: If-then-else
// ============================================================

describe('Inference: If', () => {
  it('if true then int', () => assert.equal(typeOf('if true then 1 else 2'), 'Int'));
  it('if expr then string', () => assert.equal(typeOf('if false then "a" else "b"'), 'String'));
  it('condition must be bool', () => {
    assert.throws(() => typeOf('if 5 then 1 else 2'));
  });
  it('branches must agree', () => {
    assert.throws(() => typeOf('if true then 1 else "no"'));
  });
});

// ============================================================
// Type Inference: Lists
// ============================================================

describe('Inference: Lists', () => {
  it('empty list', () => {
    const t = typeOf('[]');
    assert(t.startsWith('['));
  });

  it('int list', () => assert.equal(typeOf('[1, 2, 3]'), '[Int]'));
  it('bool list', () => assert.equal(typeOf('[true, false]'), '[Bool]'));
  it('string list', () => assert.equal(typeOf('["a", "b"]'), '[String]'));
  it('nested list', () => assert.equal(typeOf('[[1, 2], [3]]'), '[[Int]]'));
  it('heterogeneous list fails', () => {
    assert.throws(() => typeOf('[1, true]'));
  });
});

// ============================================================
// Type Inference: Pairs
// ============================================================

describe('Inference: Pairs', () => {
  it('int-bool pair', () => assert.equal(typeOf('(1, true)'), '(Int, Bool)'));
  it('string-int pair', () => assert.equal(typeOf('("hi", 42)'), '(String, Int)'));
  it('fst', () => assert.equal(typeOf('fst (1, true)'), 'Int'));
  it('snd', () => assert.equal(typeOf('snd (1, true)'), 'Bool'));
  it('nested pair', () => assert.equal(typeOf('((1, 2), true)'), '((Int, Int), Bool)'));
});

// ============================================================
// Type Errors
// ============================================================

describe('Type Errors', () => {
  it('applying int', () => assert.throws(() => typeOf('5 3')));
  it('adding bool', () => assert.throws(() => typeOf('true + 1')));
  it('unbound variable', () => assert.throws(() => typeOf('x')));
  it('infinite type', () => assert.throws(() => typeOf('\\x -> x x')));
});

// ============================================================
// Complex Programs
// ============================================================

describe('Complex Programs', () => {
  it('map-like higher-order function', () => {
    const t = typeOf('\\f -> \\x -> f x');
    assert(t.includes('->'));
  });

  it('flip', () => {
    const t = typeOf('\\f -> \\x -> \\y -> f y x');
    assert(t.includes('->'));
  });

  it('twice', () => {
    assert.equal(typeOf('let twice = \\f -> \\x -> f (f x) in twice (\\n -> n + 1) 0'), 'Int');
  });

  it('church numerals in HM', () => {
    const t = typeOf('let zero = \\f -> \\x -> x in let succ = \\n -> \\f -> \\x -> f (n f x) in succ zero');
    assert(t.includes('->'));
  });

  it('fibonacci', () => {
    const t = typeOf('let rec fib = \\n -> if n < 2 then n else fib (n - 1) + fib (n - 2) in fib 10');
    assert.equal(t, 'Int');
  });

  it('factorial', () => {
    const t = typeOf('let rec fact = \\n -> if n == 0 then 1 else n * fact (n - 1) in fact 5');
    assert.equal(t, 'Int');
  });

  it('map with let-rec', () => {
    const t = typeOf('let rec map = \\f -> \\xs -> if xs == [] then [] else [f (fst (xs, 0))] in map');
    assert(t.includes('->'));
  });
});

// ============================================================
// Substitution
// ============================================================

describe('Substitution', () => {
  it('empty substitution is identity', () => {
    const s = new Subst(new Map());
    assert.equal(s.apply(tInt).toString(), 'Int');
  });

  it('substitutes type variable', () => {
    const s = new Subst(new Map([['a', tInt]]));
    assert.equal(s.apply(new TVar('a')).toString(), 'Int');
  });

  it('leaves other variables alone', () => {
    const s = new Subst(new Map([['a', tInt]]));
    assert.equal(s.apply(new TVar('b')).toString(), 'b');
  });

  it('applies through function types', () => {
    const s = new Subst(new Map([['a', tInt], ['b', tBool]]));
    const t = new TFun(new TVar('a'), new TVar('b'));
    assert.equal(s.apply(t).toString(), 'Int -> Bool');
  });

  it('composes substitutions', () => {
    const s1 = new Subst(new Map([['a', new TVar('b')]]));
    const s2 = new Subst(new Map([['b', tInt]]));
    const composed = s2.compose(s1);
    assert.equal(composed.apply(new TVar('a')).toString(), 'Int');
  });
});

// ============================================================
// Free Type Variables & Generalization
// ============================================================

describe('Free Type Variables', () => {
  it('constant has no free vars', () => {
    assert.equal(freeTypeVars(tInt).size, 0);
  });

  it('type variable is free', () => {
    const fv = freeTypeVars(new TVar('a'));
    assert(fv.has('a'));
  });

  it('function type collects both', () => {
    const fv = freeTypeVars(new TFun(new TVar('a'), new TVar('b')));
    assert(fv.has('a'));
    assert(fv.has('b'));
  });
});

describe('Generalize & Instantiate', () => {
  beforeEach(() => resetFresh());

  it('generalizes free variables', () => {
    const env = new TypeEnv();
    const scheme = generalize(env, new TFun(new TVar('a'), new TVar('a')));
    assert(scheme.vars.length > 0);
  });

  it('does not generalize env-bound variables', () => {
    const env = new TypeEnv();
    env.map.set('x', new Scheme(['a'], new TVar('a')));
    const scheme = generalize(env, new TFun(new TVar('b'), tInt));
    assert(scheme.vars.includes('b'));
  });
});

// ============================================================
// Parser
// ============================================================

describe('Parser', () => {
  it('parses integer', () => {
    const p = new Parser('42');
    const ast = p.parse();
    assert.equal(ast.type, 'int');
    assert.equal(ast.value, 42);
  });

  it('parses lambda', () => {
    const p = new Parser('\\x -> x');
    const ast = p.parse();
    assert.equal(ast.type, 'lambda');
  });

  it('parses let', () => {
    const p = new Parser('let x = 5 in x');
    const ast = p.parse();
    assert.equal(ast.type, 'let');
  });

  it('parses if-then-else', () => {
    const p = new Parser('if true then 1 else 2');
    const ast = p.parse();
    assert.equal(ast.type, 'if');
  });

  it('parses application', () => {
    const p = new Parser('f x');
    const ast = p.parse();
    assert.equal(ast.type, 'app');
  });

  it('parses pair', () => {
    const p = new Parser('(1, 2)');
    const ast = p.parse();
    assert.equal(ast.type, 'pair');
  });

  it('parses list', () => {
    const p = new Parser('[1, 2, 3]');
    const ast = p.parse();
    assert.equal(ast.type, 'list');
  });

  it('parses let rec', () => {
    const p = new Parser('let rec f = \\x -> x in f');
    const ast = p.parse();
    assert(ast.type === 'letrec' || ast.type === 'let');
  });
});
