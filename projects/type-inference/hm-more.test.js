import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  typeOf, TVar, TCon, TFun, TList, TPair, tInt, tBool, tString, tUnit,
  Scheme, Subst, TypeEnv, unify, generalize, instantiate,
  freeTypeVars, occurs, freshVar, resetFresh, infer, Parser,
} from './types.js';

describe('Type Inference: Pattern Matching Style', () => {
  it('conditional with function application', () => {
    const t = typeOf('if true then (\\x -> x + 1) 5 else 0');
    assert.equal(t, 'Int');
  });

  it('let with conditional', () => {
    const t = typeOf('let x = if true then 5 else 10 in x + 1');
    assert.equal(t, 'Int');
  });

  it('higher-order with let', () => {
    const t = typeOf('let twice = \\f -> \\x -> f (f x) in let inc = \\n -> n + 1 in twice inc 0');
    assert.equal(t, 'Int');
  });

  it('pair in conditional', () => {
    const t = typeOf('if true then (1, 2) else (3, 4)');
    assert.equal(t, '(Int, Int)');
  });

  it('fst of conditional pair', () => {
    const t = typeOf('fst (if true then (1, 2) else (3, 4))');
    assert.equal(t, 'Int');
  });

  it('list in conditional', () => {
    const t = typeOf('if true then [1, 2] else [3]');
    assert.equal(t, '[Int]');
  });
});

describe('Type Inference: Complex Let Bindings', () => {
  it('let with function returning pair', () => {
    const t = typeOf('let mkPair = \\x -> \\y -> (x, y) in mkPair 5 true');
    assert.equal(t, '(Int, Bool)');
  });

  it('let with polymorphic pair constructor', () => {
    const t = typeOf('let p = \\x -> (x, x) in (fst (p 5), fst (p true))');
    assert.equal(t, '(Int, Bool)');
  });

  it('nested let with shadowing', () => {
    const t = typeOf('let x = 5 in let x = true in x');
    assert.equal(t, 'Bool');
  });

  it('let with string operations', () => {
    const t = typeOf('let greet = \\name -> "Hello " ++ name in greet "World"');
    assert.equal(t, 'String');
  });
});

describe('Type Inference: Recursive Functions', () => {
  it('sum up to n', () => {
    const t = typeOf('let rec sum = \\n -> if n == 0 then 0 else n + sum (n - 1) in sum 100');
    assert.equal(t, 'Int');
  });

  it('countdown', () => {
    const t = typeOf('let rec countdown = \\n -> if n == 0 then 0 else countdown (n - 1) in countdown 10');
    assert.equal(t, 'Int');
  });

  it('collatz step', () => {
    const t = typeOf('let step = \\n -> if n % 2 == 0 then n / 2 else 3 * n + 1 in step 7');
    assert.equal(t, 'Int');
  });

  it('abs value', () => {
    const t = typeOf('let abs = \\n -> if n < 0 then 0 - n else n in abs (0 - 5)');
    assert.equal(t, 'Int');
  });

  it('max function', () => {
    const t = typeOf('let max = \\a -> \\b -> if a > b then a else b in max 5 3');
    assert.equal(t, 'Int');
  });

  it('min function', () => {
    const t = typeOf('let min = \\a -> \\b -> if a < b then a else b in min 5 3');
    assert.equal(t, 'Int');
  });
});

describe('Type Inference: Higher-Kinded Style', () => {
  it('function pipeline', () => {
    const t = typeOf('let pipe = \\f -> \\g -> \\x -> g (f x) in pipe (\\x -> x + 1) (\\x -> x * 2) 3');
    assert.equal(t, 'Int');
  });

  it('double application', () => {
    const t = typeOf('let dbl = \\f -> \\x -> f (f x) in dbl (\\n -> n * 2) 1');
    assert.equal(t, 'Int');
  });

  it('triple application', () => {
    const t = typeOf('let triple = \\f -> \\x -> f (f (f x)) in triple (\\n -> n + 1) 0');
    assert.equal(t, 'Int');
  });
});

describe('Type Errors: Advanced', () => {
  it('polymorphism escape in monomorphic context', () => {
    // This should work with let-polymorphism
    const t = typeOf('let id = \\x -> x in (id 5, id true)');
    assert.equal(t, '(Int, Bool)');
  });

  it('wrong number of args to binary op', () => {
    assert.throws(() => typeOf('1 + true'));
  });

  it('comparison of incompatible types', () => {
    assert.throws(() => typeOf('5 == true'));
  });

  it('nested type error in pair', () => {
    assert.throws(() => typeOf('(1 + true, 2)'));
  });
});

describe('Unification: Complex', () => {
  it('unifies deeply nested types', () => {
    const t1 = new TFun(new TList(new TVar('a')), new TPair(new TVar('a'), tBool));
    const t2 = new TFun(new TList(tInt), new TPair(tInt, new TVar('b')));
    const s = unify(t1, t2);
    assert.equal(s.apply(new TVar('a')).toString(), 'Int');
    assert.equal(s.apply(new TVar('b')).toString(), 'Bool');
  });

  it('transitivity through unification', () => {
    const s1 = unify(new TVar('a'), new TVar('b'));
    const s2 = unify(s1.apply(new TVar('b')), tInt);
    const composed = s2.compose(s1);
    assert.equal(composed.apply(new TVar('a')).toString(), 'Int');
  });

  it('unifies function returning list', () => {
    const t1 = new TFun(new TVar('a'), new TList(new TVar('a')));
    const t2 = new TFun(tInt, new TList(tInt));
    const s = unify(t1, t2);
    assert.equal(s.apply(new TVar('a')).toString(), 'Int');
  });
});
