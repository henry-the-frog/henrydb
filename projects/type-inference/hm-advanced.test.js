import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  typeOf, TVar, TCon, TFun, TList, TPair, tInt, tBool, tString, tUnit,
  Scheme, Subst, TypeEnv, unify, generalize, instantiate,
  freeTypeVars, occurs, freshVar, resetFresh, infer, Parser,
} from './types.js';

// ============================================================
// Advanced Polymorphism
// ============================================================

describe('Let-Polymorphism: Advanced', () => {
  it('polymorphic identity used at 3 different types', () => {
    const t = typeOf(`
      let id = \\x -> x in
      let a = id 5 in
      let b = id true in
      let c = id "hello" in
      (a, (b, c))
    `);
    assert.equal(t, '(Int, (Bool, String))');
  });

  it('polymorphic const used multiple ways', () => {
    const t = typeOf(`
      let k = \\x -> \\y -> x in
      let a = k 5 true in
      let b = k "hi" 0 in
      (a, b)
    `);
    assert.equal(t, '(Int, String)');
  });

  it('polymorphic flip', () => {
    // flip K true 5 = K 5 true = 5 (Int!)
    // flip swaps the arguments, so K gets 5 first
    const t = typeOf(`
      let flip = \\f -> \\x -> \\y -> f y x in
      flip (\\a -> \\b -> a) true 5
    `);
    assert.equal(t, 'Int');
  });

  it('polymorphic compose', () => {
    const t = typeOf(`
      let compose = \\f -> \\g -> \\x -> f (g x) in
      compose (\\x -> x + 1) (\\x -> x * 2) 3
    `);
    assert.equal(t, 'Int');
  });

  it('polymorphic apply', () => {
    const t = typeOf(`
      let apply = \\f -> \\x -> f x in
      let a = apply (\\x -> x + 1) 5 in
      a
    `);
    assert.equal(t, 'Int');
  });
});

describe('Recursive Functions (let rec)', () => {
  it('factorial', () => {
    const t = typeOf('let rec fact = \\n -> if n == 0 then 1 else n * fact (n - 1) in fact 10');
    assert.equal(t, 'Int');
  });

  it('fibonacci', () => {
    const t = typeOf('let rec fib = \\n -> if n < 2 then n else fib (n - 1) + fib (n - 2) in fib 20');
    assert.equal(t, 'Int');
  });

  it('power function', () => {
    const t = typeOf(`
      let rec pow = \\base -> \\exp ->
        if exp == 0 then 1
        else base * pow base (exp - 1)
      in pow 2 10
    `);
    assert.equal(t, 'Int');
  });

  it('GCD', () => {
    const t = typeOf(`
      let rec gcd = \\a -> \\b ->
        if b == 0 then a
        else gcd b (a % b)
      in gcd 12 8
    `);
    assert.equal(t, 'Int');
  });

  it('nested let-rec', () => {
    const t = typeOf(`
      let rec isEven = \\n ->
        if n == 0 then true
        else if n == 1 then false
        else isEven (n - 2)
      in isEven 10
    `);
    assert.equal(t, 'Bool');
  });
});

describe('Higher-Order Functions', () => {
  it('map type inference', () => {
    const t = typeOf('\\f -> \\x -> f x');
    // Should be (a -> b) -> a -> b
    assert(t.includes('->'));
  });

  it('twice with int function', () => {
    const t = typeOf(`
      let twice = \\f -> \\x -> f (f x) in
      twice (\\n -> n + 1) 0
    `);
    assert.equal(t, 'Int');
  });

  it('church encoding of 2', () => {
    const t = typeOf(`
      let two = \\f -> \\x -> f (f x) in
      two (\\n -> n + 1) 0
    `);
    assert.equal(t, 'Int');
  });

  it('function returning a function', () => {
    const t = typeOf(`
      let adder = \\n -> \\m -> n + m in
      let add5 = adder 5 in
      add5 3
    `);
    assert.equal(t, 'Int');
  });
});

describe('Pairs: Advanced', () => {
  it('swap pair', () => {
    const t = typeOf(`
      let swap = \\p -> (snd p, fst p) in
      swap (1, true)
    `);
    assert.equal(t, '(Bool, Int)');
  });

  it('deeply nested pairs', () => {
    assert.equal(typeOf('((1, 2), (true, "hi"))'), '((Int, Int), (Bool, String))');
  });

  it('pair of functions', () => {
    const t = typeOf('(\\x -> x + 1, \\x -> not x)');
    assert.equal(t, '(Int -> Int, Bool -> Bool)');
  });

  it('apply pair of functions', () => {
    const t = typeOf(`
      let p = (\\x -> x + 1, \\x -> not x) in
      ((fst p) 5, (snd p) true)
    `);
    assert.equal(t, '(Int, Bool)');
  });
});

describe('Lists: Advanced', () => {
  it('nested list', () => {
    assert.equal(typeOf('[[1, 2], [3, 4]]'), '[[Int]]');
  });

  it('list of pairs', () => {
    assert.equal(typeOf('[(1, true), (2, false)]'), '[(Int, Bool)]');
  });

  it('singleton list', () => {
    assert.equal(typeOf('[42]'), '[Int]');
  });

  it('empty list', () => {
    const t = typeOf('[]');
    // Should be [α] for some type variable
    assert(t.startsWith('['));
    assert(t.endsWith(']'));
  });
});

describe('Operator Edge Cases', () => {
  it('chained arithmetic', () => {
    assert.equal(typeOf('1 + 2 * 3 - 4'), 'Int');
  });

  it('comparison chains', () => {
    assert.equal(typeOf('if 1 < 2 then 3 > 0 else 5 == 5'), 'Bool');
  });

  it('boolean operators', () => {
    assert.equal(typeOf('if true then not false else true'), 'Bool');
  });

  it('string operations', () => {
    assert.equal(typeOf('"hello" ++ " " ++ "world"'), 'String');
  });
});

describe('Type Error: Advanced', () => {
  it('applying bool to int', () => {
    assert.throws(() => typeOf('true 5'));
  });

  it('adding strings without concat', () => {
    assert.throws(() => typeOf('"a" + "b"'));
  });

  it('if with non-bool condition', () => {
    assert.throws(() => typeOf('if 1 then 2 else 3'));
  });

  it('mismatched pair in list', () => {
    assert.throws(() => typeOf('[(1, true), (2, 3)]'));
  });

  it('comparing different types', () => {
    assert.throws(() => typeOf('5 == "five"'));
  });
});

describe('Substitution: Advanced', () => {
  beforeEach(() => resetFresh());

  it('chain of substitutions', () => {
    const s1 = new Subst(new Map([['a', new TVar('b')]]));
    const s2 = new Subst(new Map([['b', new TVar('c')]]));
    const s3 = new Subst(new Map([['c', tInt]]));
    const composed = s3.compose(s2.compose(s1));
    assert.equal(composed.apply(new TVar('a')).toString(), 'Int');
  });

  it('substitution in list type', () => {
    const s = new Subst(new Map([['a', tInt]]));
    assert.equal(s.apply(new TList(new TVar('a'))).toString(), '[Int]');
  });

  it('substitution in pair type', () => {
    const s = new Subst(new Map([['a', tInt], ['b', tBool]]));
    assert.equal(s.apply(new TPair(new TVar('a'), new TVar('b'))).toString(), '(Int, Bool)');
  });

  it('substitution in nested function type', () => {
    const s = new Subst(new Map([['a', tInt], ['b', tBool], ['c', tString]]));
    const t = new TFun(new TVar('a'), new TFun(new TVar('b'), new TVar('c')));
    assert.equal(s.apply(t).toString(), 'Int -> Bool -> String');
  });
});

describe('Unification: Advanced', () => {
  beforeEach(() => resetFresh());

  it('unifies nested function types', () => {
    const t1 = new TFun(new TVar('a'), new TFun(new TVar('b'), new TVar('c')));
    const t2 = new TFun(tInt, new TFun(tBool, tString));
    const s = unify(t1, t2);
    assert.equal(s.apply(new TVar('a')).toString(), 'Int');
    assert.equal(s.apply(new TVar('b')).toString(), 'Bool');
    assert.equal(s.apply(new TVar('c')).toString(), 'String');
  });

  it('unifies list of pairs', () => {
    const t1 = new TList(new TPair(new TVar('a'), new TVar('b')));
    const t2 = new TList(new TPair(tInt, tBool));
    const s = unify(t1, t2);
    assert.equal(s.apply(new TVar('a')).toString(), 'Int');
    assert.equal(s.apply(new TVar('b')).toString(), 'Bool');
  });

  it('unifies with variable on both sides', () => {
    const s = unify(new TFun(new TVar('a'), new TVar('b')), new TFun(new TVar('b'), tInt));
    assert.equal(s.apply(new TVar('a')).toString(), 'Int');
    assert.equal(s.apply(new TVar('b')).toString(), 'Int');
  });

  it('fails on nested occurs check', () => {
    assert.throws(() => unify(new TVar('a'), new TFun(tInt, new TVar('a'))));
  });

  it('fails on list vs function', () => {
    assert.throws(() => unify(new TList(tInt), new TFun(tInt, tInt)));
  });
});

describe('Generalization & Instantiation: Advanced', () => {
  it('generalize with no free vars generalizes all', () => {
    const env = new TypeEnv();
    const scheme = generalize(env, new TFun(new TVar('x'), new TVar('y')));
    assert(scheme.vars.length >= 2);
  });

  it('instantiation produces function types (no resetFresh)', () => {
    // Note: calling resetFresh() before instantiate would cause freshVar to generate
    // 'a' which collides with scheme variable 'a' → infinite loop in Subst.apply
    // This is a real bug/footgun in the implementation
    const scheme = new Scheme(['x'], new TFun(new TVar('x'), new TVar('x')));
    const t1 = instantiate(scheme);
    assert(t1 instanceof TFun);
    assert.equal(t1.param.toString(), t1.result.toString());
  });
});
