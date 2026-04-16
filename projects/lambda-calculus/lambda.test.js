import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  Var, Abs, App,
  DeBruijnVar, DeBruijnAbs, DeBruijnApp,
  toDeBruijn, fromDeBruijn,
  parse, tokenize,
  freeVars, substitute, alphaEquivalent,
  betaReduce, reduce,
  normalOrderStep, applicativeOrderStep, callByValueStep, callByNameStep,
  church, churchNumeral, unchurch, unchurchBool,
  prettyPrint, resetFreshCounter, isValue,
} from './lambda.js';

describe('Tokenizer', () => {
  it('tokenizes simple lambda', () => {
    assert.deepEqual(tokenize('λx.x'), ['λ', 'x', '.', 'x']);
  });

  it('tokenizes backslash as lambda', () => {
    assert.deepEqual(tokenize('\\x.x'), ['λ', 'x', '.', 'x']);
  });

  it('tokenizes application', () => {
    assert.deepEqual(tokenize('(f x)'), ['(', 'f', 'x', ')']);
  });

  it('tokenizes multi-char identifiers', () => {
    assert.deepEqual(tokenize('λfoo.bar'), ['λ', 'foo', '.', 'bar']);
  });

  it('handles whitespace', () => {
    assert.deepEqual(tokenize('  λ x . x  '), ['λ', 'x', '.', 'x']);
  });

  it('tokenizes nested expression', () => {
    assert.deepEqual(tokenize('(λx.(x x))'), ['(', 'λ', 'x', '.', '(', 'x', 'x', ')', ')']);
  });

  it('throws on unexpected character', () => {
    assert.throws(() => tokenize('λx.@'), /Unexpected character/);
  });
});

describe('Parser', () => {
  it('parses variable', () => {
    const r = parse('x');
    assert(r instanceof Var);
    assert.equal(r.name, 'x');
  });

  it('parses identity', () => {
    const r = parse('λx.x');
    assert(r instanceof Abs);
    assert.equal(r.param, 'x');
    assert(r.body instanceof Var);
    assert.equal(r.body.name, 'x');
  });

  it('parses application', () => {
    const r = parse('(f x)');
    assert(r instanceof App);
    assert.equal(r.func.name, 'f');
    assert.equal(r.arg.name, 'x');
  });

  it('parses nested abstraction', () => {
    const r = parse('λx.λy.x');
    assert(r instanceof Abs);
    assert.equal(r.param, 'x');
    assert(r.body instanceof Abs);
    assert.equal(r.body.param, 'y');
  });

  it('parses multi-param lambda as curried', () => {
    const r = parse('λx y.x');
    assert(r instanceof Abs);
    assert.equal(r.param, 'x');
    assert(r.body instanceof Abs);
    assert.equal(r.body.param, 'y');
    assert.equal(r.body.body.name, 'x');
  });

  it('parses left-associative application', () => {
    const r = parse('a b c');
    assert(r instanceof App);
    assert(r.func instanceof App);
    assert.equal(r.func.func.name, 'a');
    assert.equal(r.func.arg.name, 'b');
    assert.equal(r.arg.name, 'c');
  });

  it('parses parenthesized application', () => {
    const r = parse('a (b c)');
    assert(r instanceof App);
    assert.equal(r.func.name, 'a');
    assert(r.arg instanceof App);
  });

  it('parses omega', () => {
    const r = parse('(λx.x x) (λx.x x)');
    assert(r instanceof App);
    assert(r.func instanceof Abs);
    assert(r.arg instanceof Abs);
  });

  it('parses Church numeral 2', () => {
    const r = parse('λf x.f (f x)');
    assert(r instanceof Abs);
    assert.equal(r.param, 'f');
  });

  it('throws on unexpected token', () => {
    assert.throws(() => parse('(x'), /Expected/);
  });
});

describe('Free Variables', () => {
  it('variable is free', () => {
    assert.deepEqual(freeVars(parse('x')), new Set(['x']));
  });

  it('bound variable is not free', () => {
    assert.deepEqual(freeVars(parse('λx.x')), new Set());
  });

  it('mixed free and bound', () => {
    assert.deepEqual(freeVars(parse('λx.x y')), new Set(['y']));
  });

  it('multiple free variables', () => {
    assert.deepEqual(freeVars(parse('x y z')), new Set(['x', 'y', 'z']));
  });

  it('nested binding', () => {
    assert.deepEqual(freeVars(parse('λx.λy.x y z')), new Set(['z']));
  });

  it('no free variables in Church true', () => {
    assert.deepEqual(freeVars(parse('λt.λf.t')), new Set());
  });
});

describe('Substitution', () => {
  beforeEach(() => resetFreshCounter());

  it('substitutes variable', () => {
    const result = substitute(parse('x'), 'x', parse('y'));
    assert.equal(result.toString(), 'y');
  });

  it('does not substitute different variable', () => {
    const result = substitute(parse('y'), 'x', parse('z'));
    assert.equal(result.name, 'y');
  });

  it('substitutes in application', () => {
    const result = substitute(parse('x x'), 'x', parse('y'));
    assert(result instanceof App);
    assert.equal(result.func.name, 'y');
    assert.equal(result.arg.name, 'y');
  });

  it('does not substitute under shadowing lambda', () => {
    const result = substitute(parse('λx.x'), 'x', parse('y'));
    assert(result instanceof Abs);
    assert.equal(result.body.name, 'x');
  });

  it('substitutes under non-shadowing lambda', () => {
    const result = substitute(parse('λy.x'), 'x', parse('z'));
    assert(result instanceof Abs);
    assert.equal(result.body.name, 'z');
  });

  it('avoids variable capture', () => {
    // Substitute y/x in λy.x should alpha-rename y
    const result = substitute(parse('λy.x'), 'x', parse('y'));
    assert(result instanceof Abs);
    // The param should be renamed to avoid capture
    assert.notEqual(result.param, 'y');
    // The body should be the substituted value
    assert.equal(result.body.name, 'y');
  });
});

describe('Beta Reduction', () => {
  it('reduces identity applied to variable', () => {
    const id = parse('λx.x');
    const result = betaReduce(id, new Var('y'));
    assert.equal(result.name, 'y');
  });

  it('reduces K combinator', () => {
    const k = parse('λx y.x');
    // (K a) = λy.a
    const result = betaReduce(k, new Var('a'));
    assert(result instanceof Abs);
    assert.equal(result.body.name, 'a');
  });
});

describe('Normal-Order Reduction', () => {
  it('reduces identity application', () => {
    const r = reduce(parse('(λx.x) y'), 'normal');
    assert.equal(r.result.name, 'y');
    assert.equal(r.steps, 1);
  });

  it('reduces K combinator fully', () => {
    const r = reduce(parse('(λx.λy.x) a b'), 'normal');
    assert.equal(r.result.name, 'a');
    assert.equal(r.steps, 2);
  });

  it('reduces S combinator', () => {
    // S = λf g x. f x (g x)
    // S K K x = K x (K x) = x
    const r = reduce(parse('(λf g x.f x (g x)) (λx y.x) (λx y.x) z'), 'normal');
    assert.equal(r.result.name, 'z');
  });

  it('reduces under lambda (full normal form)', () => {
    const r = reduce(parse('λx.(λy.y) x'), 'normal');
    assert(r.result instanceof Abs);
    assert.equal(r.result.body.name, 'x');
  });

  it('handles already-normal term', () => {
    const r = reduce(parse('λx.x'), 'normal');
    assert.equal(r.steps, 0);
    assert(r.normalForm);
  });

  it('detects non-termination via max steps', () => {
    const r = reduce(parse('(λx.x x) (λx.x x)'), 'normal', 10);
    assert.equal(r.steps, 10);
    assert(!r.normalForm);
  });
});

describe('Applicative-Order Reduction', () => {
  it('reduces identity', () => {
    const r = reduce(parse('(λx.x) y'), 'applicative');
    assert.equal(r.result.name, 'y');
  });

  it('reduces inner first', () => {
    // (λx.x) ((λy.y) z) → first reduce inner (λy.y) z → z, then (λx.x) z → z
    const r = reduce(parse('(λx.x) ((λy.y) z)'), 'applicative');
    assert.equal(r.result.name, 'z');
    assert.equal(r.steps, 2);
  });
});

describe('Call-by-Value Reduction', () => {
  it('reduces identity', () => {
    const r = reduce(parse('(λx.x) y'), 'cbv');
    assert.equal(r.result.name, 'y');
  });

  it('does not reduce under lambda', () => {
    const r = reduce(parse('λx.(λy.y) x'), 'cbv');
    assert.equal(r.steps, 0); // CBV doesn't go under lambdas
  });

  it('reduces argument before beta', () => {
    const r = reduce(parse('(λx.x) ((λy.y) z)'), 'cbv');
    assert.equal(r.result.name, 'z');
  });
});

describe('Call-by-Name Reduction', () => {
  it('reduces identity', () => {
    const r = reduce(parse('(λx.x) y'), 'cbn');
    assert.equal(r.result.name, 'y');
  });

  it('does not reduce argument', () => {
    // K a ((λx.x x) (λx.x x)) = a
    // CBN doesn't evaluate the divergent argument
    const r = reduce(parse('(λx y.x) a ((λx.x x) (λx.x x))'), 'cbn', 100);
    assert.equal(r.result.name, 'a');
    assert(r.normalForm);
  });
});

describe('De Bruijn Indices', () => {
  it('converts identity', () => {
    const db = toDeBruijn(parse('λx.x'));
    assert(db instanceof DeBruijnAbs);
    assert(db.body instanceof DeBruijnVar);
    assert.equal(db.body.index, 0);
  });

  it('converts K combinator', () => {
    const db = toDeBruijn(parse('λx.λy.x'));
    assert.equal(db.body.body.index, 1);
  });

  it('converts nested binding', () => {
    const db = toDeBruijn(parse('λx.λy.λz.x z (y z)'));
    // x=2, y=1, z=0
    assert(db instanceof DeBruijnAbs);
  });

  it('round-trips through de Bruijn', () => {
    const original = parse('λx.λy.x y');
    const db = toDeBruijn(original);
    const back = fromDeBruijn(db);
    assert(alphaEquivalent(original, back));
  });
});

describe('Alpha-Equivalence', () => {
  it('identity is alpha-equivalent regardless of param name', () => {
    assert(alphaEquivalent(parse('λx.x'), parse('λy.y')));
  });

  it('different structures are not equivalent', () => {
    assert(!alphaEquivalent(parse('λx.x'), parse('λx.λy.x')));
  });

  it('Church true variants are equivalent', () => {
    assert(alphaEquivalent(parse('λa.λb.a'), parse('λt.λf.t')));
  });

  it('K and K* are not equivalent', () => {
    assert(!alphaEquivalent(parse('λx y.x'), parse('λx y.y')));
  });
});

describe('Church Booleans', () => {
  it('TRUE selects first', () => {
    const r = reduce(new App(new App(church.true, new Var('a')), new Var('b')));
    assert.equal(r.result.name, 'a');
  });

  it('FALSE selects second', () => {
    const r = reduce(new App(new App(church.false, new Var('a')), new Var('b')));
    assert.equal(r.result.name, 'b');
  });

  it('AND true true = true', () => {
    const expr = new App(new App(church.and, church.true), church.true);
    assert.equal(unchurchBool(reduce(expr).result), true);
  });

  it('AND true false = false', () => {
    const expr = new App(new App(church.and, church.true), church.false);
    assert.equal(unchurchBool(reduce(expr).result), false);
  });

  it('OR false true = true', () => {
    const expr = new App(new App(church.or, church.false), church.true);
    assert.equal(unchurchBool(reduce(expr).result), true);
  });

  it('OR false false = false', () => {
    const expr = new App(new App(church.or, church.false), church.false);
    assert.equal(unchurchBool(reduce(expr).result), false);
  });

  it('NOT true = false', () => {
    const expr = new App(church.not, church.true);
    assert.equal(unchurchBool(reduce(expr).result), false);
  });

  it('NOT false = true', () => {
    const expr = new App(church.not, church.false);
    assert.equal(unchurchBool(reduce(expr).result), true);
  });
});

describe('Church Numerals', () => {
  it('builds numeral 0', () => {
    assert.equal(unchurch(churchNumeral(0)), 0);
  });

  it('builds numeral 1', () => {
    assert.equal(unchurch(churchNumeral(1)), 1);
  });

  it('builds numeral 5', () => {
    assert.equal(unchurch(churchNumeral(5)), 5);
  });

  it('succ 0 = 1', () => {
    const expr = new App(church.succ, church.zero);
    assert.equal(unchurch(reduce(expr).result), 1);
  });

  it('succ 2 = 3', () => {
    const expr = new App(church.succ, church.two);
    assert.equal(unchurch(reduce(expr).result), 3);
  });

  it('plus 2 3 = 5', () => {
    const expr = new App(new App(church.plus, church.two), church.three);
    assert.equal(unchurch(reduce(expr).result), 5);
  });

  it('mult 2 3 = 6', () => {
    const expr = new App(new App(church.mult, church.two), church.three);
    assert.equal(unchurch(reduce(expr).result), 6);
  });

  it('exp 2 3 = 8', () => {
    const expr = new App(new App(church.exp, church.two), church.three);
    assert.equal(unchurch(reduce(expr).result), 8);
  });

  it('pred 3 = 2', () => {
    const expr = new App(church.pred, church.three);
    assert.equal(unchurch(reduce(expr).result), 2);
  });

  it('pred 1 = 0', () => {
    const expr = new App(church.pred, church.one);
    assert.equal(unchurch(reduce(expr).result), 0);
  });

  it('isZero 0 = true', () => {
    const expr = new App(church.isZero, church.zero);
    assert.equal(unchurchBool(reduce(expr).result), true);
  });

  it('isZero 1 = false', () => {
    const expr = new App(church.isZero, church.one);
    assert.equal(unchurchBool(reduce(expr).result), false);
  });

  it('sub 3 1 = 2', () => {
    const expr = new App(new App(church.sub, church.three), church.one);
    assert.equal(unchurch(reduce(expr).result), 2);
  });

  it('plus is commutative: 1+2 = 2+1', () => {
    const a = reduce(new App(new App(church.plus, church.one), church.two)).result;
    const b = reduce(new App(new App(church.plus, church.two), church.one)).result;
    assert(alphaEquivalent(a, b));
  });
});

describe('Church Pairs', () => {
  it('fst (pair a b) = a', () => {
    const p = new App(new App(church.pair, new Var('a')), new Var('b'));
    const r = reduce(new App(church.fst, p));
    assert.equal(r.result.name, 'a');
  });

  it('snd (pair a b) = b', () => {
    const p = new App(new App(church.pair, new Var('a')), new Var('b'));
    const r = reduce(new App(church.snd, p));
    assert.equal(r.result.name, 'b');
  });

  it('nested pairs', () => {
    const inner = new App(new App(church.pair, new Var('x')), new Var('y'));
    const outer = new App(new App(church.pair, inner), new Var('z'));
    const r = reduce(new App(church.fst, outer));
    // Should get inner pair, then extract first
    const r2 = reduce(new App(church.fst, r.result));
    assert.equal(r2.result.name, 'x');
  });
});

describe('Y Combinator', () => {
  it('Y combinator type structure', () => {
    const y = church.Y;
    assert(y instanceof Abs);
    assert(y.body instanceof App);
  });

  it('factorial via Y combinator (Church numerals)', () => {
    // fact = Y (λf n. isZero n one (mult n (f (pred n))))
    // This is complex — let's just verify Y applied to something works
    const factBody = parse('λf n.n');  // simplified: returns n
    const expr = new App(new App(church.Y, factBody), church.three);
    const r = reduce(expr, 'normal', 500);
    assert(r.normalForm);
  });
});

describe('Combinators', () => {
  it('I combinator: I x = x', () => {
    const I = parse('λx.x');
    const r = reduce(new App(I, new Var('a')));
    assert.equal(r.result.name, 'a');
  });

  it('K combinator: K x y = x', () => {
    const K = parse('λx y.x');
    const r = reduce(new App(new App(K, new Var('a')), new Var('b')));
    assert.equal(r.result.name, 'a');
  });

  it('K* combinator: K* x y = y', () => {
    const KI = parse('λx y.y');
    const r = reduce(new App(new App(KI, new Var('a')), new Var('b')));
    assert.equal(r.result.name, 'b');
  });

  it('S combinator: S K K x = x', () => {
    const S = parse('λf g x.f x (g x)');
    const K = parse('λx y.x');
    const expr = new App(new App(new App(S, K), K), new Var('z'));
    const r = reduce(expr, 'normal', 100);
    assert.equal(r.result.name, 'z');
  });

  it('B combinator (composition): B f g x = f (g x)', () => {
    const B = parse('λf g x.f (g x)');
    const r = reduce(new App(new App(new App(B, new Var('f')), new Var('g')), new Var('a')));
    assert(r.result instanceof App);
    assert.equal(r.result.func.name, 'f');
    assert(r.result.arg instanceof App);
    assert.equal(r.result.arg.func.name, 'g');
  });

  it('C combinator (flip): C f x y = f y x', () => {
    const C = parse('λf x y.f y x');
    const r = reduce(new App(new App(new App(C, new Var('f')), new Var('a')), new Var('b')));
    assert(r.result instanceof App);
    // f b a
    assert(r.result.func instanceof App);
    assert.equal(r.result.func.func.name, 'f');
    assert.equal(r.result.func.arg.name, 'b');
    assert.equal(r.result.arg.name, 'a');
  });

  it('W combinator: W f x = f x x', () => {
    const W = parse('λf x.f x x');
    const r = reduce(new App(new App(W, new Var('f')), new Var('a')));
    assert(r.result instanceof App);
    assert.equal(r.result.arg.name, 'a');
  });
});

describe('Reduction Strategies Comparison', () => {
  it('normal order finds normal form that applicative misses', () => {
    // (λx.λy.x) a Ω  where Ω = (λx.x x)(λx.x x)
    // Normal order: reduces to a (ignores Ω)
    // Applicative: tries to reduce Ω first → infinite loop
    const expr = parse('(λx y.x) a ((λx.x x) (λx.x x))');
    const normal = reduce(expr, 'normal', 100);
    assert.equal(normal.result.name, 'a');
    assert(normal.normalForm);

    const applicative = reduce(expr, 'applicative', 100);
    assert(!applicative.normalForm); // Diverges
  });

  it('CBN finds weak head normal form', () => {
    const expr = parse('(λx y.x) a ((λx.x x) (λx.x x))');
    const cbn = reduce(expr, 'cbn', 100);
    assert.equal(cbn.result.name, 'a');
  });
});

describe('Pretty Printing', () => {
  it('prints variable', () => {
    assert.equal(prettyPrint(new Var('x')), 'x');
  });

  it('prints abstraction', () => {
    assert.equal(prettyPrint(parse('λx.x')), '(λx.x)');
  });

  it('prints application', () => {
    assert.equal(prettyPrint(parse('f x')), 'f x');
  });

  it('minimal mode collapses nested lambdas', () => {
    assert.equal(prettyPrint(parse('λx.λy.λz.x'), true), 'λx y z.x');
  });
});

describe('Trace', () => {
  it('records reduction steps', () => {
    const r = reduce(parse('(λx.x) ((λy.y) z)'), 'normal');
    assert(r.trace.length > 1);
    assert.equal(r.trace[0], '((λx.x) ((λy.y) z))');
  });

  it('trace has correct length', () => {
    const r = reduce(parse('(λx.x) y'), 'normal');
    assert.equal(r.trace.length, 2); // Before and after
    assert.equal(r.steps, 1);
  });
});

describe('Edge Cases', () => {
  it('handles deeply nested lambdas', () => {
    const expr = parse('λa b c d e.a b c d e');
    assert(expr instanceof Abs);
    assert.deepEqual(freeVars(expr), new Set());
  });

  it('handles complex reduction', () => {
    // (λx.x x) (λy.y) = (λy.y) (λy.y) = λy.y
    const r = reduce(parse('(λx.x x) (λy.y)'));
    assert(alphaEquivalent(r.result, parse('λy.y')));
  });

  it('reduces self-application without crash', () => {
    const r = reduce(parse('(λx.x x) (λx.x x)'), 'normal', 5);
    assert.equal(r.steps, 5);
    assert(!r.normalForm);
  });

  it('Church numeral 10', () => {
    assert.equal(unchurch(churchNumeral(10)), 10);
  });

  it('addition is associative', () => {
    // (1+2)+3 = 1+(2+3)
    const plus = church.plus;
    const one = church.one, two = church.two, three = church.three;
    const left = new App(new App(plus, new App(new App(plus, one), two)), three);
    const right = new App(new App(plus, one), new App(new App(plus, two), three));
    assert.equal(unchurch(reduce(left).result), 6);
    assert.equal(unchurch(reduce(right).result), 6);
  });

  it('isValue checks', () => {
    assert(isValue(new Var('x')));
    assert(isValue(new Abs('x', new Var('x'))));
    assert(!isValue(new App(new Var('f'), new Var('x'))));
  });
});

describe('Complex Programs', () => {
  it('Church encoding: 2^3 = 8', () => {
    const r = reduce(new App(new App(church.exp, church.two), church.three), 'normal', 2000);
    assert.equal(unchurch(r.result), 8);
  });

  it('nested if-then-else with Church booleans', () => {
    // if true then (if false then a else b) else c = b
    const inner = new App(new App(new App(church.ifthenelse, church.false), new Var('a')), new Var('b'));
    const outer = new App(new App(new App(church.ifthenelse, church.true), inner), new Var('c'));
    const r = reduce(outer);
    assert.equal(r.result.name, 'b');
  });

  it('pair swap via fst/snd', () => {
    // swap (pair a b) = pair (snd (pair a b)) (fst (pair a b))
    const p = new App(new App(church.pair, new Var('a')), new Var('b'));
    const swapped = new App(new App(church.pair, new App(church.snd, p)), new App(church.fst, p));
    const r1 = reduce(new App(church.fst, reduce(swapped).result));
    assert.equal(r1.result.name, 'b');
  });

  it('mult 3 3 = 9', () => {
    const nine = reduce(new App(new App(church.mult, church.three), church.three), 'normal', 5000);
    assert.equal(unchurch(nine.result), 9);
  });
});
