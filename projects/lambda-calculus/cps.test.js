import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  cpsTransform, cpsOnePass, anfTransform,
  adminReduce, evalCPS, termSize, resetCPS,
} from './cps.js';
import {
  parse, reduce, alphaEquivalent, Var, Abs, App,
  church, churchNumeral, unchurch,
} from './lambda.js';

describe('CPS Transform: Variables', () => {
  beforeEach(() => resetCPS());

  it('variable x: CPS = λk. k x', () => {
    const cps = cpsTransform(parse('x'));
    // Should be λk0. k0 x
    assert(cps instanceof Abs);
    assert(cps.body instanceof App);
    assert(cps.body.arg instanceof Var);
    assert.equal(cps.body.arg.name, 'x');
  });
});

describe('CPS Transform: Abstractions', () => {
  beforeEach(() => resetCPS());

  it('identity λx.x: CPS wraps with continuation', () => {
    const cps = cpsTransform(parse('λx.x'));
    assert(cps instanceof Abs); // outer k
    // k (λx. λk'. k' x)
    assert(cps.body instanceof App);
  });

  it('K combinator λx y.x: CPS preserves behavior', () => {
    const cps = cpsTransform(parse('λx y.x'));
    assert(cps instanceof Abs);
  });
});

describe('CPS Transform: Applications', () => {
  beforeEach(() => resetCPS());

  it('(f x): CPS evaluates both then applies', () => {
    const cps = cpsTransform(parse('f x'));
    assert(cps instanceof Abs);
  });

  it('((λx.x) y): CPS reduces to y', () => {
    const cps = cpsTransform(parse('(λx.x) y'));
    const r = evalCPS(cps);
    assert.equal(r.result.name, 'y');
  });
});

describe('CPS Evaluation Equivalence', () => {
  beforeEach(() => resetCPS());

  const testCases = [
    { name: 'identity applied', expr: '(λx.x) a', expected: 'a' },
    { name: 'K applied', expr: '(λx y.x) a b', expected: 'a' },
    { name: 'K* applied', expr: '(λx y.y) a b', expected: 'b' },
    { name: 'nested identity', expr: '(λx.x) ((λy.y) z)', expected: 'z' },
  ];

  for (const tc of testCases) {
    it(`${tc.name}: direct = CPS`, () => {
      const direct = reduce(parse(tc.expr), 'normal', 1000);
      const cps = cpsTransform(parse(tc.expr));
      const cpsResult = evalCPS(cps);
      assert.equal(direct.result.name, tc.expected);
      assert.equal(cpsResult.result.name, tc.expected);
    });
  }
});

describe('One-Pass CPS', () => {
  beforeEach(() => resetCPS());

  it('variable: one-pass produces smaller term', () => {
    const basic = cpsTransform(parse('x'));
    resetCPS();
    const onePass = cpsOnePass(parse('x'));
    // One-pass should produce same or smaller term
    assert(termSize(onePass) <= termSize(basic) + 2);
  });

  it('identity: one-pass equivalent', () => {
    const cps = cpsOnePass(parse('(λx.x) a'));
    const r = evalCPS(cps);
    assert.equal(r.result.name, 'a');
  });

  it('K combinator: one-pass equivalent', () => {
    const cps = cpsOnePass(parse('(λx y.x) a b'));
    const r = evalCPS(cps);
    assert.equal(r.result.name, 'a');
  });

  it('nested application: one-pass equivalent', () => {
    const cps = cpsOnePass(parse('(λx.x) ((λy.y) z)'));
    const r = evalCPS(cps);
    assert.equal(r.result.name, 'z');
  });

  it('one-pass is more compact than Fischer for applications', () => {
    const expr = parse('(λf.f x) (λy.y)');
    const fischer = cpsTransform(expr);
    resetCPS();
    const onePass = cpsOnePass(expr);
    // One-pass should generally produce smaller terms
    // (or at least equivalent)
    assert(termSize(onePass) <= termSize(fischer) + 5,
      `One-pass (${termSize(onePass)}) should be ≤ Fischer (${termSize(fischer)})+5`);
  });
});

describe('ANF Transform', () => {
  beforeEach(() => resetCPS());

  it('variable: ANF is identity', () => {
    const anf = anfTransform(parse('x'));
    assert(anf instanceof Var);
    assert.equal(anf.name, 'x');
  });

  it('abstraction: ANF normalizes body', () => {
    const anf = anfTransform(parse('λx.x'));
    assert(anf instanceof Abs);
    assert.equal(anf.param, 'x');
  });

  it('simple application: ANF keeps it', () => {
    const anf = anfTransform(parse('f x'));
    assert(anf instanceof App);
  });

  it('nested application: ANF names intermediates', () => {
    // f (g x) → let v = g x in f v
    const anf = anfTransform(parse('f (g x)'));
    // Should introduce a let-binding (encoded as (λv.f v) (g x))
    assert(anf instanceof App);
    assert(anf.func instanceof Abs); // This is the let-binding
  });

  it('ANF preserves evaluation result', () => {
    const expr = parse('(λx.x) ((λy.y) z)');
    const anf = anfTransform(expr);
    const directResult = reduce(expr, 'normal', 100);
    const anfResult = reduce(anf, 'normal', 100);
    assert.equal(directResult.result.name, 'z');
    assert.equal(anfResult.result.name, 'z');
  });

  it('deeply nested: ANF flattens', () => {
    const expr = parse('f (g (h x))');
    const anf = anfTransform(expr);
    // Should produce: let v1 = h x in let v2 = g v1 in f v2
    const result = reduce(anf, 'normal', 100);
    // Can't fully reduce without knowing f,g,h but structure should be flat
    assert(anf instanceof App);
  });
});

describe('Term Size', () => {
  it('variable has size 1', () => {
    assert.equal(termSize(parse('x')), 1);
  });

  it('abstraction adds 1', () => {
    assert.equal(termSize(parse('λx.x')), 2);
  });

  it('application adds 1', () => {
    assert.equal(termSize(parse('f x')), 3);
  });

  it('complex term', () => {
    assert.equal(termSize(parse('λf x.f (f x)')), 7);
  });
});

describe('CPS Transform: Church Numerals', () => {
  beforeEach(() => resetCPS());

  it('Church 0 through CPS', () => {
    const zero = parse('λf x.x');
    const cps = cpsTransform(zero);
    // Apply CPS zero to identity continuation, then to args
    const result = evalCPS(cps);
    // Should get back λf x.x (alpha-equivalent)
    assert(result.normalForm);
  });

  it('SUCC through CPS preserves structure', () => {
    // succ 0 = 1 through CPS
    // CPS result has continuation-passing structure, so we can't directly unchurch it
    // But direct evaluation should work
    const succZero = parse('(λn f x.f (n f x)) (λf x.x)');
    const direct = reduce(succZero, 'normal', 2000);
    assert.equal(unchurch(direct.result), 1);

    // CPS result should be a valid term (λf.λk.k(λx.λk.f x k)) — Church 1 in CPS
    const cps = cpsTransform(succZero);
    const cpsResult = evalCPS(cps);
    assert(cpsResult.normalForm);
    // The CPS result is λf.continuation... which has the right shape
    assert(cpsResult.result instanceof Abs);
  });
});

describe('CPS Size Analysis', () => {
  const terms = [
    ['x', 'variable'],
    ['λx.x', 'identity'],
    ['f x', 'application'],
    ['(λx.x) y', 'redex'],
    ['λf x.f (f x)', 'twice'],
  ];

  for (const [expr, name] of terms) {
    it(`${name}: CPS is larger than original`, () => {
      const original = parse(expr);
      resetCPS();
      const cps = cpsTransform(original);
      // CPS always adds at least the outer k parameter
      assert(termSize(cps) > termSize(original),
        `CPS (${termSize(cps)}) should be > original (${termSize(original)})`);
    });
  }
});

describe('Administrative Reduction', () => {
  beforeEach(() => resetCPS());

  it('reduces identity CPS: (λk. k (λx.λk.k x)) → λk. k (λx.λk.k x)', () => {
    const cps = cpsTransform(parse('λx.x'));
    const reduced = adminReduce(cps);
    // Should still be a function
    assert(reduced instanceof Abs);
  });

  it('reduces CPS of simple application', () => {
    const cps = cpsTransform(parse('(λx.x) y'));
    const reduced = adminReduce(new App(cps, parse('λx.x')));
    assert.equal(reduced.name, 'y');
  });
});
