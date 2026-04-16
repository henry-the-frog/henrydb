import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  VNeutral, VLam,
  evaluate, normalize, betaEtaEqual, resetReadback,
} from './nbe.js';
import {
  parse, Var, Abs, App, alphaEquivalent, reduce,
  church, churchNumeral, unchurch,
} from './lambda.js';

describe('NbE: Evaluate', () => {
  it('variable evaluates to neutral', () => {
    const val = evaluate(parse('x'));
    assert(val instanceof VNeutral);
    assert.equal(val.head, 'x');
  });

  it('lambda evaluates to closure', () => {
    const val = evaluate(parse('λx.x'));
    assert(val instanceof VLam);
  });

  it('application of identity', () => {
    const val = evaluate(parse('(λx.x) y'));
    assert(val instanceof VNeutral);
    assert.equal(val.head, 'y');
  });

  it('K combinator application', () => {
    const val = evaluate(parse('(λx.λy.x) a b'));
    assert(val instanceof VNeutral);
    assert.equal(val.head, 'a');
  });
});

describe('NbE: Normalize', () => {
  beforeEach(() => resetReadback());

  it('normalizes identity', () => {
    const nf = normalize(parse('λx.x'));
    assert(nf instanceof Abs);
    assert(nf.body instanceof Var);
    assert.equal(nf.body.name, nf.param);
  });

  it('normalizes identity application', () => {
    const nf = normalize(parse('(λx.x) y'));
    assert(nf instanceof Var);
    assert.equal(nf.name, 'y');
  });

  it('normalizes K combinator', () => {
    const nf = normalize(parse('(λx.λy.x) a b'));
    assert(nf instanceof Var);
    assert.equal(nf.name, 'a');
  });

  it('normalizes under lambda', () => {
    const nf = normalize(parse('λx.(λy.y) x'));
    assert(nf instanceof Abs);
    assert(nf.body instanceof Var);
    assert.equal(nf.body.name, nf.param);
  });

  it('normalizes S combinator application', () => {
    const nf = normalize(parse('(λf.λg.λx.f x (g x)) (λa.λb.a) (λa.λb.a) z'));
    assert.equal(nf.name, 'z');
  });

  it('normalizes complex nested redex', () => {
    const nf = normalize(parse('(λx.x x) (λy.y)'));
    assert(alphaEquivalent(nf, parse('λy.y')));
  });
});

describe('NbE: Beta-Eta Equality', () => {
  it('identity variants are equal', () => {
    assert(betaEtaEqual(parse('λx.x'), parse('λy.y')));
  });

  it('applied K is equal to constant', () => {
    assert(betaEtaEqual(parse('(λx.λy.x) a'), parse('λy.a')));
  });

  it('different terms are not equal', () => {
    assert(!betaEtaEqual(parse('λx.x'), parse('λx.λy.x')));
  });

  it('beta-equivalent terms', () => {
    assert(betaEtaEqual(
      parse('(λf.λx.f x) (λy.y)'),
      parse('λx.x')
    ));
  });

  it('S K K = I', () => {
    const skk = parse('(λf.λg.λx.f x (g x)) (λx.λy.x) (λx.λy.x)');
    const id = parse('λx.x');
    assert(betaEtaEqual(skk, id));
  });

  it('K applied to different values are different', () => {
    assert(!betaEtaEqual(parse('(λx.λy.x) a'), parse('(λx.λy.x) b')));
  });
});

describe('NbE: Church Numerals', () => {
  it('normalizes Church 0', () => {
    const nf = normalize(church.zero);
    // λf.λx.x
    assert(nf instanceof Abs);
    assert(nf.body instanceof Abs);
    assert.equal(nf.body.body.name, nf.body.param);
  });

  it('normalizes SUCC 0 = 1', () => {
    const succ0 = new App(church.succ, church.zero);
    const nf = normalize(succ0);
    // Should normalize to λf.λx.f x
    assert(nf instanceof Abs); // λf
    assert(nf.body instanceof Abs); // λx
    // f x
    assert(nf.body.body instanceof App);
  });

  it('normalizes PLUS 1 2 = 3', () => {
    const plus12 = new App(new App(church.plus, church.one), church.two);
    const nf = normalize(plus12);
    const n = unchurch(nf);
    assert.equal(n, 3);
  });

  it('normalizes MULT 2 3 = 6', () => {
    const mult23 = new App(new App(church.mult, church.two), church.three);
    const nf = normalize(mult23);
    assert.equal(unchurch(nf), 6);
  });

  it('PLUS is commutative via NbE', () => {
    const a = normalize(new App(new App(church.plus, church.one), church.two));
    const b = normalize(new App(new App(church.plus, church.two), church.one));
    assert(alphaEquivalent(a, b));
  });

  it('normalizes PRED 3 = 2', () => {
    const pred3 = new App(church.pred, church.three);
    const nf = normalize(pred3);
    assert.equal(unchurch(nf), 2);
  });
});

describe('NbE vs Reduction: Equivalence', () => {
  const testTerms = [
    '(λx.x) y',
    '(λx.λy.x) a b',
    '(λf.λx.f (f x)) (λy.y)',
    '(λf.λg.λx.f x (g x)) (λa.λb.a) (λa.λb.a)',
    'λx.(λy.y) x',
  ];

  for (const t of testTerms) {
    it(`NbE ≡ reduction for: ${t}`, () => {
      const nbe = normalize(parse(t));
      const reduced = reduce(parse(t), 'normal', 1000).result;
      assert(alphaEquivalent(nbe, reduced),
        `NbE: ${nbe}, Reduction: ${reduced}`);
    });
  }
});

describe('NbE: Performance Advantage', () => {
  it('normalizes deeply nested application efficiently', () => {
    // Build (I (I (I ... (I x) ...))) with 100 applications
    let term = new Var('x');
    const id = parse('λx.x');
    for (let i = 0; i < 100; i++) {
      term = new App(id, term);
    }
    const nf = normalize(term);
    assert(nf instanceof Var);
    assert.equal(nf.name, 'x');
  });

  it('normalizes church numeral computation quickly', () => {
    // 5 + 5 = 10
    const expr = new App(new App(church.plus, churchNumeral(5)), churchNumeral(5));
    const nf = normalize(expr);
    assert.equal(unchurch(nf), 10);
  });
});
