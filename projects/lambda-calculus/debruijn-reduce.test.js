import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  shift, subst, betaReduce, step, reduce, normalizeDB, size,
} from './debruijn-reduce.js';
import {
  DeBruijnVar as DBVar, DeBruijnAbs as DBAbs, DeBruijnApp as DBApp,
  parse, toDeBruijn, fromDeBruijn, alphaEquivalent,
  reduce as namedReduce,
  church, churchNumeral, unchurch,
} from './lambda.js';

// ============================================================
// Shifting
// ============================================================

describe('Shift', () => {
  it('shifts free variable', () => {
    const t = shift(1, 0, new DBVar(0));
    assert.equal(t.index, 1);
  });

  it('does not shift bound variable', () => {
    // λ. 0 — the 0 is bound, shifting with c=1 shouldn't affect it
    const t = shift(1, 1, new DBVar(0));
    assert.equal(t.index, 0);
  });

  it('shifts under lambda', () => {
    // λ. 1 → shift(1, 0) → λ. 2 (free var 1 becomes 2)
    const t = shift(1, 0, new DBAbs(new DBVar(1)));
    assert.equal(t.body.index, 2);
  });

  it('does not shift bound in lambda', () => {
    // λ. 0 → shift(1, 0) → λ. 0 (0 is bound)
    const t = shift(1, 0, new DBAbs(new DBVar(0)));
    assert.equal(t.body.index, 0);
  });
});

// ============================================================
// Substitution
// ============================================================

describe('Substitution', () => {
  it('substitutes index 0', () => {
    const result = subst(0, new DBVar(5), new DBVar(0));
    assert.equal(result.index, 5);
  });

  it('leaves other indices', () => {
    const result = subst(0, new DBVar(5), new DBVar(1));
    assert.equal(result.index, 1);
  });

  it('substitutes under lambda', () => {
    // [0 → s] in λ. 1 should replace 1 (which is 0 outside the lambda)
    const result = subst(0, new DBVar(99), new DBAbs(new DBVar(1)));
    // Under the lambda, index 0 becomes 1, and s is shifted
    assert(result.body instanceof DBVar);
    assert.equal(result.body.index, 100); // shift(1,0,99) = 100
  });
});

// ============================================================
// Beta Reduction
// ============================================================

describe('Beta Reduction (de Bruijn)', () => {
  it('(λ. 0) x → x (identity)', () => {
    const body = new DBVar(0);
    const arg = new DBVar(5);
    const result = betaReduce(body, arg);
    assert.equal(result.index, 5);
  });

  it('(λ. 1) x → free var (constant)', () => {
    // λ. 1 applied to anything returns free var 0 (shifted down)
    const body = new DBVar(1);
    const arg = new DBVar(99);
    const result = betaReduce(body, arg);
    assert.equal(result.index, 0);
  });

  it('(λ. λ. 1) x → λ. x (K combinator)', () => {
    // K = λ. λ. 1 (second-to-last binding)
    const kBody = new DBAbs(new DBVar(1));
    const arg = new DBVar(5);
    const result = betaReduce(kBody, arg);
    // Should be λ. 6 (5 shifted up by 1 under the new lambda)
    assert(result instanceof DBAbs);
    assert.equal(result.body.index, 6);
  });
});

// ============================================================
// Full Reduction
// ============================================================

describe('Full Reduction', () => {
  it('reduces identity application', () => {
    // (λ. 0) 5 → 5
    const expr = new DBApp(new DBAbs(new DBVar(0)), new DBVar(5));
    const r = reduce(expr);
    assert.equal(r.result.index, 5);
    assert.equal(r.steps, 1);
  });

  it('reduces K combinator', () => {
    // (λ. λ. 1) 5 6 → 5
    const k = new DBAbs(new DBAbs(new DBVar(1)));
    const expr = new DBApp(new DBApp(k, new DBVar(5)), new DBVar(6));
    const r = reduce(expr);
    assert.equal(r.result.index, 5);
  });

  it('reduces under lambda', () => {
    // λ. (λ. 0) 0 → λ. 0
    const expr = new DBAbs(new DBApp(new DBAbs(new DBVar(0)), new DBVar(0)));
    const r = reduce(expr);
    assert(r.result instanceof DBAbs);
    assert.equal(r.result.body.index, 0);
  });

  it('reduces omega to non-termination', () => {
    // (λ. 0 0) (λ. 0 0)
    const omega = new DBAbs(new DBApp(new DBVar(0), new DBVar(0)));
    const expr = new DBApp(omega, omega);
    const r = reduce(expr, 10);
    assert(!r.normalForm);
    assert.equal(r.steps, 10);
  });
});

// ============================================================
// Agreement with Named Reduction
// ============================================================

describe('De Bruijn ≡ Named Reduction', () => {
  // Compare at de Bruijn level since fromDeBruijn can't recover free var names
  const testTerms = [
    'λx.(λy.y) x',
    '(λf.λx.f x) (λy.y)',
    '(λx.x x) (λy.y)',
  ];

  for (const t of testTerms) {
    it(`${t}: de Bruijn agrees with named`, () => {
      const term = parse(t);
      const namedResult = namedReduce(term, 'normal', 500).result;
      const namedDB = toDeBruijn(namedResult);
      
      const db = toDeBruijn(term);
      const dbResult = reduce(db);
      
      assert(namedDB.equals(dbResult.result),
        `Named: ${namedDB}, DB: ${dbResult.result}`);
    });
  }

  it('identity: (λ.0) applied gives same index', () => {
    // (λ.0) applied to index 5 gives 5
    const expr = new DBApp(new DBAbs(new DBVar(0)), new DBVar(5));
    const r = reduce(expr);
    assert.equal(r.result.index, 5);
  });

  it('K: (λ.λ.1) applied twice gives first arg', () => {
    const k = new DBAbs(new DBAbs(new DBVar(1)));
    const expr = new DBApp(new DBApp(k, new DBVar(10)), new DBVar(20));
    const r = reduce(expr);
    assert.equal(r.result.index, 10);
  });
});

// ============================================================
// Church Numerals via De Bruijn
// ============================================================

describe('Church Numerals (de Bruijn)', () => {
  it('SUCC 0 = 1', () => {
    const expr = parse('(λn f x.f (n f x)) (λf x.x)');
    const r = normalizeDB(expr);
    assert.equal(unchurch(r.result), 1);
  });

  it('PLUS 1 2 = 3', () => {
    const expr = parse('(λm n f x.m f (n f x)) (λf x.f x) (λf x.f (f x))');
    const r = normalizeDB(expr);
    assert.equal(unchurch(r.result), 3);
  });

  it('MULT 2 3 = 6', () => {
    const expr = parse('(λm n f.m (n f)) (λf x.f (f x)) (λf x.f (f (f x)))');
    const r = normalizeDB(expr);
    assert.equal(unchurch(r.result), 6);
  });
});

// ============================================================
// Size
// ============================================================

describe('Term Size', () => {
  it('variable has size 1', () => assert.equal(size(new DBVar(0)), 1));
  it('lambda adds 1', () => assert.equal(size(new DBAbs(new DBVar(0))), 2));
  it('app adds 1', () => assert.equal(size(new DBApp(new DBVar(0), new DBVar(1))), 3));
});
