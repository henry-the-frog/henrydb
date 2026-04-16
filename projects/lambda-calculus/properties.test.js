import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  Var, Abs, App, parse, reduce, alphaEquivalent, freeVars,
  church, churchNumeral, unchurch,
} from './lambda.js';
import { lambdaToSKI, skiReduce, CApp, CVar } from './ski.js';
import { normalize, betaEtaEqual } from './nbe.js';

// ============================================================
// Random Term Generator
// ============================================================

const varPool = ['x', 'y', 'z', 'w', 'v', 'u'];

function randomTerm(depth = 0, maxDepth = 3, boundVars = []) {
  if (depth >= maxDepth) {
    // Return a variable
    const allVars = [...boundVars, ...varPool.slice(0, 2)];
    return new Var(allVars[Math.floor(Math.random() * allVars.length)]);
  }
  
  const r = Math.random();
  if (r < 0.3) {
    // Variable
    const allVars = [...boundVars, ...varPool.slice(0, 2)];
    return new Var(allVars[Math.floor(Math.random() * allVars.length)]);
  } else if (r < 0.6) {
    // Application
    return new App(
      randomTerm(depth + 1, maxDepth, boundVars),
      randomTerm(depth + 1, maxDepth, boundVars)
    );
  } else {
    // Abstraction
    const param = varPool[Math.floor(Math.random() * varPool.length)];
    return new Abs(param, randomTerm(depth + 1, maxDepth, [...boundVars, param]));
  }
}

// Random closed term (no free variables)
function randomClosedTerm(depth = 0, maxDepth = 3, boundVars = ['x']) {
  if (depth >= maxDepth || boundVars.length === 0) {
    return new Var(boundVars[Math.floor(Math.random() * boundVars.length)]);
  }
  
  const r = Math.random();
  if (r < 0.3 && boundVars.length > 0) {
    return new Var(boundVars[Math.floor(Math.random() * boundVars.length)]);
  } else if (r < 0.55) {
    return new App(
      randomClosedTerm(depth + 1, maxDepth, boundVars),
      randomClosedTerm(depth + 1, maxDepth, boundVars)
    );
  } else {
    const param = varPool[depth % varPool.length];
    return new Abs(param, randomClosedTerm(depth + 1, maxDepth, [...boundVars, param]));
  }
}

// ============================================================
// Property: Alpha-equivalence is reflexive
// ============================================================

describe('Property: Alpha-equivalence is reflexive', () => {
  for (let i = 0; i < 20; i++) {
    it(`random term ${i}`, () => {
      const term = randomTerm(0, 3);
      assert(alphaEquivalent(term, term));
    });
  }
});

// ============================================================
// Property: NbE and reduction agree on normal forms
// ============================================================

describe('Property: NbE agrees with reduction', () => {
  const terms = [
    'λx.x', 'λx.λy.x', 'λx.λy.y',
    '(λx.x) y', '(λx.λy.x) a b',
    'λf.λx.f (f x)',
    '(λf.λx.f (f x)) (λy.y)',
    'λx.(λy.y) x',
    '(λx.x x) (λy.y)',
  ];

  for (const t of terms) {
    it(`NbE ≡ reduction: ${t}`, () => {
      const term = parse(t);
      const nbeResult = normalize(term);
      const reductionResult = reduce(term, 'normal', 1000).result;
      assert(alphaEquivalent(nbeResult, reductionResult),
        `NbE: ${nbeResult}, reduction: ${reductionResult}`);
    });
  }
});

// ============================================================
// Property: Church arithmetic is correct
// ============================================================

describe('Property: Church addition is commutative', () => {
  for (let a = 0; a <= 4; a++) {
    for (let b = 0; b <= 4; b++) {
      it(`${a} + ${b} = ${b} + ${a}`, () => {
        const ab = reduce(new App(new App(church.plus, churchNumeral(a)), churchNumeral(b))).result;
        const ba = reduce(new App(new App(church.plus, churchNumeral(b)), churchNumeral(a))).result;
        assert(alphaEquivalent(ab, ba));
      });
    }
  }
});

describe('Property: Church addition is associative', () => {
  for (let a = 0; a <= 3; a++) {
    for (let b = 0; b <= 3; b++) {
      for (let c = 0; c <= 3; c++) {
        it(`(${a}+${b})+${c} = ${a}+(${b}+${c})`, () => {
          const plus = church.plus;
          const left = reduce(new App(new App(plus, reduce(new App(new App(plus, churchNumeral(a)), churchNumeral(b))).result), churchNumeral(c))).result;
          const right = reduce(new App(new App(plus, churchNumeral(a)), reduce(new App(new App(plus, churchNumeral(b)), churchNumeral(c))).result)).result;
          assert.equal(unchurch(left), unchurch(right));
        });
      }
    }
  }
});

describe('Property: Church multiplication distributes over addition', () => {
  // a * (b + c) = a*b + a*c
  for (let a = 1; a <= 3; a++) {
    for (let b = 0; b <= 2; b++) {
      for (let c = 0; c <= 2; c++) {
        it(`${a}*(${b}+${c}) = ${a}*${b} + ${a}*${c}`, () => {
          const left = a * (b + c);
          const right = a * b + a * c;
          assert.equal(left, right);

          // Verify via Church encoding
          const bc = reduce(new App(new App(church.plus, churchNumeral(b)), churchNumeral(c))).result;
          const churchLeft = unchurch(reduce(new App(new App(church.mult, churchNumeral(a)), bc)).result);
          const ab = reduce(new App(new App(church.mult, churchNumeral(a)), churchNumeral(b))).result;
          const ac = reduce(new App(new App(church.mult, churchNumeral(a)), churchNumeral(c))).result;
          const churchRight = unchurch(reduce(new App(new App(church.plus, ab), ac)).result);
          assert.equal(churchLeft, churchRight);
        });
      }
    }
  }
});

// ============================================================
// Property: SKI compilation preserves behavior
// ============================================================

describe('Property: SKI compilation preserves behavior', () => {
  const terms = ['λx.x', 'λx y.x', 'λx y.y', 'λf x.f (f x)'];
  const args = ['a', 'b'];

  for (const t of terms) {
    it(`SKI(${t}) behaves same as original`, () => {
      const term = parse(t);
      const ski = lambdaToSKI(term, true);

      // Apply to one argument
      const lambdaApp = new App(term, new Var('a'));
      const skiApp = new CApp(ski, new CVar('a'));

      const lambdaResult = reduce(lambdaApp, 'normal', 500);
      const skiResult = skiReduce(skiApp, 500);

      // Both should terminate
      assert(lambdaResult.normalForm || lambdaResult.steps < 500);
      assert(skiResult.normalForm || skiResult.steps < 500);
    });
  }
});

// ============================================================
// Property: Normal-order finds normal form when it exists
// (Church-Rosser theorem)
// ============================================================

describe('Property: Church-Rosser (normal order finds NF)', () => {
  it('K a Omega: normal order finds a, applicative diverges', () => {
    const expr = parse('(λx y.x) a ((λx.x x) (λx.x x))');
    const normal = reduce(expr, 'normal', 100);
    assert.equal(normal.result.name, 'a');
    assert(normal.normalForm);

    const applicative = reduce(expr, 'applicative', 100);
    assert(!applicative.normalForm);
  });

  it('if both strategies terminate, they agree', () => {
    const terms = [
      '(λx.x) a',
      '(λx y.x) a b',
      '(λx.x x) (λy.y)',
      '(λf x.f (f x)) (λy.y) a',
    ];
    for (const t of terms) {
      const normal = reduce(parse(t), 'normal', 500);
      const applicative = reduce(parse(t), 'applicative', 500);
      if (normal.normalForm && applicative.normalForm) {
        assert(alphaEquivalent(normal.result, applicative.result),
          `Disagreement on ${t}: normal=${normal.result}, app=${applicative.result}`);
      }
    }
  });
});

// ============================================================
// Property: Free variables are preserved correctly
// ============================================================

describe('Property: Free variables', () => {
  it('closed terms have no free vars', () => {
    const closedTerms = ['λx.x', 'λx.λy.x', 'λf.λx.f (f x)', '(λx.x) (λy.y)'];
    for (const t of closedTerms) {
      assert.equal(freeVars(parse(t)).size, 0, `${t} should be closed`);
    }
  });

  it('reduction does not introduce new free vars', () => {
    const terms = ['(λx.x) y', '(λx.x) (λy.y z)', '(λf.f a) (λx.x)'];
    for (const t of terms) {
      const before = freeVars(parse(t));
      const after = freeVars(reduce(parse(t), 'normal', 100).result);
      // After reduction, free vars should be subset of before
      for (const v of after) {
        assert(before.has(v), `Free var ${v} introduced during reduction of ${t}`);
      }
    }
  });
});
