import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  Combinator, CApp, CVar, S, K, I, B, C,
  lambdaToSKI, skiReduce, parseSKI, skiSize, toUnlambda, skiFreeIn,
} from './ski.js';
import {
  parse as parseLambda, reduce as reduceLambda,
  Var, Abs, App, alphaEquivalent, churchNumeral, unchurch,
} from './lambda.js';

describe('SKI Parser', () => {
  it('parses single combinator', () => {
    assert(parseSKI('S').equals(S));
    assert(parseSKI('K').equals(K));
    assert(parseSKI('I').equals(I));
  });

  it('parses application', () => {
    const r = parseSKI('SK');
    assert(r instanceof CApp);
    assert(r.func.equals(S));
    assert(r.arg.equals(K));
  });

  it('parses nested', () => {
    const r = parseSKI('SKK');
    assert(r instanceof CApp);
    assert(r.func instanceof CApp);
  });

  it('parses with parens', () => {
    const r = parseSKI('S(KS)K');
    assert(r instanceof CApp);
  });

  it('parses variable', () => {
    const r = parseSKI('Kx');
    assert(r instanceof CApp);
    assert(r.arg instanceof CVar);
    assert.equal(r.arg.name, 'x');
  });
});

describe('SKI Reduction', () => {
  it('I x → x', () => {
    const r = skiReduce(new CApp(I, new CVar('x')));
    assert.equal(r.result.toString(), 'x');
    assert.equal(r.steps, 1);
  });

  it('K x y → x', () => {
    const expr = new CApp(new CApp(K, new CVar('a')), new CVar('b'));
    const r = skiReduce(expr);
    assert.equal(r.result.toString(), 'a');
  });

  it('S K K x → x (identity)', () => {
    const expr = new CApp(new CApp(new CApp(S, K), K), new CVar('z'));
    const r = skiReduce(expr);
    assert.equal(r.result.toString(), 'z');
  });

  it('S f g x → f x (g x)', () => {
    const expr = new CApp(new CApp(new CApp(S, new CVar('f')), new CVar('g')), new CVar('x'));
    const r = skiReduce(expr);
    // Should be f x (g x)
    assert(r.result instanceof CApp);
    assert.equal(r.result.toString(), '(fx)(gx)');
  });

  it('B f g x → f (g x)', () => {
    const expr = new CApp(new CApp(new CApp(B, new CVar('f')), new CVar('g')), new CVar('x'));
    const r = skiReduce(expr);
    assert.equal(r.result.toString(), 'f(gx)');
  });

  it('C f x y → f y x', () => {
    const expr = new CApp(new CApp(new CApp(C, new CVar('f')), new CVar('x')), new CVar('y'));
    const r = skiReduce(expr);
    assert.equal(r.result.toString(), '(fy)x');
  });

  it('nested reduction: I (I x) → x', () => {
    const expr = new CApp(I, new CApp(I, new CVar('x')));
    const r = skiReduce(expr);
    assert.equal(r.result.toString(), 'x');
    assert.equal(r.steps, 2);
  });

  it('K I x y → y', () => {
    const expr = new CApp(new CApp(new CApp(K, I), new CVar('x')), new CVar('y'));
    const r = skiReduce(expr);
    assert.equal(r.result.toString(), 'y');
  });
});

describe('Lambda to SKI (basic)', () => {
  it('identity: λx.x → I', () => {
    const ski = lambdaToSKI(parseLambda('λx.x'));
    assert(ski.equals(I));
  });

  it('constant: λx.y → K y', () => {
    const ski = lambdaToSKI(parseLambda('λx.y'));
    assert(ski instanceof CApp);
    assert(ski.func.equals(K));
    assert.equal(ski.arg.toString(), 'y');
  });

  it('K combinator: λx y.x → S (K K) I... or similar', () => {
    // λx.λy.x
    const ski = lambdaToSKI(parseLambda('λx y.x'));
    // Reduce SKI with two args should give first
    const applied = new CApp(new CApp(ski, new CVar('a')), new CVar('b'));
    const r = skiReduce(applied, 200);
    assert.equal(r.result.toString(), 'a');
  });

  it('self-application: λx.x x', () => {
    const ski = lambdaToSKI(parseLambda('λx.x x'));
    // Apply to I should give I I → I
    const applied = new CApp(ski, I);
    const r = skiReduce(applied, 100);
    assert(r.result.equals(I));
  });

  it('S combinator: λf g x.f x (g x)', () => {
    const ski = lambdaToSKI(parseLambda('λf g x.f x (g x)'));
    // Apply to variables
    const applied = new CApp(new CApp(new CApp(ski, new CVar('p')), new CVar('q')), new CVar('r'));
    const r = skiReduce(applied, 500);
    // Should give p r (q r)
    assert(r.normalForm);
  });
});

describe('Lambda to SKI (optimized)', () => {
  it('identity: λx.x → I', () => {
    const ski = lambdaToSKI(parseLambda('λx.x'), true);
    assert(ski.equals(I));
  });

  it('eta reduction: λx.f x → f', () => {
    const ski = lambdaToSKI(parseLambda('λx.f x'), true);
    // Should just be f (eta-reduced)
    assert(ski instanceof CVar);
    assert.equal(ski.name, 'f');
  });

  it('optimized is smaller than basic', () => {
    const term = parseLambda('λf g x.f (g x)');
    const basic = lambdaToSKI(term, false);
    const optimized = lambdaToSKI(term, true);
    assert(skiSize(optimized) <= skiSize(basic),
      `Optimized (${skiSize(optimized)}) should be ≤ basic (${skiSize(basic)})`);
  });

  it('uses B combinator for composition', () => {
    const term = parseLambda('λx.f (g x)');
    const ski = lambdaToSKI(term, true);
    // Should be B f g (composition)
    assert.equal(ski.toString(), '(Bf)g');
  });

  it('uses C combinator for flip', () => {
    const term = parseLambda('λx.f x y');
    const ski = lambdaToSKI(term, true);
    // Should use C: C f y
    assert(ski.toString().includes('C'));
  });
});

describe('Lambda ↔ SKI Equivalence', () => {
  function testEquivalence(lambdaStr, args) {
    const lambdaTerm = parseLambda(lambdaStr);
    const skiTerm = lambdaToSKI(lambdaTerm, true);

    // Apply both to the same arguments
    let lambdaApp = lambdaTerm;
    let skiApp = skiTerm;
    for (const arg of args) {
      lambdaApp = new App(lambdaApp, new Var(arg));
      skiApp = new CApp(skiApp, new CVar(arg));
    }

    const lambdaResult = reduceLambda(lambdaApp, 'normal', 1000);
    const skiResult = skiReduce(skiApp, 1000);

    return { lambdaResult: lambdaResult.result.toString(), skiResult: skiResult.result.toString() };
  }

  it('identity equivalence', () => {
    const r = testEquivalence('λx.x', ['a']);
    assert.equal(r.lambdaResult, 'a');
    assert.equal(r.skiResult, 'a');
  });

  it('K equivalence', () => {
    const r = testEquivalence('λx y.x', ['a', 'b']);
    assert.equal(r.lambdaResult, 'a');
    assert.equal(r.skiResult, 'a');
  });

  it('K* equivalence', () => {
    const r = testEquivalence('λx y.y', ['a', 'b']);
    assert.equal(r.lambdaResult, 'b');
    assert.equal(r.skiResult, 'b');
  });

  it('composition equivalence: λf g x.f (g x)', () => {
    const lambdaTerm = parseLambda('λf g x.f (g x)');
    const skiTerm = lambdaToSKI(lambdaTerm, true);

    let skiApp = skiTerm;
    skiApp = new CApp(skiApp, new CVar('p'));
    skiApp = new CApp(skiApp, new CVar('q'));
    skiApp = new CApp(skiApp, new CVar('r'));
    const r = skiReduce(skiApp, 500);
    // Should be p(qr)
    assert.equal(r.result.toString(), 'p(qr)');
  });
});

describe('Size Metrics', () => {
  it('combinator has size 1', () => {
    assert.equal(skiSize(S), 1);
    assert.equal(skiSize(K), 1);
    assert.equal(skiSize(I), 1);
  });

  it('application adds sizes', () => {
    assert.equal(skiSize(new CApp(S, K)), 2);
    assert.equal(skiSize(new CApp(new CApp(S, K), I)), 3);
  });

  it('complex expression size', () => {
    const ski = lambdaToSKI(parseLambda('λx y.x'));
    assert(skiSize(ski) > 0);
  });
});

describe('Unlambda Notation', () => {
  it('converts S to s', () => {
    assert.equal(toUnlambda(S), 's');
  });

  it('converts application with backtick', () => {
    assert.equal(toUnlambda(new CApp(S, K)), '`sk');
  });

  it('converts SKI to unlambda', () => {
    const skk = new CApp(new CApp(S, K), K);
    assert.equal(toUnlambda(skk), '``skk');
  });

  it('converts nested application', () => {
    const expr = new CApp(new CApp(new CApp(S, K), K), new CVar('x'));
    assert.equal(toUnlambda(expr), '```skkx');
  });
});

describe('Free Variable Check', () => {
  it('variable is free', () => {
    assert(skiFreeIn('x', new CVar('x')));
  });

  it('different variable is not free', () => {
    assert(!skiFreeIn('x', new CVar('y')));
  });

  it('combinator has no free vars', () => {
    assert(!skiFreeIn('x', S));
    assert(!skiFreeIn('x', K));
  });

  it('free in application', () => {
    assert(skiFreeIn('x', new CApp(new CVar('x'), K)));
    assert(skiFreeIn('x', new CApp(K, new CVar('x'))));
  });
});

describe('Combinator Size Comparison', () => {
  const terms = [
    'λx.x',
    'λx y.x',
    'λx y.y',
    'λf g x.f x (g x)',
    'λf g x.f (g x)',
    'λf x y.f y x',
    'λf x.f x x',
  ];

  for (const t of terms) {
    it(`${t}: optimized ≤ basic`, () => {
      const term = parseLambda(t);
      const basic = lambdaToSKI(term, false);
      const opt = lambdaToSKI(term, true);
      assert(skiSize(opt) <= skiSize(basic),
        `Optimized ${skiSize(opt)} > basic ${skiSize(basic)} for ${t}`);
    });
  }
});

describe('Church Numerals via SKI', () => {
  it('Church 0 through SKI', () => {
    const zero = parseLambda('λf x.x');
    const ski = lambdaToSKI(zero, true);
    // Apply to inc and 0 (as variables)
    const applied = new CApp(new CApp(ski, new CVar('f')), new CVar('z'));
    const r = skiReduce(applied, 100);
    assert.equal(r.result.toString(), 'z');
  });

  it('Church 1 through SKI', () => {
    const one = parseLambda('λf x.f x');
    const ski = lambdaToSKI(one, true);
    const applied = new CApp(new CApp(ski, new CVar('f')), new CVar('z'));
    const r = skiReduce(applied, 100);
    assert.equal(r.result.toString(), 'fz');
  });

  it('Church 2 through SKI', () => {
    const two = parseLambda('λf x.f (f x)');
    const ski = lambdaToSKI(two, true);
    const applied = new CApp(new CApp(ski, new CVar('f')), new CVar('z'));
    const r = skiReduce(applied, 200);
    assert.equal(r.result.toString(), 'f(fz)');
  });
});

describe('Omega in SKI', () => {
  it('omega diverges', () => {
    // ω = λx.x x → S I I
    const omega = lambdaToSKI(parseLambda('λx.x x'));
    const omegaOmega = new CApp(omega, omega.clone());
    const r = skiReduce(omegaOmega, 20);
    assert(!r.normalForm);
    assert.equal(r.steps, 20);
  });
});
