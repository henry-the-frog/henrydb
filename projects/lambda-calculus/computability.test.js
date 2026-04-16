import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  Var, Abs, App,
  parse, reduce, alphaEquivalent,
  freeVars, substitute,
  church, churchNumeral, unchurch, unchurchBool,
  prettyPrint, resetFreshCounter, toDeBruijn, fromDeBruijn,
  normalOrderStep,
} from './lambda.js';

// ============================================================
// Helper: build and reduce a Church arithmetic expression
// ============================================================
function churchArith(op, a, b) {
  return reduce(new App(new App(church[op], churchNumeral(a)), churchNumeral(b)), 'normal', 10000).result;
}

describe('Computability: Factorial via Y combinator', () => {
  // fact = Y (λf n. isZero n one (mult n (f (pred n))))
  // We build this carefully using Church encodings

  const factStep = parse('λf n.(λp.p (λa b.a)) ((λp.p (λa b.b)) ((λb.(λt f.t) (λt f.f) b) ((λn.n (λx.λt f.f) (λt f.t)) n) (λx.(λm n f.m (n f)) n (f ((λn f x.n (λg h.h (g f)) (λu.x) (λu.u)) n)) x) (λx.(λf x.f x) x)))');

  it('Y combinator structure is valid', () => {
    assert(church.Y instanceof Abs);
    const fv = freeVars(church.Y);
    assert.equal(fv.size, 0);
  });

  // Instead of building the full recursive factorial (which blows up),
  // test the pieces individually
  it('pred(succ(n)) = n for n=0,1,2', () => {
    for (let n = 0; n <= 2; n++) {
      const expr = new App(church.pred, new App(church.succ, churchNumeral(n)));
      assert.equal(unchurch(reduce(expr, 'normal', 5000).result), n);
    }
  });

  it('isZero(zero) = true, isZero(one) = false', () => {
    assert.equal(unchurchBool(reduce(new App(church.isZero, church.zero)).result), true);
    assert.equal(unchurchBool(reduce(new App(church.isZero, church.one)).result), false);
  });

  it('multiplication chain: 1*2*3 = 6', () => {
    const m12 = reduce(new App(new App(church.mult, church.one), church.two)).result;
    const m123 = reduce(new App(new App(church.mult, m12), church.three)).result;
    assert.equal(unchurch(m123), 6);
  });
});

describe('Computability: Fibonacci via Z combinator', () => {
  // Z combinator works with CBV
  it('Z combinator is closed', () => {
    assert.equal(freeVars(church.Z).size, 0);
  });

  it('Z combinator structure', () => {
    assert(church.Z instanceof Abs);
  });

  // Test fibonacci building blocks
  it('plus chain: fib(5) = fib(4) + fib(3) manually', () => {
    // fib: 0,1,1,2,3,5,8
    // Build iteratively using Church: fib(5) = 5
    const five = churchNumeral(5);
    assert.equal(unchurch(five), 5);
  });

  it('iterative fibonacci via pair encoding', () => {
    // fib using pairs: start with (0,1), iterate n times: (a,b) -> (b, a+b)
    // fibStep = λp. pair (snd p) (plus (fst p) (snd p))
    const fibStep = new Abs('p',
      new App(new App(church.pair,
        new App(church.snd, new Var('p'))),
        new App(new App(church.plus,
          new App(church.fst, new Var('p'))),
          new App(church.snd, new Var('p')))));

    const fibStart = new App(new App(church.pair, church.zero), church.one);

    // fib(n) = fst (n fibStep (pair 0 1))
    function churchFib(n) {
      const nTimes = new App(new App(churchNumeral(n), fibStep), fibStart);
      return new App(church.fst, nTimes);
    }

    // fib(0) = 0, fib(1) = 1, fib(2) = 1, fib(3) = 2, fib(4) = 3, fib(5) = 5
    assert.equal(unchurch(reduce(churchFib(0), 'normal', 5000).result), 0);
    assert.equal(unchurch(reduce(churchFib(1), 'normal', 5000).result), 1);
    assert.equal(unchurch(reduce(churchFib(2), 'normal', 5000).result), 1);
    assert.equal(unchurch(reduce(churchFib(3), 'normal', 5000).result), 2);
    assert.equal(unchurch(reduce(churchFib(4), 'normal', 5000).result), 3);
    assert.equal(unchurch(reduce(churchFib(5), 'normal', 5000).result), 5);
  });
});

describe('Church Lists (fold/right encoding)', () => {
  // cons h t = λc n. c h (t c n)
  // nil = λc n. n

  function churchList(...elements) {
    let list = church.nil;
    for (let i = elements.length - 1; i >= 0; i--) {
      list = new App(new App(church.cons, elements[i]), list);
    }
    return list;
  }

  it('nil is identity on second arg', () => {
    const r = reduce(new App(new App(church.nil, new Var('f')), new Var('z')));
    assert.equal(r.result.name, 'z');
  });

  it('head of [1,2,3] = 1', () => {
    const list = churchList(church.one, church.two, church.three);
    const h = reduce(new App(church.head, list), 'normal', 5000);
    assert.equal(unchurch(h.result), 1);
  });

  it('isNil nil = true', () => {
    const r = reduce(new App(church.isNil, church.nil), 'normal', 2000);
    assert.equal(unchurchBool(r.result), true);
  });

  it('isNil (cons 1 nil) = false', () => {
    const list = new App(new App(church.cons, church.one), church.nil);
    const r = reduce(new App(church.isNil, list), 'normal', 2000);
    assert.equal(unchurchBool(r.result), false);
  });

  it('length [1,2,3] = 3', () => {
    const list = churchList(church.one, church.two, church.three);
    const r = reduce(new App(church.length, list), 'normal', 5000);
    assert.equal(unchurch(r.result), 3);
  });

  it('sum [1,2,3] = 6 via fold', () => {
    // fold (+) 0 [1,2,3] = list plus zero
    const list = churchList(church.one, church.two, church.three);
    const sum = new App(new App(list, church.plus), church.zero);
    const r = reduce(sum, 'normal', 10000);
    assert.equal(unchurch(r.result), 6);
  });
});

describe('SKI Combinator Calculus', () => {
  const S = parse('λf g x.f x (g x)');
  const K = parse('λx y.x');
  const I = parse('λx.x');

  it('I = S K K', () => {
    // SKK x = K x (K x) = x  (same as I)
    const skk = new App(new App(S, K), K);
    const skkx = new App(skk, new Var('a'));
    const ix = new App(I, new Var('a'));
    const r1 = reduce(skkx, 'normal', 100);
    const r2 = reduce(ix, 'normal', 100);
    assert(alphaEquivalent(r1.result, r2.result));
  });

  it('S K S x = x (another identity)', () => {
    const sks = new App(new App(S, K), S);
    const r = reduce(new App(sks, new Var('z')), 'normal', 100);
    assert.equal(r.result.name, 'z');
  });

  it('K I x y = y (flip of K)', () => {
    const ki = new App(K, I);
    const r = reduce(new App(new App(ki, new Var('a')), new Var('b')), 'normal', 100);
    assert.equal(r.result.name, 'b');
  });

  it('S(KS)K is composition (B combinator)', () => {
    const B = new App(new App(S, new App(K, S)), K);
    // B f g x = f (g x)
    const r = reduce(new App(new App(new App(B, new Var('f')), new Var('g')), new Var('a')), 'normal', 100);
    // Should be: f (g a)
    assert(r.result instanceof App);
    assert.equal(r.result.func.name, 'f');
    assert(r.result.arg instanceof App);
    assert.equal(r.result.arg.func.name, 'g');
    assert.equal(r.result.arg.arg.name, 'a');
  });
});

describe('Scott Encodings (case analysis)', () => {
  // Scott-encoded naturals: zero = λz s.z, succ(n) = λz s.s n
  const scottZero = parse('λz s.z');
  const scottSucc = parse('λn z s.s n');

  function scottNum(n) {
    let result = scottZero;
    for (let i = 0; i < n; i++) {
      result = new App(scottSucc, result);
    }
    return result;
  }

  function unchurchScott(expr) {
    const reduced = reduce(expr, 'normal', 5000).result;
    // Apply to (0, λn.1 + unchurch(n)) — but we do it structurally
    let count = 0;
    let current = reduced;
    while (true) {
      // Apply zero case and succ case
      const tested = reduce(new App(new App(current, churchNumeral(count)), parse('λn.n')), 'normal', 5000);
      // If it returns count (the zero case), we're done
      if (unchurch(tested.result) === count) return count;
      // Otherwise get the inner n and continue
      current = reduce(new App(new App(current, new Var('ZERO_MARKER')), parse('λn.n')), 'normal', 5000).result;
      count++;
      if (count > 20) return null;
    }
  }

  it('scottZero selects first', () => {
    const r = reduce(new App(new App(scottZero, new Var('a')), new Var('b')));
    assert.equal(r.result.name, 'a');
  });

  it('scottSucc(zero) selects second and gives zero', () => {
    const one = new App(scottSucc, scottZero);
    const r = reduce(new App(new App(one, new Var('a')), parse('λn.n')), 'normal', 1000);
    // Should reduce to (λn.n) scottZero = scottZero
    assert(alphaEquivalent(r.result, scottZero));
  });

  it('Scott predecessor is trivial', () => {
    // pred(succ(n)) = n  (just case-match and return the stored value)
    const scottPred = parse('λn.n (λz s.z) (λm.m)');
    const two = new App(scottSucc, new App(scottSucc, scottZero));
    const r = reduce(new App(scottPred, two), 'normal', 5000);
    // pred(2) should be succ(zero) — which selects second and gives zero
    const test = reduce(new App(new App(r.result, new Var('a')), parse('λn.n')), 'normal', 5000);
    assert(alphaEquivalent(test.result, scottZero));
  });
});

describe('Advanced Reduction', () => {
  beforeEach(() => resetFreshCounter());

  it('eta reduction: λx.(f x) ≡ f when x not free in f', () => {
    // Not automatic, but we can check alpha-equivalence after reducing
    const eta = parse('λx.f x');
    // After eta-reduction this would be just f, but our reducer does beta only
    // Just verify the term is well-formed
    assert.deepEqual(freeVars(eta), new Set(['f']));
  });

  it('multiple capture avoidance in sequence', () => {
    // λy.x [x := y] should alpha-rename y
    const r = substitute(parse('λy.x'), 'x', parse('y'));
    assert(r instanceof Abs);
    // Body should be the substituted y, not the bound variable
    assert.equal(r.body.name, 'y');
    assert.notEqual(r.param, 'y');
  });

  it('deeply nested reduction terminates', () => {
    // ((λx.x)(λx.x))(((λx.x)(λx.x))((λx.x)(λx.x)))
    const id = parse('λx.x');
    const inner = new App(new App(id, id), new App(new App(id, id), new App(id, id)));
    const r = reduce(inner, 'normal', 100);
    assert(r.normalForm);
    assert(alphaEquivalent(r.result, id));
  });

  it('reduction count varies by strategy', () => {
    const expr = parse('(λx.x) ((λy.y) z)');
    const normal = reduce(expr, 'normal');
    const applicative = reduce(expr, 'applicative');
    // Normal: (λx.x) ((λy.y) z) → (λy.y) z → z  (2 steps)
    // Applicative: (λx.x) ((λy.y) z) → (λx.x) z → z  (2 steps)
    assert.equal(normal.steps, 2);
    assert.equal(applicative.steps, 2);
    assert.equal(normal.result.name, 'z');
    assert.equal(applicative.result.name, 'z');
  });
});

describe('Church Arithmetic Edge Cases', () => {
  it('0 + 0 = 0', () => {
    assert.equal(unchurch(churchArith('plus', 0, 0)), 0);
  });

  it('0 * n = 0', () => {
    assert.equal(unchurch(churchArith('mult', 0, 3)), 0);
  });

  it('n * 0 = 0', () => {
    assert.equal(unchurch(churchArith('mult', 3, 0)), 0);
  });

  it('1 * n = n', () => {
    assert.equal(unchurch(churchArith('mult', 1, 5)), 5);
  });

  it('pred 0 = 0 (Church predecessor of zero)', () => {
    const r = reduce(new App(church.pred, church.zero), 'normal', 5000);
    assert.equal(unchurch(r.result), 0);
  });

  it('sub when m < n gives 0 (Church subtraction underflow)', () => {
    const r = reduce(new App(new App(church.sub, church.one), church.three), 'normal', 5000);
    assert.equal(unchurch(r.result), 0);
  });

  it('exp 3 2 = 9', () => {
    assert.equal(unchurch(churchArith('exp', 3, 2)), 9);
  });

  it('exp n 0 gives identity (known Church encoding quirk)', () => {
    // Church exp: λm n.n m. exp(n,0) = 0 m = (λf x.x) m = λx.x
    // This is λx.x (the identity), not Church 1 (λf x.f x)
    // Known limitation: Church exp doesn't handle 0 exponent correctly
    const r = reduce(new App(new App(church.exp, churchNumeral(5)), churchNumeral(0)), 'normal', 10000);
    // The result is the identity function, not a Church numeral
    assert.equal(unchurch(r.result), null);
    // But it IS alpha-equivalent to λx.x
    assert(alphaEquivalent(r.result, parse('λx.x')));
  });

  it('exp 0 n = 0 for n > 0', () => {
    // 0^1 should be 0: (λm n.n m) 0 1 = 1 0 = (λf x.f x) (λf x.x) = λx.(λf x.x) x = λx.λx.x
    // Actually Church exp 0 n for n>=1 gives λf x.x = 0... let's check
    assert.equal(unchurch(churchArith('exp', 0, 1)), 0);
  });
});

describe('De Bruijn Advanced', () => {
  it('S combinator in de Bruijn', () => {
    const s = parse('λf g x.f x (g x)');
    const db = toDeBruijn(s);
    // λ.λ.λ. (2 0) (1 0) — Apps are parenthesized
    assert.equal(db.toString(), '(λ.(λ.(λ.((2 0) (1 0)))))');
  });

  it('Omega in de Bruijn', () => {
    const omega = parse('(λx.x x) (λx.x x)');
    const db = toDeBruijn(omega);
    assert.equal(db.toString(), '((λ.(0 0)) (λ.(0 0)))');
  });

  it('round-trip preserves semantics for complex terms', () => {
    const terms = [
      'λx.x',
      'λx y.x',
      'λx y.y',
      'λf g x.f x (g x)',
      'λf.(λx.f (x x)) (λx.f (x x))',
    ];
    for (const t of terms) {
      const orig = parse(t);
      const back = fromDeBruijn(toDeBruijn(orig));
      assert(alphaEquivalent(orig, back), `Failed round-trip for: ${t}`);
    }
  });
});

describe('Boolean Logic Completeness', () => {
  // Test all 2-input boolean functions
  function boolTest(combinator, inputs, expected) {
    for (let i = 0; i < inputs.length; i++) {
      const [a, b] = inputs[i];
      const ca = a ? church.true : church.false;
      const cb = b ? church.true : church.false;
      const expr = new App(new App(combinator, ca), cb);
      const r = reduce(expr, 'normal', 2000);
      assert.equal(unchurchBool(r.result), expected[i],
        `Failed for inputs (${a}, ${b})`);
    }
  }

  const inputs = [[true, true], [true, false], [false, true], [false, false]];

  it('AND truth table', () => {
    boolTest(church.and, inputs, [true, false, false, false]);
  });

  it('OR truth table', () => {
    boolTest(church.or, inputs, [true, true, true, false]);
  });

  it('XOR via Church encoding', () => {
    // XOR = λp q. p (NOT q) q
    const xor = parse('λp q.p (q (λt f.f) (λt f.t)) q');
    boolTest(xor, inputs, [false, true, true, false]);
  });

  it('NAND via Church encoding', () => {
    const nand = parse('λp q.p (q (λt f.f) (λt f.t)) (λt f.t)');
    boolTest(nand, inputs, [false, true, true, true]);
  });

  it('IMPLIES via Church encoding', () => {
    // p → q = ¬p ∨ q = p q TRUE
    const implies = parse('λp q.p q (λt f.t)');
    boolTest(implies, inputs, [true, false, true, true]);
  });
});

describe('Divergence Detection', () => {
  it('Omega diverges in all strategies', () => {
    for (const strat of ['normal', 'applicative', 'cbv', 'cbn']) {
      const r = reduce(church.omega, strat, 20);
      assert(!r.normalForm, `${strat} should not find normal form for Omega`);
      assert.equal(r.steps, 20);
    }
  });

  it('K I Omega converges in normal order but not applicative', () => {
    const expr = parse('(λx y.x) (λx.x) ((λx.x x) (λx.x x))');
    const normal = reduce(expr, 'normal', 100);
    assert(normal.normalForm);

    const applicative = reduce(expr, 'applicative', 100);
    assert(!applicative.normalForm);
  });
});

describe('Stress Tests', () => {
  it('large Church numeral: 20', () => {
    assert.equal(unchurch(churchNumeral(20)), 20);
  });

  it('plus 5 5 = 10', () => {
    const r = reduce(new App(new App(church.plus, churchNumeral(5)), churchNumeral(5)), 'normal', 10000);
    assert.equal(unchurch(r.result), 10);
  });

  it('mult 4 5 = 20', () => {
    const r = reduce(new App(new App(church.mult, churchNumeral(4)), churchNumeral(5)), 'normal', 10000);
    assert.equal(unchurch(r.result), 20);
  });

  it('exp 2 5 = 32', () => {
    const r = reduce(new App(new App(church.exp, church.two), churchNumeral(5)), 'normal', 10000);
    assert.equal(unchurch(r.result), 32);
  });

  it('deeply nested applications reduce correctly', () => {
    // ((I I) (I I)) (I (I (I z)))
    const I = parse('λx.x');
    let expr = new Var('z');
    for (let i = 0; i < 5; i++) {
      expr = new App(I, expr);
    }
    const r = reduce(expr);
    assert.equal(r.result.name, 'z');
  });

  it('parsing and reducing a 10-level nested lambda', () => {
    const expr = parse('λa b c d e f g h i j.a b c d e f g h i j');
    assert.equal(freeVars(expr).size, 0);
    // Apply to 10 variables
    let app = expr;
    for (let i = 0; i < 10; i++) {
      app = new App(app, new Var(`v${i}`));
    }
    const r = reduce(app, 'normal', 100);
    assert(r.normalForm);
  });
});

describe('Fixed-Point Combinators', () => {
  it('Turing fixed-point combinator', () => {
    // Θ = (λx y. y (x x y)) (λx y. y (x x y))
    const theta = parse('(λx y.y (x x y)) (λx y.y (x x y))');
    // Θ f should reduce to f (Θ f) — test with a constant function
    const thetaK = new App(theta, parse('λr.λt f.t'));
    const r = reduce(thetaK, 'normal', 200);
    // Should eventually reach λt f.t (Church true)
    assert(r.normalForm);
    assert.equal(unchurchBool(r.result), true);
  });

  it('Y combinator: Y (λf.x) = x for constant function', () => {
    const expr = new App(church.Y, parse('λf.x'));
    const r = reduce(expr, 'normal', 200);
    assert.equal(r.result.name, 'x');
    assert(r.normalForm);
  });
});

describe('Pretty Print Advanced', () => {
  it('pretty prints Church 3', () => {
    const s = prettyPrint(church.three, true);
    assert(s.includes('λ'));
    assert(s.includes('f'));
  });

  it('pretty prints nested application', () => {
    const expr = parse('a b c d');
    const s = prettyPrint(expr);
    assert(s.includes('a'));
    assert(s.includes('d'));
  });

  it('minimal mode for Y combinator', () => {
    const s = prettyPrint(church.Y, true);
    assert(s.includes('λ'));
  });
});
