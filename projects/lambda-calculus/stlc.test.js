import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  TBool, TInt, TUnit, TArrow, TProd,
  TmVar, TmAbs, TmApp, TmBool, TmInt, TmUnit,
  TmIf, TmLet, TmBinOp, TmPair, TmFst, TmSnd, TmFix,
  TypeEnv, TypeError, typecheck, evaluate,
} from './stlc.js';

// ============================================================
// Type Equality
// ============================================================

describe('Type Equality', () => {
  it('Bool = Bool', () => assert(new TBool().equals(new TBool())));
  it('Int = Int', () => assert(new TInt().equals(new TInt())));
  it('Unit = Unit', () => assert(new TUnit().equals(new TUnit())));
  it('Bool ≠ Int', () => assert(!new TBool().equals(new TInt())));
  it('Int → Bool = Int → Bool', () => {
    assert(new TArrow(new TInt(), new TBool()).equals(new TArrow(new TInt(), new TBool())));
  });
  it('Int → Bool ≠ Bool → Int', () => {
    assert(!new TArrow(new TInt(), new TBool()).equals(new TArrow(new TBool(), new TInt())));
  });
  it('(Int × Bool) = (Int × Bool)', () => {
    assert(new TProd(new TInt(), new TBool()).equals(new TProd(new TInt(), new TBool())));
  });
  it('nested arrow types', () => {
    const t1 = new TArrow(new TInt(), new TArrow(new TInt(), new TInt()));
    const t2 = new TArrow(new TInt(), new TArrow(new TInt(), new TInt()));
    assert(t1.equals(t2));
  });
});

describe('Type toString', () => {
  it('arrow type', () => assert.equal(new TArrow(new TInt(), new TBool()).toString(), 'Int → Bool'));
  it('nested arrow (right)', () => {
    assert.equal(new TArrow(new TInt(), new TArrow(new TBool(), new TInt())).toString(), 'Int → Bool → Int');
  });
  it('nested arrow (left needs parens)', () => {
    assert.equal(new TArrow(new TArrow(new TInt(), new TBool()), new TInt()).toString(), '(Int → Bool) → Int');
  });
  it('product type', () => assert.equal(new TProd(new TInt(), new TBool()).toString(), 'Int × Bool'));
});

// ============================================================
// Type Checking: Literals
// ============================================================

describe('Typecheck Literals', () => {
  it('true : Bool', () => assert(typecheck(new TmBool(true)).equals(new TBool())));
  it('false : Bool', () => assert(typecheck(new TmBool(false)).equals(new TBool())));
  it('42 : Int', () => assert(typecheck(new TmInt(42)).equals(new TInt())));
  it('0 : Int', () => assert(typecheck(new TmInt(0)).equals(new TInt())));
  it('() : Unit', () => assert(typecheck(new TmUnit()).equals(new TUnit())));
});

// ============================================================
// Type Checking: Variables
// ============================================================

describe('Typecheck Variables', () => {
  it('unbound variable throws', () => {
    assert.throws(() => typecheck(new TmVar('x')), TypeError);
  });
});

// ============================================================
// Type Checking: Abstraction
// ============================================================

describe('Typecheck Abstraction', () => {
  it('λx:Int.x : Int → Int', () => {
    const term = new TmAbs('x', new TInt(), new TmVar('x'));
    const t = typecheck(term);
    assert(t.equals(new TArrow(new TInt(), new TInt())));
  });

  it('λx:Bool.x : Bool → Bool', () => {
    const term = new TmAbs('x', new TBool(), new TmVar('x'));
    const t = typecheck(term);
    assert(t.equals(new TArrow(new TBool(), new TBool())));
  });

  it('λx:Int.λy:Bool.x : Int → Bool → Int', () => {
    const term = new TmAbs('x', new TInt(), new TmAbs('y', new TBool(), new TmVar('x')));
    const t = typecheck(term);
    assert(t.equals(new TArrow(new TInt(), new TArrow(new TBool(), new TInt()))));
  });

  it('λf:(Int→Int).λx:Int.f x : (Int→Int) → Int → Int', () => {
    const term = new TmAbs('f', new TArrow(new TInt(), new TInt()),
      new TmAbs('x', new TInt(),
        new TmApp(new TmVar('f'), new TmVar('x'))));
    const t = typecheck(term);
    assert(t.equals(new TArrow(new TArrow(new TInt(), new TInt()), new TArrow(new TInt(), new TInt()))));
  });
});

// ============================================================
// Type Checking: Application
// ============================================================

describe('Typecheck Application', () => {
  it('(λx:Int.x) 5 : Int', () => {
    const term = new TmApp(new TmAbs('x', new TInt(), new TmVar('x')), new TmInt(5));
    assert(typecheck(term).equals(new TInt()));
  });

  it('applying non-function throws', () => {
    const term = new TmApp(new TmInt(5), new TmInt(3));
    assert.throws(() => typecheck(term), TypeError);
  });

  it('argument type mismatch throws', () => {
    const term = new TmApp(
      new TmAbs('x', new TInt(), new TmVar('x')),
      new TmBool(true));
    assert.throws(() => typecheck(term), TypeError);
  });
});

// ============================================================
// Type Checking: If-then-else
// ============================================================

describe('Typecheck If', () => {
  it('if true then 1 else 2 : Int', () => {
    const term = new TmIf(new TmBool(true), new TmInt(1), new TmInt(2));
    assert(typecheck(term).equals(new TInt()));
  });

  it('condition must be Bool', () => {
    const term = new TmIf(new TmInt(1), new TmInt(2), new TmInt(3));
    assert.throws(() => typecheck(term), TypeError);
  });

  it('branches must agree', () => {
    const term = new TmIf(new TmBool(true), new TmInt(1), new TmBool(false));
    assert.throws(() => typecheck(term), TypeError);
  });
});

// ============================================================
// Type Checking: Let
// ============================================================

describe('Typecheck Let', () => {
  it('let x = 5 in x : Int', () => {
    const term = new TmLet('x', new TmInt(5), new TmVar('x'));
    assert(typecheck(term).equals(new TInt()));
  });

  it('let x = true in if x then 1 else 2 : Int', () => {
    const term = new TmLet('x', new TmBool(true),
      new TmIf(new TmVar('x'), new TmInt(1), new TmInt(2)));
    assert(typecheck(term).equals(new TInt()));
  });

  it('nested let', () => {
    const term = new TmLet('x', new TmInt(5),
      new TmLet('y', new TmInt(10),
        new TmBinOp('+', new TmVar('x'), new TmVar('y'))));
    assert(typecheck(term).equals(new TInt()));
  });
});

// ============================================================
// Type Checking: Binary Operators
// ============================================================

describe('Typecheck BinOp', () => {
  it('1 + 2 : Int', () => {
    assert(typecheck(new TmBinOp('+', new TmInt(1), new TmInt(2))).equals(new TInt()));
  });

  it('1 < 2 : Bool', () => {
    assert(typecheck(new TmBinOp('<', new TmInt(1), new TmInt(2))).equals(new TBool()));
  });

  it('true && false : Bool', () => {
    assert(typecheck(new TmBinOp('&&', new TmBool(true), new TmBool(false))).equals(new TBool()));
  });

  it('1 == 2 : Bool', () => {
    assert(typecheck(new TmBinOp('==', new TmInt(1), new TmInt(2))).equals(new TBool()));
  });

  it('type mismatch in arithmetic', () => {
    assert.throws(() => typecheck(new TmBinOp('+', new TmInt(1), new TmBool(true))), TypeError);
  });

  it('type mismatch in comparison', () => {
    assert.throws(() => typecheck(new TmBinOp('<', new TmBool(true), new TmInt(1))), TypeError);
  });

  it('== requires same types', () => {
    assert.throws(() => typecheck(new TmBinOp('==', new TmInt(1), new TmBool(true))), TypeError);
  });
});

// ============================================================
// Type Checking: Pairs
// ============================================================

describe('Typecheck Pairs', () => {
  it('(1, true) : Int × Bool', () => {
    assert(typecheck(new TmPair(new TmInt(1), new TmBool(true))).equals(new TProd(new TInt(), new TBool())));
  });

  it('fst (1, true) : Int', () => {
    assert(typecheck(new TmFst(new TmPair(new TmInt(1), new TmBool(true)))).equals(new TInt()));
  });

  it('snd (1, true) : Bool', () => {
    assert(typecheck(new TmSnd(new TmPair(new TmInt(1), new TmBool(true)))).equals(new TBool()));
  });

  it('fst of non-pair throws', () => {
    assert.throws(() => typecheck(new TmFst(new TmInt(5))), TypeError);
  });

  it('nested pairs', () => {
    const inner = new TmPair(new TmInt(1), new TmInt(2));
    const outer = new TmPair(inner, new TmBool(true));
    const t = typecheck(outer);
    assert(t.equals(new TProd(new TProd(new TInt(), new TInt()), new TBool())));
  });
});

// ============================================================
// Type Checking: Fix (general recursion)
// ============================================================

describe('Typecheck Fix', () => {
  it('fix (λx:Int.x) : Int', () => {
    const term = new TmFix(new TmAbs('x', new TInt(), new TmVar('x')));
    assert(typecheck(term).equals(new TInt()));
  });

  it('fix requires function type', () => {
    assert.throws(() => typecheck(new TmFix(new TmInt(5))), TypeError);
  });

  it('fix requires T → T', () => {
    const term = new TmFix(new TmAbs('x', new TInt(), new TmBool(true)));
    assert.throws(() => typecheck(term), TypeError);
  });
});

// ============================================================
// Evaluation
// ============================================================

describe('Evaluate Literals', () => {
  it('42 → 42', () => {
    const r = evaluate(new TmInt(42));
    assert(r.result instanceof TmInt);
    assert.equal(r.result.value, 42);
  });

  it('true → true', () => {
    const r = evaluate(new TmBool(true));
    assert(r.result instanceof TmBool);
    assert.equal(r.result.value, true);
  });
});

describe('Evaluate Application', () => {
  it('(λx:Int.x) 5 → 5', () => {
    const term = new TmApp(new TmAbs('x', new TInt(), new TmVar('x')), new TmInt(5));
    const r = evaluate(term);
    assert(r.result instanceof TmInt);
    assert.equal(r.result.value, 5);
  });

  it('(λx:Int.x+1) 5 → 6', () => {
    const term = new TmApp(
      new TmAbs('x', new TInt(), new TmBinOp('+', new TmVar('x'), new TmInt(1))),
      new TmInt(5));
    const r = evaluate(term);
    assert(r.result instanceof TmInt);
    assert.equal(r.result.value, 6);
  });

  it('K combinator: (λx:Int.λy:Bool.x) 5 true → 5', () => {
    const term = new TmApp(
      new TmApp(
        new TmAbs('x', new TInt(), new TmAbs('y', new TBool(), new TmVar('x'))),
        new TmInt(5)),
      new TmBool(true));
    const r = evaluate(term);
    assert(r.result instanceof TmInt);
    assert.equal(r.result.value, 5);
  });
});

describe('Evaluate If', () => {
  it('if true then 1 else 2 → 1', () => {
    const r = evaluate(new TmIf(new TmBool(true), new TmInt(1), new TmInt(2)));
    assert.equal(r.result.value, 1);
  });

  it('if false then 1 else 2 → 2', () => {
    const r = evaluate(new TmIf(new TmBool(false), new TmInt(1), new TmInt(2)));
    assert.equal(r.result.value, 2);
  });

  it('nested if', () => {
    const r = evaluate(new TmIf(
      new TmBinOp('<', new TmInt(3), new TmInt(5)),
      new TmInt(10),
      new TmInt(20)));
    assert.equal(r.result.value, 10);
  });
});

describe('Evaluate Let', () => {
  it('let x = 5 in x → 5', () => {
    const r = evaluate(new TmLet('x', new TmInt(5), new TmVar('x')));
    assert.equal(r.result.value, 5);
  });

  it('let x = 5 in x + 3 → 8', () => {
    const r = evaluate(new TmLet('x', new TmInt(5), new TmBinOp('+', new TmVar('x'), new TmInt(3))));
    assert.equal(r.result.value, 8);
  });

  it('let with computed value', () => {
    const r = evaluate(new TmLet('x', new TmBinOp('*', new TmInt(3), new TmInt(4)),
      new TmBinOp('+', new TmVar('x'), new TmInt(1))));
    assert.equal(r.result.value, 13);
  });
});

describe('Evaluate BinOp', () => {
  it('2 + 3 → 5', () => assert.equal(evaluate(new TmBinOp('+', new TmInt(2), new TmInt(3))).result.value, 5));
  it('7 - 4 → 3', () => assert.equal(evaluate(new TmBinOp('-', new TmInt(7), new TmInt(4))).result.value, 3));
  it('3 * 5 → 15', () => assert.equal(evaluate(new TmBinOp('*', new TmInt(3), new TmInt(5))).result.value, 15));
  it('10 / 3 → 3', () => assert.equal(evaluate(new TmBinOp('/', new TmInt(10), new TmInt(3))).result.value, 3));
  it('10 % 3 → 1', () => assert.equal(evaluate(new TmBinOp('%', new TmInt(10), new TmInt(3))).result.value, 1));
  it('3 < 5 → true', () => assert.equal(evaluate(new TmBinOp('<', new TmInt(3), new TmInt(5))).result.value, true));
  it('5 > 3 → true', () => assert.equal(evaluate(new TmBinOp('>', new TmInt(5), new TmInt(3))).result.value, true));
  it('3 <= 3 → true', () => assert.equal(evaluate(new TmBinOp('<=', new TmInt(3), new TmInt(3))).result.value, true));
  it('3 == 3 → true', () => assert.equal(evaluate(new TmBinOp('==', new TmInt(3), new TmInt(3))).result.value, true));
  it('3 != 4 → true', () => assert.equal(evaluate(new TmBinOp('!=', new TmInt(3), new TmInt(4))).result.value, true));
  it('true && false → false', () => assert.equal(evaluate(new TmBinOp('&&', new TmBool(true), new TmBool(false))).result.value, false));
  it('false || true → true', () => assert.equal(evaluate(new TmBinOp('||', new TmBool(false), new TmBool(true))).result.value, true));
});

describe('Evaluate Pairs', () => {
  it('fst (1, 2) → 1', () => {
    const r = evaluate(new TmFst(new TmPair(new TmInt(1), new TmInt(2))));
    assert.equal(r.result.value, 1);
  });

  it('snd (1, 2) → 2', () => {
    const r = evaluate(new TmSnd(new TmPair(new TmInt(1), new TmInt(2))));
    assert.equal(r.result.value, 2);
  });

  it('fst (snd ((1,2), (3,4))) → 3', () => {
    const r = evaluate(new TmFst(new TmSnd(new TmPair(
      new TmPair(new TmInt(1), new TmInt(2)),
      new TmPair(new TmInt(3), new TmInt(4))))));
    assert.equal(r.result.value, 3);
  });

  it('pair with computed values', () => {
    const r = evaluate(new TmFst(new TmPair(
      new TmBinOp('+', new TmInt(1), new TmInt(2)),
      new TmBinOp('*', new TmInt(3), new TmInt(4)))));
    assert.equal(r.result.value, 3);
  });
});

describe('Evaluate Fix (recursion)', () => {
  it('factorial via fix', () => {
    // fix (λfact:Int→Int. λn:Int. if n == 0 then 1 else n * fact(n-1))
    const fact = new TmFix(
      new TmAbs('fact', new TArrow(new TInt(), new TInt()),
        new TmAbs('n', new TInt(),
          new TmIf(
            new TmBinOp('==', new TmVar('n'), new TmInt(0)),
            new TmInt(1),
            new TmBinOp('*', new TmVar('n'),
              new TmApp(new TmVar('fact'), new TmBinOp('-', new TmVar('n'), new TmInt(1))))))));

    // fact(5) = 120
    const r = evaluate(new TmApp(fact, new TmInt(5)));
    assert.equal(r.result.value, 120);
  });

  it('fibonacci via fix', () => {
    // fix (λfib:Int→Int. λn:Int. if n <= 1 then n else fib(n-1) + fib(n-2))
    const fib = new TmFix(
      new TmAbs('fib', new TArrow(new TInt(), new TInt()),
        new TmAbs('n', new TInt(),
          new TmIf(
            new TmBinOp('<=', new TmVar('n'), new TmInt(1)),
            new TmVar('n'),
            new TmBinOp('+',
              new TmApp(new TmVar('fib'), new TmBinOp('-', new TmVar('n'), new TmInt(1))),
              new TmApp(new TmVar('fib'), new TmBinOp('-', new TmVar('n'), new TmInt(2))))))));

    assert.equal(evaluate(new TmApp(fib, new TmInt(0))).result.value, 0);
    assert.equal(evaluate(new TmApp(fib, new TmInt(1))).result.value, 1);
    assert.equal(evaluate(new TmApp(fib, new TmInt(5))).result.value, 5);
    assert.equal(evaluate(new TmApp(fib, new TmInt(10))).result.value, 55);
  });
});

describe('Strong Normalization (STLC without fix)', () => {
  // STLC without fix guarantees termination for all well-typed terms
  it('complex nested applications terminate', () => {
    const term = new TmApp(
      new TmApp(
        new TmAbs('f', new TArrow(new TInt(), new TInt()),
          new TmAbs('x', new TInt(),
            new TmApp(new TmVar('f'), new TmApp(new TmVar('f'), new TmVar('x'))))),
        new TmAbs('y', new TInt(), new TmBinOp('+', new TmVar('y'), new TmInt(1)))),
      new TmInt(0));
    typecheck(term); // Should typecheck
    const r = evaluate(term);
    assert(r.normalForm);
    assert.equal(r.result.value, 2);
  });

  it('higher-order function terminates', () => {
    // apply twice: (λf. λx. f (f x)) (λy. y + 1) 0 → 2
    const twice = new TmAbs('f', new TArrow(new TInt(), new TInt()),
      new TmAbs('x', new TInt(),
        new TmApp(new TmVar('f'), new TmApp(new TmVar('f'), new TmVar('x')))));
    const inc = new TmAbs('y', new TInt(), new TmBinOp('+', new TmVar('y'), new TmInt(1)));
    const term = new TmApp(new TmApp(twice, inc), new TmInt(0));
    const r = evaluate(term);
    assert(r.normalForm);
    assert.equal(r.result.value, 2);
  });

  it('church-like numeral 3 terminates', () => {
    // (λf:Int→Int. λx:Int. f(f(f x))) (λy:Int. y+1) 0 → 3
    const three = new TmAbs('f', new TArrow(new TInt(), new TInt()),
      new TmAbs('x', new TInt(),
        new TmApp(new TmVar('f'),
          new TmApp(new TmVar('f'),
            new TmApp(new TmVar('f'), new TmVar('x'))))));
    const inc = new TmAbs('y', new TInt(), new TmBinOp('+', new TmVar('y'), new TmInt(1)));
    const r = evaluate(new TmApp(new TmApp(three, inc), new TmInt(0)));
    assert.equal(r.result.value, 3);
  });
});

describe('Complex Programs', () => {
  it('absolute value function', () => {
    const abs = new TmAbs('n', new TInt(),
      new TmIf(new TmBinOp('<', new TmVar('n'), new TmInt(0)),
        new TmBinOp('-', new TmInt(0), new TmVar('n')),
        new TmVar('n')));
    assert.equal(evaluate(new TmApp(abs, new TmInt(-5))).result.value, 5);
    assert.equal(evaluate(new TmApp(abs, new TmInt(3))).result.value, 3);
  });

  it('max function', () => {
    const max = new TmAbs('a', new TInt(),
      new TmAbs('b', new TInt(),
        new TmIf(new TmBinOp('>', new TmVar('a'), new TmVar('b')),
          new TmVar('a'), new TmVar('b'))));
    assert.equal(evaluate(new TmApp(new TmApp(max, new TmInt(3)), new TmInt(7))).result.value, 7);
    assert.equal(evaluate(new TmApp(new TmApp(max, new TmInt(10)), new TmInt(5))).result.value, 10);
  });

  it('GCD via fix', () => {
    // fix (λgcd:Int→Int→Int. λa:Int. λb:Int. if b == 0 then a else gcd b (a % b))
    const gcd = new TmFix(
      new TmAbs('gcd', new TArrow(new TInt(), new TArrow(new TInt(), new TInt())),
        new TmAbs('a', new TInt(),
          new TmAbs('b', new TInt(),
            new TmIf(
              new TmBinOp('==', new TmVar('b'), new TmInt(0)),
              new TmVar('a'),
              new TmApp(new TmApp(new TmVar('gcd'), new TmVar('b')),
                new TmBinOp('%', new TmVar('a'), new TmVar('b'))))))));
    assert.equal(evaluate(new TmApp(new TmApp(gcd, new TmInt(12)), new TmInt(8))).result.value, 4);
    assert.equal(evaluate(new TmApp(new TmApp(gcd, new TmInt(100)), new TmInt(75))).result.value, 25);
  });

  it('sum 1..n via fix', () => {
    const sum = new TmFix(
      new TmAbs('sum', new TArrow(new TInt(), new TInt()),
        new TmAbs('n', new TInt(),
          new TmIf(
            new TmBinOp('==', new TmVar('n'), new TmInt(0)),
            new TmInt(0),
            new TmBinOp('+', new TmVar('n'),
              new TmApp(new TmVar('sum'), new TmBinOp('-', new TmVar('n'), new TmInt(1))))))));
    assert.equal(evaluate(new TmApp(sum, new TmInt(10))).result.value, 55);
    assert.equal(evaluate(new TmApp(sum, new TmInt(100))).result.value, 5050);
  });
});
