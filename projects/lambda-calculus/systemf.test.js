import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  TVar, TBool, TInt, TUnit, TArrow, TForall, TProd,
  FVar, FAbs, FApp, FTyAbs, FTyApp,
  FBool, FInt, FUnit, FIf, FLet, FBinOp, FPair, FFst, FSnd,
  TypeEnv, FTypeError, typecheck, evaluate,
} from './systemf.js';

// ============================================================
// Type Operations
// ============================================================

describe('Type Substitution', () => {
  it('substitutes type variable', () => {
    const t = new TVar('α').subst('α', new TInt());
    assert(t.equals(new TInt()));
  });

  it('does not substitute different variable', () => {
    const t = new TVar('β').subst('α', new TInt());
    assert(t.equals(new TVar('β')));
  });

  it('substitutes in arrow type', () => {
    const t = new TArrow(new TVar('α'), new TVar('α')).subst('α', new TBool());
    assert(t.equals(new TArrow(new TBool(), new TBool())));
  });

  it('respects shadowing in forall', () => {
    // ∀α. α should not have α substituted
    const t = new TForall('α', new TVar('α')).subst('α', new TInt());
    assert(t.equals(new TForall('α', new TVar('α'))));
  });

  it('substitutes under non-shadowing forall', () => {
    const t = new TForall('β', new TVar('α')).subst('α', new TInt());
    assert(t.equals(new TForall('β', new TInt())));
  });
});

describe('Type Equality', () => {
  it('alpha-equivalent foralls', () => {
    const a = new TForall('α', new TVar('α'));
    const b = new TForall('β', new TVar('β'));
    assert(a.equals(b));
  });

  it('different forall bodies', () => {
    const a = new TForall('α', new TVar('α'));
    const b = new TForall('α', new TInt());
    assert(!a.equals(b));
  });
});

describe('Free Type Variables', () => {
  it('TVar has itself free', () => {
    assert.deepEqual(new TVar('α').freeVars(), new Set(['α']));
  });

  it('bound variable is not free', () => {
    assert.deepEqual(new TForall('α', new TVar('α')).freeVars(), new Set());
  });

  it('mixed free and bound', () => {
    const t = new TForall('α', new TArrow(new TVar('α'), new TVar('β')));
    assert.deepEqual(t.freeVars(), new Set(['β']));
  });
});

// ============================================================
// Polymorphic Identity: Λα. λx:α. x
// Type: ∀α. α → α
// ============================================================

describe('Polymorphic Identity', () => {
  const polyId = new FTyAbs('α', new FAbs('x', new TVar('α'), new FVar('x')));

  it('typechecks to ∀α. α → α', () => {
    const t = typecheck(polyId);
    assert(t instanceof TForall);
    assert(t.body instanceof TArrow);
    assert(t.body.param.equals(new TVar('α')));
    assert(t.body.ret.equals(new TVar('α')));
  });

  it('instantiated at Int: (Λα.λx:α.x)[Int] 5 → 5', () => {
    const term = new FApp(new FTyApp(polyId, new TInt()), new FInt(5));
    const t = typecheck(term);
    assert(t.equals(new TInt()));
    const r = evaluate(term);
    assert.equal(r.result.value, 5);
  });

  it('instantiated at Bool: (Λα.λx:α.x)[Bool] true → true', () => {
    const term = new FApp(new FTyApp(polyId, new TBool()), new FBool(true));
    assert(typecheck(term).equals(new TBool()));
    assert.equal(evaluate(term).result.value, true);
  });

  it('instantiated at function type', () => {
    const intToInt = new TArrow(new TInt(), new TInt());
    const inc = new FAbs('n', new TInt(), new FBinOp('+', new FVar('n'), new FInt(1)));
    const term = new FApp(new FTyApp(polyId, intToInt), inc);
    const t = typecheck(term);
    assert(t.equals(intToInt));
    // Apply result to 5: should get 6
    const r = evaluate(new FApp(term, new FInt(5)));
    assert.equal(r.result.value, 6);
  });
});

// ============================================================
// Polymorphic Const: Λα β. λx:α. λy:β. x
// Type: ∀α. ∀β. α → β → α
// ============================================================

describe('Polymorphic Const (K)', () => {
  const polyK = new FTyAbs('α', new FTyAbs('β',
    new FAbs('x', new TVar('α'), new FAbs('y', new TVar('β'), new FVar('x')))));

  it('typechecks to ∀α.∀β. α → β → α', () => {
    const t = typecheck(polyK);
    assert(t instanceof TForall);
    assert.equal(t.typeVar, 'α');
    assert(t.body instanceof TForall);
  });

  it('K[Int][Bool] 5 true → 5', () => {
    const term = new FApp(new FApp(
      new FTyApp(new FTyApp(polyK, new TInt()), new TBool()),
      new FInt(5)), new FBool(true));
    assert(typecheck(term).equals(new TInt()));
    assert.equal(evaluate(term).result.value, 5);
  });
});

// ============================================================
// Church Booleans in System F
// ============================================================

describe('Church Booleans (System F)', () => {
  // CBool = ∀α. α → α → α
  const CBool = new TForall('α', new TArrow(new TVar('α'), new TArrow(new TVar('α'), new TVar('α'))));

  // true = Λα. λt:α. λf:α. t
  const cTrue = new FTyAbs('α', new FAbs('t', new TVar('α'), new FAbs('f', new TVar('α'), new FVar('t'))));
  // false = Λα. λt:α. λf:α. f
  const cFalse = new FTyAbs('α', new FAbs('t', new TVar('α'), new FAbs('f', new TVar('α'), new FVar('f'))));

  it('Church true typechecks', () => {
    const t = typecheck(cTrue);
    assert(t.equals(CBool));
  });

  it('Church false typechecks', () => {
    const t = typecheck(cFalse);
    assert(t.equals(CBool));
  });

  it('Church true [Int] 1 0 → 1', () => {
    const term = new FApp(new FApp(new FTyApp(cTrue, new TInt()), new FInt(1)), new FInt(0));
    assert.equal(evaluate(term).result.value, 1);
  });

  it('Church false [Int] 1 0 → 0', () => {
    const term = new FApp(new FApp(new FTyApp(cFalse, new TInt()), new FInt(1)), new FInt(0));
    assert.equal(evaluate(term).result.value, 0);
  });
});

// ============================================================
// Church Numerals in System F
// ============================================================

describe('Church Numerals (System F)', () => {
  // CNat = ∀α. (α → α) → α → α
  const CNat = new TForall('α', new TArrow(new TArrow(new TVar('α'), new TVar('α')),
    new TArrow(new TVar('α'), new TVar('α'))));

  // zero = Λα. λf:α→α. λx:α. x
  const cZero = new FTyAbs('α', new FAbs('f', new TArrow(new TVar('α'), new TVar('α')),
    new FAbs('x', new TVar('α'), new FVar('x'))));

  // succ = λn:CNat. Λα. λf:α→α. λx:α. f (n [α] f x)
  const cSucc = new FAbs('n', CNat,
    new FTyAbs('α', new FAbs('f', new TArrow(new TVar('α'), new TVar('α')),
      new FAbs('x', new TVar('α'),
        new FApp(new FVar('f'),
          new FApp(new FApp(new FTyApp(new FVar('n'), new TVar('α')), new FVar('f')), new FVar('x')))))));

  it('zero typechecks as CNat', () => {
    assert(typecheck(cZero).equals(CNat));
  });

  it('succ typechecks as CNat → CNat', () => {
    const t = typecheck(cSucc);
    assert(t.equals(new TArrow(CNat, CNat)));
  });

  it('zero [Int] (+1) 0 → 0', () => {
    const inc = new FAbs('y', new TInt(), new FBinOp('+', new FVar('y'), new FInt(1)));
    const term = new FApp(new FApp(new FTyApp(cZero, new TInt()), inc), new FInt(0));
    assert.equal(evaluate(term).result.value, 0);
  });

  it('succ(zero) [Int] (+1) 0 → 1', () => {
    const inc = new FAbs('y', new TInt(), new FBinOp('+', new FVar('y'), new FInt(1)));
    const one = new FApp(cSucc, cZero);
    const term = new FApp(new FApp(new FTyApp(one, new TInt()), inc), new FInt(0));
    const r = evaluate(term);
    assert.equal(r.result.value, 1);
  });

  it('succ(succ(zero)) [Int] (+1) 0 → 2', () => {
    const inc = new FAbs('y', new TInt(), new FBinOp('+', new FVar('y'), new FInt(1)));
    const two = new FApp(cSucc, new FApp(cSucc, cZero));
    const term = new FApp(new FApp(new FTyApp(two, new TInt()), inc), new FInt(0));
    const r = evaluate(term);
    assert.equal(r.result.value, 2);
  });
});

// ============================================================
// Existential Types (encoded in System F)
// ============================================================

describe('Existential Types', () => {
  // ∃α. {val: α, show: α → Int}
  // Encoded as: ∀β. (∀α. α → (α → Int) → β) → β
  
  it('existential package typechecks', () => {
    // pack <Int, {val=5, show=λx:Int.x}> 
    // = Λβ. λf:(∀α. α → (α → Int) → β). f [Int] 5 (λx:Int. x)
    const pack = new FTyAbs('β',
      new FAbs('f', new TForall('α', new TArrow(new TVar('α'),
        new TArrow(new TArrow(new TVar('α'), new TInt()), new TVar('β')))),
        new FApp(new FApp(
          new FTyApp(new FVar('f'), new TInt()),
          new FInt(5)),
          new FAbs('x', new TInt(), new FVar('x')))));
    
    const t = typecheck(pack);
    assert(t instanceof TForall);
  });
});

// ============================================================
// Type Errors
// ============================================================

describe('Type Errors', () => {
  it('applying wrong type argument', () => {
    const polyId = new FTyAbs('α', new FAbs('x', new TVar('α'), new FVar('x')));
    // Apply to Int, then give Bool value → type error
    const term = new FApp(new FTyApp(polyId, new TInt()), new FBool(true));
    assert.throws(() => typecheck(term), FTypeError);
  });

  it('unbound type variable in annotation', () => {
    // λx:α. x where α is not in scope
    const term = new FAbs('x', new TVar('α'), new FVar('x'));
    assert.throws(() => typecheck(term), FTypeError);
  });

  it('type application to non-forall', () => {
    const term = new FTyApp(new FInt(5), new TInt());
    assert.throws(() => typecheck(term), FTypeError);
  });

  it('applying int as function', () => {
    assert.throws(() => typecheck(new FApp(new FInt(5), new FInt(3))), FTypeError);
  });
});

// ============================================================
// Evaluation
// ============================================================

describe('Evaluate System F', () => {
  it('basic arithmetic', () => {
    const r = evaluate(new FBinOp('+', new FInt(3), new FInt(4)));
    assert.equal(r.result.value, 7);
  });

  it('if-then-else', () => {
    const r = evaluate(new FIf(new FBool(true), new FInt(1), new FInt(2)));
    assert.equal(r.result.value, 1);
  });

  it('let binding', () => {
    const r = evaluate(new FLet('x', new FInt(5), new FBinOp('*', new FVar('x'), new FVar('x'))));
    assert.equal(r.result.value, 25);
  });

  it('pairs', () => {
    const r = evaluate(new FFst(new FPair(new FInt(1), new FBool(true))));
    assert.equal(r.result.value, 1);
  });

  it('polymorphic function applied multiple times', () => {
    const polyId = new FTyAbs('α', new FAbs('x', new TVar('α'), new FVar('x')));
    const r1 = evaluate(new FApp(new FTyApp(polyId, new TInt()), new FInt(42)));
    const r2 = evaluate(new FApp(new FTyApp(polyId, new TBool()), new FBool(false)));
    assert.equal(r1.result.value, 42);
    assert.equal(r2.result.value, false);
  });
});

// ============================================================
// Higher-rank types
// ============================================================

describe('Higher-Rank Types', () => {
  it('rank-2 function: takes polymorphic argument', () => {
    // f : (∀α. α → α) → Int × Bool
    // f = λg:(∀α.α→α). (g[Int] 5, g[Bool] true)
    const polyIdType = new TForall('α', new TArrow(new TVar('α'), new TVar('α')));
    const f = new FAbs('g', polyIdType,
      new FPair(
        new FApp(new FTyApp(new FVar('g'), new TInt()), new FInt(5)),
        new FApp(new FTyApp(new FVar('g'), new TBool()), new FBool(true))));
    
    const t = typecheck(f);
    assert(t instanceof TArrow);
    assert(t.param.equals(polyIdType));
    assert(t.ret.equals(new TProd(new TInt(), new TBool())));

    // Apply to the polymorphic identity
    const polyId = new FTyAbs('α', new FAbs('x', new TVar('α'), new FVar('x')));
    const result = evaluate(new FApp(f, polyId));
    assert(result.result instanceof FPair);
    assert.equal(result.result.fst.value, 5);
    assert.equal(result.result.snd.value, true);
  });
});

// ============================================================
// Parametricity (Free Theorems)
// ============================================================

describe('Parametricity', () => {
  it('∀α. α → α must be identity', () => {
    // Any term of type ∀α. α → α must behave as identity
    // (by parametricity / free theorem)
    const polyId = new FTyAbs('α', new FAbs('x', new TVar('α'), new FVar('x')));
    
    // Test with various types
    const tests = [
      [new TInt(), new FInt(42), 42],
      [new TBool(), new FBool(true), true],
      [new TInt(), new FInt(0), 0],
      [new TBool(), new FBool(false), false],
    ];
    
    for (const [ty, val, expected] of tests) {
      const r = evaluate(new FApp(new FTyApp(polyId, ty), val));
      assert.equal(r.result.value, expected);
    }
  });
});

// ============================================================
// Strong Normalization
// ============================================================

describe('Strong Normalization', () => {
  it('complex polymorphic term terminates', () => {
    // (Λα. λx:α. x)[Int→Int] (λy:Int. y + 1) applied to 5
    const polyId = new FTyAbs('α', new FAbs('x', new TVar('α'), new FVar('x')));
    const inc = new FAbs('y', new TInt(), new FBinOp('+', new FVar('y'), new FInt(1)));
    const term = new FApp(new FApp(new FTyApp(polyId, new TArrow(new TInt(), new TInt())), inc), new FInt(5));
    const r = evaluate(term);
    assert(r.normalForm);
    assert.equal(r.result.value, 6);
  });

  it('nested type abstractions terminate', () => {
    // Λα. Λβ. λx:α. λy:β. x  applied to types and values
    const term = new FTyAbs('α', new FTyAbs('β',
      new FAbs('x', new TVar('α'), new FAbs('y', new TVar('β'), new FVar('x')))));
    const applied = new FApp(new FApp(
      new FTyApp(new FTyApp(term, new TInt()), new TBool()),
      new FInt(99)), new FBool(false));
    const r = evaluate(applied);
    assert(r.normalForm);
    assert.equal(r.result.value, 99);
  });
});
