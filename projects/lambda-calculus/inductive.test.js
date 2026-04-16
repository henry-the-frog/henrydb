import { strict as assert } from 'assert';
import {
  Star, Box, Var, Pi, Lam, App, Nat, Zero, Succ, NatElim,
  Context, TypeError,
  infer, check, normalize, betaEq, arrow, resetNames
} from './coc.js';

import {
  defineInductive,
  defineBool, defineMaybe, defineEither, defineList, definePair, defineUnit, defineVoid,
  boolElim, listFold
} from './inductive.js';

let passed = 0, failed = 0, total = 0;
const ctx = new Context();
const star = new Star();
const nat = new Nat();
const zero = new Zero();
const one = new Succ(zero);
const two = new Succ(one);
const three = new Succ(two);

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// ============================================================
// Bool
// ============================================================

const boolDef = defineBool();

test('Bool type is ★', () => {
  assert.ok(betaEq(infer(ctx, boolDef.type), star));
});

test('Bool.true well-typed', () => {
  const t = boolDef.constructors.true;
  const tType = infer(ctx, t);
  assert.ok(betaEq(tType, boolDef.type));
});

test('Bool.false well-typed', () => {
  const f = boolDef.constructors.false;
  const fType = infer(ctx, f);
  assert.ok(betaEq(fType, boolDef.type));
});

test('Bool elimination: true selects first', () => {
  const t = boolDef.constructors.true;
  const result = boolElim(t, one, zero, nat);
  assert.ok(betaEq(result, one));
});

test('Bool elimination: false selects second', () => {
  const f = boolDef.constructors.false;
  const result = boolElim(f, one, zero, nat);
  assert.ok(betaEq(result, zero));
});

test('Bool not: λ(b:Bool).b Bool false true', () => {
  const b = new Var('b');
  const boolNot = new Lam('b', boolDef.type,
    boolElim(b, boolDef.constructors.false, boolDef.constructors.true, boolDef.type));
  
  const boolCtx = ctx.extend('b', boolDef.type);
  // not true = false
  const result = normalize(new App(boolNot, boolDef.constructors.true));
  assert.ok(betaEq(result, boolDef.constructors.false));
});

// ============================================================
// Unit
// ============================================================

const unitDef = defineUnit();

test('Unit type is ★', () => {
  assert.ok(betaEq(infer(ctx, unitDef.type), star));
});

test('Unit.tt well-typed', () => {
  const tt = unitDef.constructors.tt;
  const ttType = infer(ctx, tt);
  assert.ok(betaEq(ttType, unitDef.type));
});

// ============================================================
// Void (empty type)
// ============================================================

const voidDef = defineVoid();

test('Void type is ★', () => {
  assert.ok(betaEq(infer(ctx, voidDef.type), star));
});

test('Void has no constructors', () => {
  assert.equal(Object.keys(voidDef.constructors).length, 0);
});

// ============================================================
// Maybe
// ============================================================

const maybeDef = defineMaybe();

test('Maybe type is well-formed', () => {
  const maybeType = infer(ctx, maybeDef.type);
  assert.ok(maybeType); // Just check it type-checks
});

test('Maybe.nothing well-typed', () => {
  const nothing = maybeDef.constructors.nothing;
  // nothing : Π(A:★). Maybe A
  const nType = infer(ctx, nothing);
  assert.ok(nType instanceof Pi);
});

test('Maybe.just well-typed', () => {
  const just = maybeDef.constructors.just;
  const jType = infer(ctx, just);
  assert.ok(jType instanceof Pi);
});

// ============================================================
// Pair
// ============================================================

const pairDef = definePair();

test('Pair type is well-formed', () => {
  const pairType = infer(ctx, pairDef.type);
  assert.ok(pairType);
});

test('Pair.mkpair constructs pairs', () => {
  const mkpair = pairDef.constructors.mkpair;
  const mkType = infer(ctx, mkpair);
  assert.ok(mkType instanceof Pi);
});

test('Pair projection: fst extracts first', () => {
  const mkpair = pairDef.constructors.mkpair;
  // mkpair ℕ ℕ 1 2 : Pair ℕ ℕ
  const pair12 = new App(new App(new App(new App(mkpair, nat), nat), one), two);
  
  // fst = λ(p:Pair ℕ ℕ). p ℕ (λ(a:ℕ).λ(b:ℕ).a)
  const pairNatNat = normalize(new App(new App(pairDef.type, nat), nat));
  const result = normalize(new App(new App(pair12, nat),
    new Lam('a', nat, new Lam('b', nat, new Var('a')))));
  assert.ok(betaEq(result, one), `Expected 1, got ${result}`);
});

test('Pair projection: snd extracts second', () => {
  const mkpair = pairDef.constructors.mkpair;
  const pair12 = new App(new App(new App(new App(mkpair, nat), nat), one), two);
  const result = normalize(new App(new App(pair12, nat),
    new Lam('a', nat, new Lam('b', nat, new Var('b')))));
  assert.ok(betaEq(result, two), `Expected 2, got ${result}`);
});

// ============================================================
// List
// ============================================================

const listDef = defineList();

test('List.nil well-typed', () => {
  const nil = listDef.constructors.nil;
  const nilType = infer(ctx, nil);
  assert.ok(nilType instanceof Pi);
});

test('List.cons well-typed', () => {
  const cons = listDef.constructors.cons;
  const consType = infer(ctx, cons);
  assert.ok(consType instanceof Pi);
});

test('List fold: sum of [1, 2, 3] = 6', () => {
  resetNames();
  const nil = listDef.constructors.nil;
  const cons = listDef.constructors.cons;
  
  // Build list [1, 2, 3] = cons ℕ 1 (cons ℕ 2 (cons ℕ 3 (nil ℕ)))
  const nilNat = new App(nil, nat);
  const list = new App(new App(new App(cons, nat), one),
    new App(new App(new App(cons, nat), two),
      new App(new App(new App(cons, nat), three), nilNat)));
  
  // fold with addition: sum = fold + 0
  // fold function: f = λ(x:ℕ).λ(acc:ℕ). natElim(λ_.ℕ, acc, λk.λih.S ih, x)
  // Actually simpler: use the list's Church encoding directly
  // list ℕ 0 (λx.λacc. x + acc)
  const P = new Lam('_', nat, nat);
  const addFn = new Lam('x', nat, new Lam('acc', nat,
    new NatElim(P, new Var('acc'),
      new Lam('k', nat, new Lam('ih', nat, new Succ(new Var('ih')))),
      new Var('x'))));
  
  const result = normalize(listFold(list, zero, addFn, nat, nat));
  const six = new Succ(new Succ(new Succ(new Succ(new Succ(new Succ(zero))))));
  assert.ok(betaEq(result, six), `Expected 6, got ${result}`);
});

test('List length: |[1, 2, 3]| = 3', () => {
  resetNames();
  const nil = listDef.constructors.nil;
  const cons = listDef.constructors.cons;
  
  const nilNat = new App(nil, nat);
  const list = new App(new App(new App(cons, nat), one),
    new App(new App(new App(cons, nat), two),
      new App(new App(new App(cons, nat), three), nilNat)));
  
  // length = fold (λx.λacc. S acc) 0
  const lenFn = new Lam('x', nat, new Lam('acc', nat, new Succ(new Var('acc'))));
  const result = normalize(listFold(list, zero, lenFn, nat, nat));
  assert.ok(betaEq(result, three), `Expected 3, got ${result}`);
});

// ============================================================
// Either
// ============================================================

const eitherDef = defineEither();

test('Either type is well-formed', () => {
  const eitherType = infer(ctx, eitherDef.type);
  assert.ok(eitherType);
});

test('Either.left constructs left value', () => {
  const left = eitherDef.constructors.left;
  const leftType = infer(ctx, left);
  assert.ok(leftType instanceof Pi);
});

test('Either case analysis: left extracts correctly', () => {
  const left = eitherDef.constructors.left;
  // left ℕ ★ 42 : Either ℕ ★
  const leftVal = new App(new App(new App(left, nat), star), new Succ(zero));
  // Eliminate: leftVal ℕ (λx.x) (λy.0) — should give 1
  const result = normalize(
    new App(new App(new App(leftVal, nat),
      new Lam('x', nat, new Var('x'))),
      new Lam('y', star, zero)));
  assert.ok(betaEq(result, one), `Expected 1, got ${result}`);
});

// ============================================================
// Report
// ============================================================

console.log(`\nInductive types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
