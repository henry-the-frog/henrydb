import { strict as assert } from 'assert';
import { BVar, FVar, Lam, App, Num, openTerm, closeTerm, freeVars, subst, isLocallyClosed, resetFresh } from './locally-nameless.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { resetFresh(); fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('open: BVar(0) with "x" → FVar(x)', () => {
  const r = openTerm(new BVar(0), 'x');
  assert.equal(r.name, 'x');
});

test('close: FVar(x) with "x" → BVar(0)', () => {
  const r = closeTerm(new FVar('x'), 'x');
  assert.equal(r.idx, 0);
});

test('open/close roundtrip', () => {
  const original = new BVar(0);
  const opened = openTerm(original, 'x');
  const closed = closeTerm(opened, 'x');
  assert.equal(closed.idx, 0);
});

test('freeVars: (x y) → {x, y}', () => {
  const fv = freeVars(new App(new FVar('x'), new FVar('y')));
  assert.ok(fv.has('x') && fv.has('y'));
});

test('freeVars: λ.0 → {} (bound)', () => {
  assert.equal(freeVars(new Lam(new BVar(0))).size, 0);
});

test('subst: x[x:=42] → 42', () => {
  assert.equal(subst(new FVar('x'), 'x', new Num(42)).n, 42);
});

test('subst: y[x:=42] → y', () => {
  assert.equal(subst(new FVar('y'), 'x', new Num(42)).name, 'y');
});

test('locally closed: λ.0 → true', () => {
  assert.ok(isLocallyClosed(new Lam(new BVar(0))));
});

test('locally closed: BVar(0) → false (dangling)', () => {
  assert.ok(!isLocallyClosed(new BVar(0)));
});

test('locally closed: (λ.0) x → true', () => {
  assert.ok(isLocallyClosed(new App(new Lam(new BVar(0)), new FVar('x'))));
});

test('open under lambda: λ.BVar(1) opens to λ.FVar(x)', () => {
  const body = new Lam(new BVar(1)); // BVar(1) refers to outer binder
  const opened = openTerm(body, 'x');
  assert.equal(opened.body.name, 'x');
});

console.log(`\nLocally nameless tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
