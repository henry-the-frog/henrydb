import { strict as assert } from 'assert';
import {
  TRecord, TFun, tInt, tNat, tStr, tBool, tTop, tBot,
  isSubtype, circle, rect
} from './subtype-poly.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Base subtyping
test('Nat <: Int', () => assert.ok(isSubtype(tNat, tInt)));
test('Int <: Top', () => assert.ok(isSubtype(tInt, tTop)));
test('Nat <: Top (transitive)', () => assert.ok(isSubtype(tNat, tTop)));
test('Int !<: Nat', () => assert.ok(!isSubtype(tInt, tNat)));
test('Bot <: everything', () => assert.ok(isSubtype(tBot, tInt)));

// Record subtyping
test('width: {x,y,z} <: {x,y}', () => {
  const t1 = new TRecord(new Map([['x', tInt], ['y', tInt], ['z', tInt]]));
  const t2 = new TRecord(new Map([['x', tInt], ['y', tInt]]));
  assert.ok(isSubtype(t1, t2));
});

test('width: {x,y} !<: {x,y,z}', () => {
  const t1 = new TRecord(new Map([['x', tInt], ['y', tInt]]));
  const t2 = new TRecord(new Map([['x', tInt], ['y', tInt], ['z', tInt]]));
  assert.ok(!isSubtype(t1, t2));
});

test('depth: {x:Nat} <: {x:Int}', () => {
  const t1 = new TRecord(new Map([['x', tNat]]));
  const t2 = new TRecord(new Map([['x', tInt]]));
  assert.ok(isSubtype(t1, t2));
});

// Function subtyping
test('function: contravariant param', () => {
  // (Top → Int) <: (Int → Int)  because Top ⊇ Int
  assert.ok(isSubtype(new TFun(tTop, tInt), new TFun(tInt, tInt)));
});

test('function: covariant return', () => {
  // (Int → Nat) <: (Int → Int)  because Nat ⊆ Int
  assert.ok(isSubtype(new TFun(tInt, tNat), new TFun(tInt, tInt)));
});

// Virtual dispatch
test('circle area', () => {
  const c = circle(5);
  assert.ok(Math.abs(c.call('area') - Math.PI * 25) < 0.01);
});

test('rect area', () => {
  const r = rect(3, 4);
  assert.equal(r.call('area'), 12);
});

test('polymorphic dispatch: shapes', () => {
  const shapes = [circle(1), rect(2, 3)];
  const areas = shapes.map(s => s.call('area'));
  assert.ok(Math.abs(areas[0] - Math.PI) < 0.01);
  assert.equal(areas[1], 6);
});

console.log(`\nSubtype polymorphism tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
