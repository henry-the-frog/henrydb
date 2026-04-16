import { strict as assert } from 'assert';
import {
  TPolyVariant, tInt, tStr, tBool, VTag,
  isSubtype, typeEquals, unionVariants, intersectVariants,
  matchVariant, checkExhaustive
} from './poly-variants.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

function pv(tags) { return new TPolyVariant(new Map(Object.entries(tags))); }

// ============================================================
// Subtyping
// ============================================================

test('[`A|`B] <: [`A|`B|`C]', () => {
  const t1 = pv({ A: null, B: null });
  const t2 = pv({ A: null, B: null, C: null });
  assert.ok(isSubtype(t1, t2));
});

test('[`A|`B|`C] !<: [`A|`B]', () => {
  const t1 = pv({ A: null, B: null, C: null });
  const t2 = pv({ A: null, B: null });
  assert.ok(!isSubtype(t1, t2));
});

test('[`Ok(Int)] <: [`Ok(Int)|`Err(Str)]', () => {
  const t1 = new TPolyVariant(new Map([['Ok', tInt]]));
  const t2 = new TPolyVariant(new Map([['Ok', tInt], ['Err', tStr]]));
  assert.ok(isSubtype(t1, t2));
});

test('same type: subtype of itself', () => {
  const t = pv({ A: null, B: null });
  assert.ok(isSubtype(t, t));
});

// ============================================================
// Union and intersection
// ============================================================

test('union: [`A] ∪ [`B] = [`A|`B]', () => {
  const t1 = pv({ A: null });
  const t2 = pv({ B: null });
  const u = unionVariants(t1, t2);
  assert.ok(u.tags.has('A'));
  assert.ok(u.tags.has('B'));
});

test('intersect: [`A|`B] ∩ [`B|`C] = [`B]', () => {
  const t1 = pv({ A: null, B: null });
  const t2 = pv({ B: null, C: null });
  const i = intersectVariants(t1, t2);
  assert.ok(i.tags.has('B'));
  assert.ok(!i.tags.has('A'));
  assert.ok(!i.tags.has('C'));
});

// ============================================================
// Pattern matching
// ============================================================

test('match `Ok(42) against Ok/Err', () => {
  const val = new VTag('Ok', 42);
  const cases = [
    { tag: 'Ok', param: 'v', body: 'success' },
    { tag: 'Err', param: 'e', body: 'failure' },
  ];
  const result = matchVariant(val, cases);
  assert.ok(result.matched);
  assert.equal(result.body, 'success');
  assert.equal(result.arg, 42);
});

test('match `Err("oops") against Ok/Err', () => {
  const val = new VTag('Err', 'oops');
  const cases = [
    { tag: 'Ok', param: 'v', body: 'success' },
    { tag: 'Err', param: 'e', body: 'failure' },
  ];
  const result = matchVariant(val, cases);
  assert.equal(result.body, 'failure');
});

test('match unhandled tag', () => {
  const val = new VTag('Unknown');
  const cases = [{ tag: 'Ok', param: 'v', body: 'ok' }];
  const result = matchVariant(val, cases);
  assert.ok(!result.matched);
});

// ============================================================
// Exhaustiveness
// ============================================================

test('exhaustive: all tags covered', () => {
  const type = pv({ Ok: null, Err: null });
  const cases = [{ tag: 'Ok' }, { tag: 'Err' }];
  const { exhaustive, missing } = checkExhaustive(type, cases);
  assert.ok(exhaustive);
  assert.equal(missing.length, 0);
});

test('non-exhaustive: missing Err', () => {
  const type = pv({ Ok: null, Err: null });
  const cases = [{ tag: 'Ok' }];
  const { exhaustive, missing } = checkExhaustive(type, cases);
  assert.ok(!exhaustive);
  assert.deepStrictEqual(missing, ['Err']);
});

// ============================================================
// ToString
// ============================================================

test('toString: poly variant type', () => {
  const t = new TPolyVariant(new Map([['Ok', tInt], ['Err', tStr]]));
  const s = t.toString();
  assert.ok(s.includes('Ok'));
  assert.ok(s.includes('Err'));
});

// ============================================================
// Report
// ============================================================

console.log(`\nPolymorphic variant tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
