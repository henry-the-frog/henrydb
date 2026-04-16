import { strict as assert } from 'assert';
import {
  ePure, eIO, eExc, eState, ESet,
  effectUnion, effectEquals, effectSubset, isPure,
  inferEffect, ENum, EPerform, ESeq
} from './effect-polymorphism.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('pure ∪ IO = IO', () => assert.ok(effectEquals(effectUnion(ePure, eIO), eIO)));
test('IO ∪ IO = IO', () => assert.ok(effectEquals(effectUnion(eIO, eIO), eIO)));
test('pure ∪ pure = pure', () => assert.ok(isPure(effectUnion(ePure, ePure))));
test('IO ∪ Exc = {IO, Exception}', () => {
  const u = effectUnion(eIO, eExc);
  assert.equal(u.effects.length, 2);
});

test('effectSubset: IO ⊆ {IO, Exc}', () => {
  assert.ok(effectSubset(eIO, new ESet(['IO', 'Exception'])));
});
test('effectSubset: {IO, Exc} ⊄ IO', () => {
  assert.ok(!effectSubset(new ESet(['IO', 'Exception']), eIO));
});

test('isPure: empty', () => assert.ok(isPure(ePure)));
test('isPure: IO is not', () => assert.ok(!isPure(eIO)));

test('infer: literal is pure', () => {
  assert.ok(isPure(inferEffect(new ENum(42))));
});
test('infer: perform IO has IO effect', () => {
  assert.ok(effectEquals(inferEffect(new EPerform(eIO)), eIO));
});
test('infer: seq combines effects', () => {
  const effect = inferEffect(new ESeq(new EPerform(eIO), new EPerform(eExc)));
  assert.ok(effectSubset(eIO, effect));
  assert.ok(effectSubset(eExc, effect));
});

console.log(`\nEffect polymorphism tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
