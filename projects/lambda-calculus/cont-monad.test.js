import { strict as assert } from 'assert';
import { creturn, cbind, callcc, runCont, earlyReturn, tryCatch } from './cont-monad.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('return + runCont', () => assert.equal(runCont(creturn(42)), 42));

test('bind: chain two operations', () => {
  const result = runCont(cbind(creturn(5), x => creturn(x + 1)));
  assert.equal(result, 6);
});

test('bind: chain three', () => {
  const result = runCont(
    cbind(creturn(2), x =>
    cbind(creturn(x * 3), y =>
    creturn(y + 1))));
  assert.equal(result, 7);
});

test('callcc: no escape used → normal', () => {
  const result = runCont(callcc(k => creturn(42)));
  assert.equal(result, 42);
});

test('callcc: escape used → early return', () => {
  const result = runCont(
    cbind(callcc(exit => cbind(exit(42), () => creturn(99))), x => creturn(x + 1)));
  // exit(42) jumps out, 99 never reached
  // Then x = 42, so result = 43
  assert.equal(result, 43);
});

test('earlyReturn: positive → doubled', () => {
  assert.equal(runCont(earlyReturn(5)), 10);
});

test('earlyReturn: negative → "negative!"', () => {
  assert.equal(runCont(earlyReturn(-3)), 'negative!');
});

test('tryCatch: no error', () => {
  const result = runCont(tryCatch(
    throwErr => creturn(42),
    err => creturn(`caught: ${err}`)));
  assert.equal(result, 42);
});

test('tryCatch: with error', () => {
  const result = runCont(tryCatch(
    throwErr => cbind(throwErr('oops'), () => creturn(99)),
    err => creturn(`caught: ${err}`)));
  assert.equal(result, 'caught: oops');
});

test('monad law: left identity', () => {
  const f = x => creturn(x * 2);
  assert.equal(runCont(cbind(creturn(5), f)), runCont(f(5)));
});

test('monad law: right identity', () => {
  const m = creturn(42);
  assert.equal(runCont(cbind(m, creturn)), runCont(m));
});

console.log(`\nCont monad tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
