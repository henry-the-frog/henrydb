import { strict as assert } from 'assert';
import { freturn, fbind, get, put, del, runInMemory, runWithLogging } from './free-monad.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('pure: return value', () => {
  const { value } = runInMemory(freturn(42));
  assert.equal(value, 42);
});

test('put + get', () => {
  const prog = fbind(put('name', 'Alice'), () => get('name'));
  const { value } = runInMemory(prog);
  assert.equal(value, 'Alice');
});

test('put + put + get (overwrite)', () => {
  const prog = fbind(put('x', 1), () => fbind(put('x', 2), () => get('x')));
  const { value } = runInMemory(prog);
  assert.equal(value, 2);
});

test('get missing key → null', () => {
  const { value } = runInMemory(get('missing'));
  assert.equal(value, null);
});

test('put + delete + get → null', () => {
  const prog = fbind(put('x', 42), () => fbind(del('x'), () => get('x')));
  const { value } = runInMemory(prog);
  assert.equal(value, null);
});

test('complex: multiple keys', () => {
  const prog = fbind(put('a', 1), () =>
    fbind(put('b', 2), () =>
      fbind(get('a'), a =>
        fbind(get('b'), b =>
          freturn(a + b)))));
  const { value } = runInMemory(prog);
  assert.equal(value, 3);
});

test('store persists across operations', () => {
  const prog = fbind(put('counter', 0), () =>
    fbind(get('counter'), n =>
      fbind(put('counter', n + 1), () =>
        get('counter'))));
  const { value } = runInMemory(prog);
  assert.equal(value, 1);
});

// ============================================================
// Logging interpreter (same program, different interpretation!)
// ============================================================

test('logging: records operations', () => {
  const prog = fbind(put('x', 42), () => get('x'));
  const { log } = runWithLogging(prog);
  assert.equal(log.length, 2);
  assert.ok(log[0].includes('PUT'));
  assert.ok(log[1].includes('GET'));
});

test('logging: captures values', () => {
  const prog = fbind(put('name', 'Bob'), () => get('name'));
  const { log, value } = runWithLogging(prog);
  assert.equal(value, 'Bob');
  assert.ok(log[1].includes('Bob'));
});

test('same program, two interpreters', () => {
  const prog = fbind(put('x', 42), () => get('x'));
  const memResult = runInMemory(prog);
  const logResult = runWithLogging(prog);
  assert.equal(memResult.value, logResult.value); // Same result!
  assert.ok(logResult.log.length > 0); // But logging has extra info
});

console.log(`\nFree monad tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
