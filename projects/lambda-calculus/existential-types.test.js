import { strict as assert } from 'assert';
import { pack, unpack, intCounter, objCounter, arrCounter, listStack, checkAbstraction } from './existential-types.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Counter tests — all implementations behave the same!
function counterTest(counter) {
  return unpack(counter, (_, ops) => {
    let c = ops.zero();
    c = ops.increment(c);
    c = ops.increment(c);
    c = ops.increment(c);
    c = ops.decrement(c);
    return ops.get(c);
  });
}

test('intCounter: increment 3, decrement 1 → 2', () => {
  assert.equal(counterTest(intCounter), 2);
});

test('objCounter: same behavior as intCounter', () => {
  assert.equal(counterTest(objCounter), 2);
});

test('arrCounter: same behavior as intCounter', () => {
  assert.equal(counterTest(arrCounter), 2);
});

test('all counters agree (abstraction preserved)', () => {
  assert.ok(checkAbstraction(intCounter, objCounter, counterTest));
  assert.ok(checkAbstraction(objCounter, arrCounter, counterTest));
  assert.ok(checkAbstraction(intCounter, arrCounter, counterTest));
});

// Stack tests
test('stack: push and pop', () => {
  const result = unpack(listStack, (_, ops) => {
    let s = ops.empty();
    s = ops.push(1, s);
    s = ops.push(2, s);
    s = ops.push(3, s);
    const top = ops.pop(s);
    return top.value;
  });
  assert.equal(result, 3);
});

test('stack: isEmpty', () => {
  const result = unpack(listStack, (_, ops) => {
    return ops.isEmpty(ops.empty());
  });
  assert.ok(result);
});

test('stack: not empty after push', () => {
  const result = unpack(listStack, (_, ops) => {
    return ops.isEmpty(ops.push(1, ops.empty()));
  });
  assert.ok(!result);
});

// Custom existential
test('custom: abstract set', () => {
  const setPackage = pack('Array', [], {
    empty: () => [],
    add: (x, s) => s.includes(x) ? s : [...s, x],
    contains: (x, s) => s.includes(x),
    size: (s) => s.length,
  });
  
  const result = unpack(setPackage, (_, ops) => {
    let s = ops.empty();
    s = ops.add(1, s);
    s = ops.add(2, s);
    s = ops.add(1, s); // Duplicate
    return ops.size(s);
  });
  assert.equal(result, 2); // No duplicates
});

// Abstraction guarantee
test('different implementations, same result', () => {
  // The key property: you can swap implementations without changing behavior
  const test1 = counterTest(intCounter);
  const test2 = counterTest(objCounter);
  const test3 = counterTest(arrCounter);
  assert.equal(test1, test2);
  assert.equal(test2, test3);
});

test('pack creates valid package', () => {
  const pkg = pack('Int', 42, { get: x => x });
  assert.equal(unpack(pkg, (val, ops) => ops.get(val)), 42);
});

console.log(`\nExistential types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
