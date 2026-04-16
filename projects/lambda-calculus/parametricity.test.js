import { strict as assert } from 'assert';
import {
  TForall, TFun, TList, TVar,
  freeTheorem, verifyListFreeTheorem, verifyBinaryFreeTheorem,
  validListFunctions, invalidListFunctions
} from './parametricity.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const g = x => x * 2;
const xs = [1, 2, 3, 4, 5];

// Valid implementations satisfy the free theorem
for (const [name, f] of Object.entries(validListFunctions)) {
  test(`free theorem holds: ${name}`, () => {
    assert.ok(verifyListFreeTheorem(f, g, xs),
      `map(g, ${name}(xs)) !== ${name}(map(g, xs))`);
  });
}

// Invalid implementations may violate the free theorem
test('sort violates free theorem (sometimes)', () => {
  // sort([3,1,2]) = [1,2,3], map(*2) = [2,4,6]
  // map(*2, [3,1,2]) = [6,2,4], sort = [2,4,6]
  // Actually same here! Let's use a function that truly breaks it:
  const sortFn = xs => [...xs].sort((a,b) => a-b);
  // With g = x => -x (negation reverses order):
  const neg = x => -x;
  const testXs = [3, 1, 2];
  const left = sortFn(testXs).map(neg);   // [1,2,3] → [-1,-2,-3]
  const right = sortFn(testXs.map(neg));   // [-3,-1,-2] → [-3,-2,-1]
  assert.notDeepStrictEqual(left, right);
});

// Free theorem generation
test('freeTheorem: ∀a. [a] → [a]', () => {
  const ty = new TForall('a', new TFun(new TList(new TVar('a')), new TList(new TVar('a'))));
  const ft = freeTheorem(ty);
  assert.ok(ft.quantifier === 'a');
});

// Binary free theorem: const satisfies it
test('binary: const(x,y)=x, g(const(x,y)) = const(g(x),g(y))', () => {
  const constFn = (x, y) => x;
  assert.ok(verifyBinaryFreeTheorem(constFn, g, 3, 5));
});

console.log(`\nParametricity tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
