import { strict as assert } from 'assert';
import { cZ, cS, cToInt, cPred, sZ, sS, sToInt, sPred, pZ, pS, pToInt, pPred, benchPred, benchFold } from './encoding-comparison.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

// Church
test('Church: 0', () => assert.equal(cToInt(cZ), 0));
test('Church: 3', () => assert.equal(cToInt(cS(cS(cS(cZ)))), 3));
test('Church: pred(3) = 2', () => assert.equal(cToInt(cPred(cS(cS(cS(cZ))))), 2));

// Scott
test('Scott: 0', () => assert.equal(sToInt(sZ), 0));
test('Scott: 3', () => assert.equal(sToInt(sS(sS(sS(sZ)))), 3));
test('Scott: pred(3) = 2', () => assert.equal(sToInt(sPred(sS(sS(sS(sZ))))), 2));

// Parigot
test('Parigot: 0', () => assert.equal(pToInt(pZ), 0));
test('Parigot: 3', () => assert.equal(pToInt(pS(pS(pS(pZ)))), 3));
test('Parigot: pred(3) = 2', () => assert.equal(pToInt(pPred(pS(pS(pS(pZ))))), 2));

// All agree
test('all agree on 5', () => {
  const c = cToInt(cS(cS(cS(cS(cS(cZ))))));
  const s = sToInt(sS(sS(sS(sS(sS(sZ))))));
  const p = pToInt(pS(pS(pS(pS(pS(pZ))))));
  assert.equal(c, 5); assert.equal(s, 5); assert.equal(p, 5);
});

// Benchmark fold
test('fold: all give same result for n=5', () => {
  assert.equal(benchFold('church', 5), 5);
  assert.equal(benchFold('scott', 5), 5);
  assert.equal(benchFold('parigot', 5), 5);
});

console.log(`\nEncoding comparison tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
