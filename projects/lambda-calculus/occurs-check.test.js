import { strict as assert } from 'assert';
import { TVar, TFun, TCon, occurs, findInfiniteTypes, unifyWithCheck, applySubst } from './occurs-check.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const a = new TVar('a'), b = new TVar('b');
const tInt = new TCon('Int'), tBool = new TCon('Bool');

test('occurs: a in a → true', () => assert.ok(occurs('a', a)));
test('occurs: a in Int → false', () => assert.ok(!occurs('a', tInt)));
test('occurs: a in a → Int → true', () => assert.ok(occurs('a', new TFun(a, tInt))));
test('occurs: a in List<a> → true', () => assert.ok(occurs('a', new TCon('List', [a]))));
test('occurs: b in a → Int → false', () => assert.ok(!occurs('b', new TFun(a, tInt))));

test('unify: a = Int → ok', () => assert.ok(unifyWithCheck(a, tInt).ok));
test('unify: a = a → a → FAIL (infinite)', () => assert.ok(!unifyWithCheck(a, new TFun(a, a)).ok));
test('unify: Int = Int → ok', () => assert.ok(unifyWithCheck(tInt, tInt).ok));
test('unify: Int = Bool → fail', () => assert.ok(!unifyWithCheck(tInt, tBool).ok));

test('findInfiniteTypes: detects a = List<a>', () => {
  const issues = findInfiniteTypes([[a, new TCon('List', [a])]]);
  assert.ok(issues.length > 0);
  assert.equal(issues[0].issue, 'infinite type');
});

test('unify: a→b = Int→Bool → ok', () => {
  const r = unifyWithCheck(new TFun(a, b), new TFun(tInt, tBool));
  assert.ok(r.ok);
  assert.equal(applySubst(r.subst, a).name, 'Int');
});

console.log(`\n🎉🎉🎉 MODULE #120! Occurs check tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
