import { strict as assert } from 'assert';
import { TypeFamily, Equation, Add1, Append, If } from './type-families.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('Add1: Z → S(Z)', () => assert.equal(Add1.apply('Z'), 'S(Z)'));
test('Add1: S(Z) → S(S(Z))', () => assert.equal(Add1.apply('S(Z)'), 'S(S(Z))'));
test('Append: Nil ys → ys', () => assert.equal(Append.apply('Nil', 'list'), 'list'));
test('If: True → then', () => assert.equal(If.apply('True', 'Int', 'Bool'), 'Int'));
test('If: False → else', () => assert.equal(If.apply('False', 'Int', 'Bool'), 'Bool'));

test('custom family', () => {
  const Not = new TypeFamily('Not', [
    new Equation(['True'], 'False'),
    new Equation(['False'], 'True'),
  ]);
  assert.equal(Not.apply('True'), 'False');
  assert.equal(Not.apply('False'), 'True');
});

test('no match → error', () => {
  const F = new TypeFamily('F', [new Equation(['A'], 'B')]);
  assert.throws(() => F.apply('C'), /No match/);
});

test('multi-arg family', () => {
  const Pair = new TypeFamily('Pair', [
    new Equation(['$a', '$b'], b => `(${b.get('$a')}, ${b.get('$b')})`),
  ]);
  assert.equal(Pair.apply('Int', 'Bool'), '(Int, Bool)');
});

test('overlapping: first match wins', () => {
  const F = new TypeFamily('F', [
    new Equation(['Int'], 'found-int'),
    new Equation(['$x'], b => `generic-${b.get('$x')}`),
  ]);
  assert.equal(F.apply('Int'), 'found-int');
  assert.equal(F.apply('Bool'), 'generic-Bool');
});

test('literal pattern + wildcard', () => {
  const F = new TypeFamily('F', [new Equation(['A', '$x'], b => b.get('$x'))]);
  assert.equal(F.apply('A', 'result'), 'result');
  assert.throws(() => F.apply('B', 'result'), /No match/);
});

console.log(`\nType families tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
