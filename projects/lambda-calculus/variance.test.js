import { strict as assert } from 'assert';
import {
  COVARIANT, CONTRAVARIANT, INVARIANT, BIVARIANT,
  TBase, TApp, TFun, TypeCtor,
  isSubtype, inferVariance, composeVariance
} from './variance.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const Cat = new TBase('Cat');
const Dog = new TBase('Dog');
const Animal = new TBase('Animal');
const Int = new TBase('Int');

const ctors = new Map([
  ['List', new TypeCtor('List', [{ name: 'T', variance: COVARIANT }])],
  ['Func', new TypeCtor('Func', [{ name: 'In', variance: CONTRAVARIANT }, { name: 'Out', variance: COVARIANT }])],
  ['MutRef', new TypeCtor('MutRef', [{ name: 'T', variance: INVARIANT }])],
  ['Phantom', new TypeCtor('Phantom', [{ name: 'T', variance: BIVARIANT }])],
]);

// Covariant (List)
test('covariant: List<Cat> <: List<Animal>', () => {
  assert.ok(isSubtype(new TApp('List', [Cat]), new TApp('List', [Animal]), ctors));
});

test('covariant: List<Animal> !<: List<Cat>', () => {
  assert.ok(!isSubtype(new TApp('List', [Animal]), new TApp('List', [Cat]), ctors));
});

// Contravariant (Consumer/Function input)
test('contravariant: Func<Animal,Int> <: Func<Cat,Int>', () => {
  assert.ok(isSubtype(
    new TApp('Func', [Animal, Int]),
    new TApp('Func', [Cat, Int]),
    ctors));
});

test('contravariant: Func<Cat,Int> !<: Func<Animal,Int>', () => {
  assert.ok(!isSubtype(
    new TApp('Func', [Cat, Int]),
    new TApp('Func', [Animal, Int]),
    ctors));
});

// Invariant (MutableRef)
test('invariant: MutRef<Cat> !<: MutRef<Animal>', () => {
  assert.ok(!isSubtype(new TApp('MutRef', [Cat]), new TApp('MutRef', [Animal]), ctors));
});

test('invariant: MutRef<Cat> <: MutRef<Cat> (same type)', () => {
  assert.ok(isSubtype(new TApp('MutRef', [Cat]), new TApp('MutRef', [Cat]), ctors));
});

// Bivariant (Phantom)
test('bivariant: Phantom<Cat> <: Phantom<Animal>', () => {
  assert.ok(isSubtype(new TApp('Phantom', [Cat]), new TApp('Phantom', [Animal]), ctors));
});

test('bivariant: Phantom<Animal> <: Phantom<Cat>', () => {
  assert.ok(isSubtype(new TApp('Phantom', [Animal]), new TApp('Phantom', [Cat]), ctors));
});

// Variance inference
test('infer: covariant (return position)', () => {
  const v = inferVariance('T', new TFun(new TBase('Int'), new TBase('T')));
  assert.equal(v, COVARIANT);
});

test('infer: contravariant (param position)', () => {
  const v = inferVariance('T', new TFun(new TBase('T'), new TBase('Int')));
  assert.equal(v, CONTRAVARIANT);
});

test('infer: invariant (both positions)', () => {
  const v = inferVariance('T', new TFun(new TBase('T'), new TBase('T')));
  assert.equal(v, INVARIANT);
});

// Compose variance
test('compose: + ∘ + = +', () => assert.equal(composeVariance(COVARIANT, COVARIANT), COVARIANT));
test('compose: + ∘ - = -', () => assert.equal(composeVariance(COVARIANT, CONTRAVARIANT), CONTRAVARIANT));
test('compose: - ∘ - = +', () => assert.equal(composeVariance(CONTRAVARIANT, CONTRAVARIANT), COVARIANT));

console.log(`\nVariance tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
