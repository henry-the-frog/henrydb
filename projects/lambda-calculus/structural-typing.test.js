import { strict as assert } from 'assert';
import { StructType, structSubtype, structEquiv, commonFields, mergeTypes, matchesInterface } from './structural-typing.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const point2D = new StructType({ x: 'number', y: 'number' });
const point3D = new StructType({ x: 'number', y: 'number', z: 'number' });
const named = new StructType({ name: 'string' });

test('subtype: 3D <: 2D (width)', () => assert.ok(structSubtype(point3D, point2D)));
test('subtype: 2D !<: 3D', () => assert.ok(!structSubtype(point2D, point3D)));
test('subtype: self', () => assert.ok(structSubtype(point2D, point2D)));
test('equiv: same fields', () => assert.ok(structEquiv(point2D, new StructType({ x: 'number', y: 'number' }))));
test('equiv: different → false', () => assert.ok(!structEquiv(point2D, point3D)));
test('commonFields: 2D ∩ named = {}', () => assert.equal(commonFields(point2D, named).fields.size, 0));
test('commonFields: 3D ∩ 2D = {x,y}', () => assert.equal(commonFields(point3D, point2D).fields.size, 2));
test('mergeTypes: combine', () => {
  const r = mergeTypes(point2D, named);
  assert.equal(r.fields.size, 3);
});
test('mergeTypes: conflict → error', () => {
  assert.throws(() => mergeTypes(new StructType({ x: 'number' }), new StructType({ x: 'string' })), /Conflict/);
});
test('matchesInterface: ok', () => assert.ok(matchesInterface({ x: 1, y: 2, z: 3 }, point2D)));
test('matchesInterface: missing', () => assert.ok(!matchesInterface({ x: 1 }, point2D)));

console.log(`\nStructural typing tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
