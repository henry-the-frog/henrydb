import { strict as assert } from 'assert';
import {
  RowEmpty, RowExtend, RowVar, RecordType, VariantType, Record,
  rowFields, rowHas, rowGet, rowWithout, makeRow,
  RowSubst, unifyRows, rowSubtype, checkRecord
} from './row-poly.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const empty = new RowEmpty();

// ============================================================
// Row construction
// ============================================================

test('empty row', () => {
  assert.equal(empty.toString(), '∅');
});

test('single field row', () => {
  const r = new RowExtend('name', 'String', empty);
  assert.equal(r.toString(), 'name: String');
});

test('multi-field row', () => {
  const r = makeRow([
    { label: 'name', type: 'String' },
    { label: 'age', type: 'Int' },
  ]);
  assert.ok(r.toString().includes('name'));
  assert.ok(r.toString().includes('age'));
});

test('open row with variable', () => {
  const r = makeRow([{ label: 'name', type: 'String' }], new RowVar('ρ'));
  assert.ok(r.toString().includes('ρ'));
});

test('record type toString', () => {
  const r = new RecordType(makeRow([{ label: 'x', type: 'Int' }]));
  assert.equal(r.toString(), '{x: Int}');
});

// ============================================================
// Row operations
// ============================================================

test('rowFields extracts fields', () => {
  const r = makeRow([
    { label: 'a', type: 'Int' },
    { label: 'b', type: 'Bool' },
  ]);
  const { fields, tail } = rowFields(r);
  assert.equal(fields.length, 2);
  assert.equal(fields[0].label, 'a');
  assert.equal(tail.tag, 'RowEmpty');
});

test('rowHas checks field existence', () => {
  const r = makeRow([{ label: 'name', type: 'String' }, { label: 'age', type: 'Int' }]);
  assert.ok(rowHas(r, 'name'));
  assert.ok(rowHas(r, 'age'));
  assert.ok(!rowHas(r, 'email'));
});

test('rowGet retrieves field type', () => {
  const r = makeRow([{ label: 'name', type: 'String' }, { label: 'age', type: 'Int' }]);
  assert.equal(rowGet(r, 'name'), 'String');
  assert.equal(rowGet(r, 'age'), 'Int');
  assert.equal(rowGet(r, 'email'), null);
});

test('rowWithout removes field', () => {
  const r = makeRow([{ label: 'a', type: 'Int' }, { label: 'b', type: 'Bool' }, { label: 'c', type: 'Str' }]);
  const r2 = rowWithout(r, 'b');
  assert.ok(rowHas(r2, 'a'));
  assert.ok(!rowHas(r2, 'b'));
  assert.ok(rowHas(r2, 'c'));
});

// ============================================================
// Record values
// ============================================================

test('record creation', () => {
  const r = new Record(new Map([['name', 'Alice'], ['age', 30]]));
  assert.equal(r.get('name'), 'Alice');
  assert.equal(r.get('age'), 30);
});

test('record extend', () => {
  const r = new Record(new Map([['name', 'Alice']]));
  const r2 = r.extend('age', 30);
  assert.equal(r2.get('age'), 30);
  assert.equal(r.has('age'), false); // original unchanged
});

test('record restrict', () => {
  const r = new Record(new Map([['name', 'Alice'], ['age', 30]]));
  const r2 = r.restrict('age');
  assert.ok(!r2.has('age'));
  assert.ok(r2.has('name'));
});

// ============================================================
// Row subtyping (width subtyping)
// ============================================================

test('{name, age} <: {name} (open)', () => {
  const r1 = makeRow([{ label: 'name', type: 'String' }, { label: 'age', type: 'Int' }]);
  const r2 = makeRow([{ label: 'name', type: 'String' }], new RowVar('ρ'));
  assert.ok(rowSubtype(r1, r2).isSubtype);
});

test('{name} !<: {name, age} (missing field)', () => {
  const r1 = makeRow([{ label: 'name', type: 'String' }]);
  const r2 = makeRow([{ label: 'name', type: 'String' }, { label: 'age', type: 'Int' }]);
  assert.ok(!rowSubtype(r1, r2).isSubtype);
});

test('{a, b, c} <: {a, c} (open)', () => {
  const r1 = makeRow([
    { label: 'a', type: 'Int' }, { label: 'b', type: 'Bool' }, { label: 'c', type: 'Str' }
  ]);
  const r2 = makeRow([{ label: 'a', type: 'Int' }, { label: 'c', type: 'Str' }], new RowVar('ρ'));
  assert.ok(rowSubtype(r1, r2).isSubtype);
});

// ============================================================
// Record type checking
// ============================================================

test('record matches type', () => {
  const r = new Record(new Map([['name', 'Alice'], ['age', 30]]));
  const t = new RecordType(makeRow([{ label: 'name', type: 'String' }, { label: 'age', type: 'Int' }]));
  assert.ok(checkRecord(r, t));
});

test('record missing field fails', () => {
  const r = new Record(new Map([['name', 'Alice']]));
  const t = new RecordType(makeRow([{ label: 'name', type: 'String' }, { label: 'age', type: 'Int' }]));
  assert.ok(!checkRecord(r, t));
});

test('record with extra fields matches open type', () => {
  const r = new Record(new Map([['name', 'Alice'], ['age', 30], ['email', 'a@b.c']]));
  const t = new RecordType(makeRow([{ label: 'name', type: 'String' }], new RowVar('ρ')));
  assert.ok(checkRecord(r, t));
});

// ============================================================
// Row unification
// ============================================================

test('unify same rows', () => {
  const r = makeRow([{ label: 'a', type: 'Int' }]);
  const subst = unifyRows(r, r);
  assert.ok(subst);
});

test('unify row with variable', () => {
  const r1 = makeRow([{ label: 'a', type: 'Int' }]);
  const r2 = new RowVar('ρ');
  const subst = unifyRows(r1, r2);
  assert.ok(subst.map.has('ρ'));
});

test('unify two row variables', () => {
  const r1 = new RowVar('ρ1');
  const r2 = new RowVar('ρ2');
  const subst = unifyRows(r1, r2);
  assert.ok(subst.map.size > 0);
});

// ============================================================
// Report
// ============================================================

console.log(`\nRow polymorphism tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
