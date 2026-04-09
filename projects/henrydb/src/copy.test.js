// copy.test.js
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { CopyManager } from './copy.js';

const cm = new CopyManager(null);

const sampleRows = [
  { id: 1, name: 'Alice', age: 30, active: true },
  { id: 2, name: 'Bob', age: 25, active: false },
  { id: 3, name: 'Carol', age: 35, active: true },
];

describe('COPY TO', () => {
  test('CSV with header', () => {
    const csv = cm.copyTo('users', sampleRows);
    const lines = csv.trim().split('\n');
    assert.equal(lines[0], 'id,name,age,active');
    assert.equal(lines[1], '1,Alice,30,true');
    assert.equal(lines.length, 4); // header + 3 rows
  });

  test('CSV without header', () => {
    const csv = cm.copyTo('users', sampleRows, { header: false });
    const lines = csv.trim().split('\n');
    assert.equal(lines.length, 3);
    assert.equal(lines[0], '1,Alice,30,true');
  });

  test('TSV format', () => {
    const tsv = cm.copyTo('users', sampleRows, { format: 'tsv' });
    assert.ok(tsv.includes('\t'));
    const lines = tsv.trim().split('\n');
    assert.equal(lines[0], 'id\tname\tage\tactive');
  });

  test('custom delimiter', () => {
    const data = cm.copyTo('users', sampleRows, { delimiter: '|' });
    assert.ok(data.includes('|'));
    assert.ok(data.includes('Alice'));
  });

  test('NULL handling', () => {
    const rows = [{ id: 1, name: null, age: 30 }];
    const csv = cm.copyTo('t', rows, { null: '\\N' });
    assert.ok(csv.includes('\\N'));
  });

  test('quotes fields with delimiters', () => {
    const rows = [{ id: 1, name: 'Smith, Jr.', value: 10 }];
    const csv = cm.copyTo('t', rows);
    assert.ok(csv.includes('"Smith, Jr."'));
  });

  test('escapes quotes in values', () => {
    const rows = [{ id: 1, name: 'She said "hello"', value: 10 }];
    const csv = cm.copyTo('t', rows);
    assert.ok(csv.includes('""hello""'));
  });

  test('specific columns', () => {
    const csv = cm.copyTo('users', sampleRows, { columns: ['name', 'age'] });
    const lines = csv.trim().split('\n');
    assert.equal(lines[0], 'name,age');
    assert.equal(lines[1], 'Alice,30');
  });
});

describe('COPY FROM', () => {
  test('CSV with header', () => {
    const csv = 'id,name,age\n1,Alice,30\n2,Bob,25\n';
    const result = cm.copyFrom(csv, []);
    assert.equal(result.count, 2);
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[0].age, 30);
  });

  test('CSV without header', () => {
    const csv = '1,Alice,30\n2,Bob,25\n';
    const result = cm.copyFrom(csv, ['id', 'name', 'age'], { header: false });
    assert.equal(result.count, 2);
    assert.equal(result.rows[0].id, 1);
  });

  test('TSV format', () => {
    const tsv = 'id\tname\n1\tAlice\n2\tBob\n';
    const result = cm.copyFrom(tsv, [], { format: 'tsv' });
    assert.equal(result.count, 2);
    assert.equal(result.rows[0].name, 'Alice');
  });

  test('auto-detects integer types', () => {
    const csv = 'id,value\n1,100\n2,200\n';
    const result = cm.copyFrom(csv, []);
    assert.equal(typeof result.rows[0].id, 'number');
    assert.equal(typeof result.rows[0].value, 'number');
  });

  test('auto-detects boolean types', () => {
    const csv = 'id,active\n1,true\n2,false\n';
    const result = cm.copyFrom(csv, []);
    assert.equal(result.rows[0].active, true);
    assert.equal(result.rows[1].active, false);
  });

  test('handles quoted CSV fields', () => {
    const csv = 'id,name\n1,"Smith, Jr."\n2,"Normal"\n';
    const result = cm.copyFrom(csv, []);
    assert.equal(result.rows[0].name, 'Smith, Jr.');
  });

  test('handles escaped quotes', () => {
    const csv = 'id,name\n1,"She said ""hello"""\n';
    const result = cm.copyFrom(csv, []);
    assert.equal(result.rows[0].name, 'She said "hello"');
  });

  test('NULL handling', () => {
    const csv = 'id,name\n1,\\N\n';
    const result = cm.copyFrom(csv, [], { null: '\\N' });
    assert.equal(result.rows[0].name, null);
  });

  test('roundtrip: TO then FROM', () => {
    const csv = cm.copyTo('t', sampleRows);
    const result = cm.copyFrom(csv, []);
    assert.equal(result.count, 3);
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[0].age, 30);
    assert.equal(result.rows[2].name, 'Carol');
  });

  test('empty data', () => {
    const result = cm.copyFrom('id,name\n', []);
    assert.equal(result.count, 0);
  });
});
