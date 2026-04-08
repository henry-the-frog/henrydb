// tuple-descriptor.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TupleDescriptor } from './tuple-descriptor.js';

describe('TupleDescriptor', () => {
  const schema = new TupleDescriptor([
    { name: 'id', type: 'INT32' },
    { name: 'name', type: 'VARCHAR' },
    { name: 'salary', type: 'FLOAT64' },
    { name: 'active', type: 'BOOL' },
  ]);

  it('roundtrip basic row', () => {
    const row = { id: 42, name: 'Alice', salary: 120000.50, active: true };
    const buf = schema.serialize(row);
    const decoded = schema.deserialize(buf);
    assert.deepEqual(decoded, row);
  });

  it('null values', () => {
    const row = { id: 1, name: null, salary: 50000, active: false };
    const buf = schema.serialize(row);
    const decoded = schema.deserialize(buf);
    assert.equal(decoded.name, null);
    assert.equal(decoded.id, 1);
  });

  it('all nulls', () => {
    const row = { id: null, name: null, salary: null, active: null };
    const buf = schema.serialize(row);
    const decoded = schema.deserialize(buf);
    assert.deepEqual(decoded, row);
  });

  it('unicode strings', () => {
    const row = { id: 1, name: '日本語テスト 🎉', salary: 0, active: true };
    const buf = schema.serialize(row);
    const decoded = schema.deserialize(buf);
    assert.equal(decoded.name, '日本語テスト 🎉');
  });

  it('empty string', () => {
    const row = { id: 0, name: '', salary: 0, active: false };
    const buf = schema.serialize(row);
    const decoded = schema.deserialize(buf);
    assert.equal(decoded.name, '');
  });

  it('negative numbers', () => {
    const row = { id: -100, name: 'test', salary: -999.99, active: false };
    const buf = schema.serialize(row);
    const decoded = schema.deserialize(buf);
    assert.equal(decoded.id, -100);
    assert.ok(Math.abs(decoded.salary - (-999.99)) < 0.001);
  });

  it('estimate size', () => {
    const row = { id: 1, name: 'Alice', salary: 100000, active: true };
    const estimated = schema.estimateSize(row);
    const actual = schema.serialize(row).length;
    assert.equal(estimated, actual);
  });

  it('column names', () => {
    assert.deepEqual(schema.columnNames, ['id', 'name', 'salary', 'active']);
  });

  it('compact binary representation', () => {
    const row = { id: 1, name: 'Alice', salary: 120000, active: true };
    const buf = schema.serialize(row);
    const jsonSize = Buffer.byteLength(JSON.stringify(row));
    console.log(`    Binary: ${buf.length} bytes vs JSON: ${jsonSize} bytes (${(jsonSize / buf.length).toFixed(1)}x)`);
    assert.ok(buf.length < jsonSize);
  });

  it('benchmark: 10K serialize/deserialize', () => {
    const rows = Array.from({ length: 10000 }, (_, i) => ({
      id: i, name: `user_${i}`, salary: 50000 + Math.random() * 100000, active: i % 2 === 0,
    }));

    const t0 = Date.now();
    const buffers = rows.map(r => schema.serialize(r));
    const serMs = Date.now() - t0;

    const t1 = Date.now();
    const decoded = buffers.map(b => schema.deserialize(b));
    const deserMs = Date.now() - t1;

    console.log(`    10K rows: serialize ${serMs}ms, deserialize ${deserMs}ms`);
    assert.equal(decoded.length, 10000);
    assert.equal(decoded[0].id, 0);
  });
});
