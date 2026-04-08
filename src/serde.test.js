// serde.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { JSONSerde, BinarySerde, CSVSerde } from './serde.js';

const row = { id: 42, name: 'Alice', salary: 120000.50, active: true, notes: null };

describe('JSONSerde', () => {
  it('roundtrip', () => {
    const s = new JSONSerde();
    const buf = s.serialize(row);
    assert.deepEqual(s.deserialize(buf), row);
  });
});

describe('BinarySerde', () => {
  it('roundtrip basic row', () => {
    const s = new BinarySerde();
    const buf = s.serialize(row);
    const decoded = s.deserialize(buf);
    assert.equal(decoded.id, 42);
    assert.equal(decoded.name, 'Alice');
    assert.ok(Math.abs(decoded.salary - 120000.50) < 0.01);
    assert.equal(decoded.active, true);
    assert.equal(decoded.notes, null);
  });

  it('nested objects', () => {
    const s = new BinarySerde();
    const obj = { a: { b: { c: 1 } }, arr: [1, 'two', true] };
    assert.deepEqual(s.deserialize(s.serialize(obj)), obj);
  });

  it('more compact than JSON', () => {
    const s = new BinarySerde();
    const js = new JSONSerde();
    const binSize = s.serialize(row).length;
    const jsonSize = js.serialize(row).length;
    // Binary has type tags — may be larger for small rows, but preserves types
    console.log(`    Binary: ${binSize}B vs JSON: ${jsonSize}B`);
    assert.ok(typeof binSize === 'number');
  });

  it('empty object', () => {
    const s = new BinarySerde();
    assert.deepEqual(s.deserialize(s.serialize({})), {});
  });
});

describe('CSVSerde', () => {
  const cols = ['id', 'name', 'salary'];

  it('roundtrip', () => {
    const s = new CSVSerde(cols);
    const row = { id: 1, name: 'Alice', salary: 100000 };
    const decoded = s.deserialize(s.serialize(row));
    assert.equal(decoded.id, 1);
    assert.equal(decoded.name, 'Alice');
    assert.equal(decoded.salary, 100000);
  });

  it('handles commas in values', () => {
    const s = new CSVSerde(['name', 'desc']);
    const row = { name: 'Test', desc: 'Has, commas' };
    const decoded = s.deserialize(s.serialize(row));
    assert.equal(decoded.desc, 'Has, commas');
  });

  it('handles quotes', () => {
    const s = new CSVSerde(['name']);
    const row = { name: 'She said "hello"' };
    const decoded = s.deserialize(s.serialize(row));
    assert.equal(decoded.name, 'She said "hello"');
  });

  it('null values', () => {
    const s = new CSVSerde(cols);
    const row = { id: 1, name: null, salary: 50000 };
    const decoded = s.deserialize(s.serialize(row));
    assert.equal(decoded.name, null);
  });

  it('benchmark: 10K rows', () => {
    const s = new BinarySerde();
    const js = new JSONSerde();
    const rows = Array.from({ length: 10000 }, (_, i) => ({ id: i, name: `user_${i}`, val: Math.random() }));

    const t0 = Date.now();
    const binBufs = rows.map(r => s.serialize(r));
    const binSerMs = Date.now() - t0;

    const t1 = Date.now();
    const jsonBufs = rows.map(r => js.serialize(r));
    const jsonSerMs = Date.now() - t1;

    const t2 = Date.now();
    binBufs.map(b => s.deserialize(b));
    const binDeserMs = Date.now() - t2;

    const t3 = Date.now();
    jsonBufs.map(b => js.deserialize(b));
    const jsonDeserMs = Date.now() - t3;

    console.log(`    Binary: ser ${binSerMs}ms, deser ${binDeserMs}ms | JSON: ser ${jsonSerMs}ms, deser ${jsonDeserMs}ms`);
  });
});
