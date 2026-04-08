// typed-columns.test.js — Tests for TypedArray-backed columns
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TypedColumn } from './typed-columns.js';

describe('TypedColumn', () => {

  it('INT column: push and get', () => {
    const col = new TypedColumn('INT');
    for (let i = 0; i < 100; i++) col.push(i * 10);
    assert.equal(col.length, 100);
    assert.equal(col.get(0), 0);
    assert.equal(col.get(99), 990);
  });

  it('FLOAT column: push and get', () => {
    const col = new TypedColumn('FLOAT');
    col.push(3.14);
    col.push(2.718);
    assert.ok(Math.abs(col.get(0) - 3.14) < 0.001);
    assert.ok(Math.abs(col.get(1) - 2.718) < 0.001);
  });

  it('handles null values', () => {
    const col = new TypedColumn('INT');
    col.push(10);
    col.push(null);
    col.push(30);
    assert.equal(col.get(0), 10);
    assert.equal(col.get(1), null);
    assert.equal(col.get(2), 30);
    assert.ok(col.isNull(1));
    assert.ok(!col.isNull(0));
  });

  it('auto-grows capacity', () => {
    const col = new TypedColumn('INT', 4);
    for (let i = 0; i < 1000; i++) col.push(i);
    assert.equal(col.length, 1000);
    assert.equal(col.get(999), 999);
  });

  it('SUM', () => {
    const col = new TypedColumn('INT');
    for (let i = 1; i <= 100; i++) col.push(i);
    assert.equal(col.sum(), 5050);
  });

  it('SUM with nulls', () => {
    const col = new TypedColumn('INT');
    col.push(10);
    col.push(null);
    col.push(30);
    assert.equal(col.sum(), 40);
  });

  it('COUNT', () => {
    const col = new TypedColumn('INT');
    col.push(1);
    col.push(null);
    col.push(3);
    assert.equal(col.count(), 2);
  });

  it('AVG', () => {
    const col = new TypedColumn('FLOAT');
    col.push(10);
    col.push(20);
    col.push(30);
    assert.equal(col.avg(), 20);
  });

  it('MIN and MAX', () => {
    const col = new TypedColumn('INT');
    col.push(50);
    col.push(10);
    col.push(90);
    col.push(30);
    assert.equal(col.min(), 10);
    assert.equal(col.max(), 90);
  });

  it('filterEquals', () => {
    const col = new TypedColumn('INT');
    for (let i = 0; i < 100; i++) col.push(i % 5);

    const indices = col.filterEquals(3);
    assert.equal(indices.length, 20);
    assert.ok(indices instanceof Uint32Array);
  });

  it('filterRange', () => {
    const col = new TypedColumn('INT');
    for (let i = 0; i < 100; i++) col.push(i);

    const indices = col.filterRange(20, 30);
    assert.equal(indices.length, 11); // 20..30 inclusive
  });

  it('filterGT and filterLT', () => {
    const col = new TypedColumn('INT');
    for (let i = 0; i < 100; i++) col.push(i);

    const gt90 = col.filterGT(90);
    assert.equal(gt90.length, 9); // 91..99

    const lt10 = col.filterLT(10);
    assert.equal(lt10.length, 10); // 0..9
  });

  it('sumSelection', () => {
    const col = new TypedColumn('INT');
    for (let i = 0; i < 10; i++) col.push(i * 10);

    const selection = new Uint32Array([0, 2, 4]); // values: 0, 20, 40
    assert.equal(col.sumSelection(selection), 60);
  });

  it('toArray returns view of active elements', () => {
    const col = new TypedColumn('INT', 1024);
    for (let i = 0; i < 5; i++) col.push(i);
    
    const arr = col.toArray();
    assert.equal(arr.length, 5);
    assert.ok(arr instanceof Int32Array);
  });

  it('benchmark: TypedColumn SUM vs JS array on 10M elements', () => {
    const col = new TypedColumn('INT', 10000000);
    const jsArr = [];
    for (let i = 0; i < 10000000; i++) {
      col.push(i % 1000);
      jsArr.push(i % 1000);
    }

    // TypedColumn SUM
    const t0 = Date.now();
    const typedSum = col.sum();
    const typedMs = Date.now() - t0;

    // JS array SUM
    const t1 = Date.now();
    let jsSum = 0;
    for (let i = 0; i < jsArr.length; i++) jsSum += jsArr[i];
    const jsMs = Date.now() - t1;

    console.log(`    TypedColumn: ${typedMs}ms vs JS Array: ${jsMs}ms (${(jsMs / Math.max(typedMs, 0.1)).toFixed(1)}x)`);
    assert.equal(typedSum, jsSum);
  });

  it('benchmark: TypedColumn filter vs JS array on 1M elements', () => {
    const col = new TypedColumn('INT', 1000000);
    const jsArr = new Array(1000000);
    for (let i = 0; i < 1000000; i++) {
      col.push(i);
      jsArr[i] = i;
    }

    // TypedColumn filterGT
    const t0 = Date.now();
    const typedResult = col.filterGT(900000);
    const typedMs = Date.now() - t0;

    // JS array filter
    const t1 = Date.now();
    const jsResult = [];
    for (let i = 0; i < jsArr.length; i++) {
      if (jsArr[i] > 900000) jsResult.push(i);
    }
    const jsMs = Date.now() - t1;

    console.log(`    TypedFilter: ${typedMs}ms vs JSFilter: ${jsMs}ms (${(jsMs / Math.max(typedMs, 0.1)).toFixed(1)}x)`);
    assert.equal(typedResult.length, jsResult.length);
  });
});
