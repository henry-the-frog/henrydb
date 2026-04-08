// histogram.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Histogram } from './histogram.js';

describe('Histogram', () => {
  const values = Array.from({ length: 1000 }, (_, i) => i);

  it('creates equi-depth buckets', () => {
    const hist = new Histogram(values, 10);
    assert.equal(hist.buckets.length, 10);
    assert.equal(hist.totalRows, 1000);
    assert.equal(hist.min, 0);
    assert.equal(hist.max, 999);
  });

  it('NDV count', () => {
    const hist = new Histogram(values);
    assert.equal(hist.ndv, 1000);
  });

  it('MCV for skewed data', () => {
    const skewed = [];
    for (let i = 0; i < 500; i++) skewed.push(42);
    for (let i = 0; i < 500; i++) skewed.push(i);
    const hist = new Histogram(skewed, 10);
    assert.equal(hist.mcv[0].value, 42);
    assert.ok(hist.mcv[0].frequency > 0.4);
  });

  it('estimateEQ for MCV value', () => {
    const skewed = [];
    for (let i = 0; i < 500; i++) skewed.push(42);
    for (let i = 0; i < 500; i++) skewed.push(i);
    const hist = new Histogram(skewed, 10);
    assert.ok(hist.estimateEQ(42) > 0.4);
  });

  it('estimateEQ for non-MCV value', () => {
    const hist = new Histogram(values, 10);
    const sel = hist.estimateEQ(500);
    assert.ok(sel > 0 && sel < 0.01);
  });

  it('estimateRange', () => {
    const hist = new Histogram(values, 10);
    const sel = hist.estimateRange(0, 499);
    assert.ok(sel >= 0.4 && sel <= 0.6); // ~50%
  });

  it('estimateRange full range', () => {
    const hist = new Histogram(values, 10);
    const sel = hist.estimateRange(0, 999);
    assert.ok(sel >= 0.9);
  });

  it('handles nulls', () => {
    const withNulls = [...values.slice(0, 900), ...Array(100).fill(null)];
    const hist = new Histogram(withNulls, 10);
    assert.equal(hist.nullCount, 100);
    assert.ok(Math.abs(hist.nullFraction - 0.1) < 0.01);
  });

  it('empty input', () => {
    const hist = new Histogram([]);
    assert.equal(hist.buckets.length, 0);
    assert.equal(hist.ndv, 0);
  });

  it('single value', () => {
    const hist = new Histogram([42, 42, 42]);
    assert.equal(hist.min, 42);
    assert.equal(hist.max, 42);
    assert.equal(hist.ndv, 1);
  });
});
