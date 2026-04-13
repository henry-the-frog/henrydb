// composite-key.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CompositeKey, makeCompositeKey } from './composite-key.js';

describe('CompositeKey', () => {
  it('creates key from values', () => {
    const k = new CompositeKey([1, 'hello', 42]);
    assert.deepEqual(k.values, [1, 'hello', 42]);
  });

  it('equal keys have equal string representation', () => {
    const k1 = new CompositeKey([1, 'abc']);
    const k2 = new CompositeKey([1, 'abc']);
    assert.equal(k1.valueOf(), k2.valueOf());
  });

  it('different keys have different string representation', () => {
    const k1 = new CompositeKey([1, 'abc']);
    const k2 = new CompositeKey([2, 'abc']);
    assert.notEqual(k1.valueOf(), k2.valueOf());
  });

  it('orders numbers correctly', () => {
    const k1 = new CompositeKey([1]);
    const k2 = new CompositeKey([10]);
    const k3 = new CompositeKey([2]);
    assert.ok(k1.valueOf() < k2.valueOf());
    assert.ok(k3.valueOf() < k2.valueOf());
    assert.ok(k1.valueOf() < k3.valueOf());
  });

  it('orders strings correctly', () => {
    const k1 = new CompositeKey(['apple']);
    const k2 = new CompositeKey(['banana']);
    assert.ok(k1.valueOf() < k2.valueOf());
  });

  it('handles null values', () => {
    const k1 = new CompositeKey([null, 1]);
    const k2 = new CompositeKey([1, 1]);
    assert.ok(k1.valueOf() < k2.valueOf(), 'null should sort before numbers');
  });

  it('handles negative numbers', () => {
    const k1 = new CompositeKey([-10]);
    const k2 = new CompositeKey([0]);
    const k3 = new CompositeKey([10]);
    assert.ok(k1.valueOf() < k2.valueOf());
    assert.ok(k2.valueOf() < k3.valueOf());
  });

  it('composite order: first column takes priority', () => {
    const k1 = new CompositeKey([1, 'z']);
    const k2 = new CompositeKey([2, 'a']);
    assert.ok(k1.valueOf() < k2.valueOf());
  });

  it('composite order: same first, second decides', () => {
    const k1 = new CompositeKey([1, 'a']);
    const k2 = new CompositeKey([1, 'b']);
    assert.ok(k1.valueOf() < k2.valueOf());
  });

  it('startsWith prefix matching', () => {
    const k = new CompositeKey([1, 'hello', 42]);
    assert.ok(k.startsWith([1]));
    assert.ok(k.startsWith([1, 'hello']));
    assert.ok(k.startsWith([1, 'hello', 42]));
    assert.ok(!k.startsWith([2]));
    assert.ok(!k.startsWith([1, 'world']));
  });

  it('makeCompositeKey factory', () => {
    const k = makeCompositeKey([3, 'test']);
    assert.ok(k instanceof CompositeKey);
    assert.deepEqual(k.values, [3, 'test']);
  });

  it('mixed types in composite key', () => {
    const k1 = new CompositeKey([1, 'abc', null]);
    const k2 = new CompositeKey([1, 'abc', 0]);
    assert.ok(k1.valueOf() < k2.valueOf(), 'null should sort before numbers');
  });
});
