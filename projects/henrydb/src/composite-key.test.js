// composite-key.test.js — Tests for CompositeKey
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CompositeKey, makeCompositeKey } from './composite-key.js';

describe('CompositeKey', () => {
  it('integer comparison: ascending order', () => {
    const k1 = new CompositeKey([1]);
    const k2 = new CompositeKey([2]);
    const k3 = new CompositeKey([10]);
    assert.ok(k1 < k2, '1 < 2');
    assert.ok(k2 < k3, '2 < 10');
    assert.ok(k1 < k3, '1 < 10');
  });

  it('negative numbers sort correctly', () => {
    const km5 = new CompositeKey([-5]);
    const km1 = new CompositeKey([-1]);
    const k0 = new CompositeKey([0]);
    const k5 = new CompositeKey([5]);
    assert.ok(km5 < km1, '-5 < -1');
    assert.ok(km1 < k0, '-1 < 0');
    assert.ok(k0 < k5, '0 < 5');
    assert.ok(km5 < k5, '-5 < 5');
  });

  it('string comparison: alphabetical order', () => {
    const ka = new CompositeKey(['apple']);
    const kb = new CompositeKey(['banana']);
    const kc = new CompositeKey(['cherry']);
    assert.ok(ka < kb, 'apple < banana');
    assert.ok(kb < kc, 'banana < cherry');
  });

  it('null sorts first', () => {
    const kn = new CompositeKey([null]);
    const k1 = new CompositeKey([1]);
    const ks = new CompositeKey(['hello']);
    assert.ok(kn < k1, 'null < 1');
    assert.ok(kn < ks, 'null < string');
  });

  it('numbers sort before strings', () => {
    const knum = new CompositeKey([5]);
    const kstr = new CompositeKey(['5']);
    assert.ok(knum < kstr, 'number < string (by type prefix)');
  });

  it('composite key: multi-column sort', () => {
    const k1 = new CompositeKey([1, 'a']);
    const k2 = new CompositeKey([1, 'b']);
    const k3 = new CompositeKey([2, 'a']);
    assert.ok(k1 < k2, '(1,a) < (1,b)');
    assert.ok(k2 < k3, '(1,b) < (2,a)');
    assert.ok(k1 < k3, '(1,a) < (2,a)');
  });

  it('equality check', () => {
    const k1 = new CompositeKey([1, 'hello']);
    const k2 = new CompositeKey([1, 'hello']);
    assert.equal(k1.valueOf(), k2.valueOf());
  });

  it('startsWith: prefix matching', () => {
    const k = new CompositeKey([1, 'a', 100]);
    assert.ok(k.startsWith([1]));
    assert.ok(k.startsWith([1, 'a']));
    assert.ok(k.startsWith([1, 'a', 100]));
    assert.ok(!k.startsWith([2]));
    assert.ok(!k.startsWith([1, 'b']));
    assert.ok(!k.startsWith([1, 'a', 100, 'extra']));
  });

  it('makeCompositeKey factory', () => {
    const k = makeCompositeKey([42, 'test']);
    assert.ok(k instanceof CompositeKey);
    assert.deepEqual(k.values, [42, 'test']);
  });

  it('large numbers sort correctly', () => {
    const k1 = new CompositeKey([1000000]);
    const k2 = new CompositeKey([9999999]);
    const k3 = new CompositeKey([10000000]);
    assert.ok(k1 < k2, '1M < 9.99M');
    assert.ok(k2 < k3, '9.99M < 10M');
  });

  it('zero sorts between negative and positive', () => {
    const km = new CompositeKey([-100]);
    const kz = new CompositeKey([0]);
    const kp = new CompositeKey([100]);
    assert.ok(km < kz, '-100 < 0');
    assert.ok(kz < kp, '0 < 100');
  });

  it('float ordering', () => {
    const k1 = new CompositeKey([1.5]);
    const k2 = new CompositeKey([2.5]);
    const k3 = new CompositeKey([10.5]);
    // Note: float comparison may fail due to string representation
    // This test documents current behavior
    const sorted = [k3, k1, k2].sort((a, b) => {
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    });
    assert.equal(sorted[0].values[0], 1.5);
    assert.equal(sorted[1].values[0], 2.5);
    assert.equal(sorted[2].values[0], 10.5);
  });

  it('mixed null and non-null composite', () => {
    const k1 = new CompositeKey([1, null]);
    const k2 = new CompositeKey([1, 'a']);
    const k3 = new CompositeKey([null, 'a']);
    // null sorts first due to \x00 prefix
    assert.ok(k1 < k2, '(1,null) < (1,a)');
    assert.ok(k3 < k1, '(null,a) < (1,null)');
  });
});
