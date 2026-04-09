// ordered-map.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { OrderedMap } from './ordered-map.js';

describe('OrderedMap', () => {
  it('preserves insertion order', () => {
    const om = new OrderedMap();
    om.set('c', 3); om.set('a', 1); om.set('b', 2);
    assert.deepEqual([...om].map(e => e.key), ['c', 'a', 'b']);
  });

  it('first and last', () => {
    const om = new OrderedMap();
    om.set('x', 1); om.set('y', 2); om.set('z', 3);
    assert.equal(om.first().key, 'x');
    assert.equal(om.last().key, 'z');
  });
});
