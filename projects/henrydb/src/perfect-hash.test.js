// perfect-hash.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PerfectHash } from './perfect-hash.js';

describe('PerfectHash', () => {
  it('maps keys to unique positions', () => {
    const keys = ['apple', 'banana', 'cherry', 'date', 'elderberry'];
    const ph = new PerfectHash(keys);
    
    const positions = new Set();
    for (const k of keys) {
      const pos = ph.get(k);
      assert.ok(pos >= 0 && pos < keys.length, `Out of range: ${pos}`);
      positions.add(pos);
    }
    assert.equal(positions.size, keys.length, 'Collisions detected');
  });

  it('works with 100 keys', () => {
    const keys = Array.from({ length: 100 }, (_, i) => `key-${i}`);
    const ph = new PerfectHash(keys);
    
    const positions = new Set();
    for (const k of keys) positions.add(ph.get(k));
    assert.equal(positions.size, 100);
  });
});
