// compression.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RLE, DeltaEncoding } from './compression.js';

describe('RLE', () => {
  it('encode and decode', () => {
    const arr = ['a', 'a', 'a', 'b', 'b', 'c'];
    const encoded = RLE.encode(arr);
    assert.equal(encoded.length, 3);
    assert.deepEqual(RLE.decode(encoded), arr);
  });

  it('compression ratio for sorted column', () => {
    const col = Array(1000).fill('M').concat(Array(500).fill('F'));
    const ratio = RLE.ratio(col);
    console.log(`  RLE ratio: ${ratio.toFixed(0)}x (1500 → 2 runs)`);
    assert.ok(ratio > 100);
  });
});

describe('DeltaEncoding', () => {
  it('encode and decode', () => {
    const arr = [100, 102, 105, 110, 120];
    const deltas = DeltaEncoding.encode(arr);
    assert.deepEqual(deltas, [100, 2, 3, 5, 10]);
    assert.deepEqual(DeltaEncoding.decode(deltas), arr);
  });

  it('timestamps', () => {
    const timestamps = [1000, 1001, 1002, 1005, 1010];
    const deltas = DeltaEncoding.encode(timestamps);
    assert.deepEqual(deltas, [1000, 1, 1, 3, 5]);
  });
});
