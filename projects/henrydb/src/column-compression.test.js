// column-compression.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  rleEncode, rleDecode,
  deltaEncode, deltaDecode,
  forDeltaEncode, forDeltaDecode,
  bitPackEncode, bitPackDecode,
  compressionRatio,
} from './column-compression.js';

describe('Column Compression', () => {
  describe('RLE', () => {
    it('encode/decode', () => {
      const data = ['a', 'a', 'a', 'b', 'b', 'c', 'c', 'c', 'c'];
      const encoded = rleEncode(data);
      assert.equal(encoded.length, 3);
      assert.deepEqual(encoded[0], { value: 'a', count: 3 });
      assert.deepEqual(rleDecode(encoded), data);
    });

    it('single value column', () => {
      const data = Array(1000).fill('active');
      const encoded = rleEncode(data);
      assert.equal(encoded.length, 1);
      assert.equal(encoded[0].count, 1000);
    });

    it('all distinct values', () => {
      const data = [1, 2, 3, 4, 5];
      const encoded = rleEncode(data);
      assert.equal(encoded.length, 5); // No compression
    });
  });

  describe('Delta Encoding', () => {
    it('encode/decode', () => {
      const data = [100, 102, 105, 103, 108];
      const encoded = deltaEncode(data);
      assert.equal(encoded.base, 100);
      assert.deepEqual(encoded.deltas, [0, 2, 5, 3, 8]);
      assert.deepEqual(deltaDecode(encoded), data);
    });

    it('sequential timestamps', () => {
      const data = Array.from({ length: 100 }, (_, i) => 1000000 + i * 1000);
      const encoded = deltaEncode(data);
      // All deltas are small numbers vs large timestamps
      assert.ok(Math.max(...encoded.deltas) < 100000);
    });
  });

  describe('Frame-of-Reference Delta', () => {
    it('encode/decode', () => {
      const data = [100, 102, 105, 109, 115];
      const encoded = forDeltaEncode(data);
      assert.equal(encoded.base, 100);
      assert.deepEqual(encoded.deltas, [2, 3, 4, 6]);
      assert.deepEqual(forDeltaDecode(encoded), data);
    });

    it('monotonically increasing', () => {
      const data = Array.from({ length: 100 }, (_, i) => i * 10);
      const encoded = forDeltaEncode(data);
      // All deltas should be 10
      assert.ok(encoded.deltas.every(d => d === 10));
    });
  });

  describe('Bit Packing', () => {
    it('encode/decode small values', () => {
      const data = [0, 1, 2, 3, 4, 5, 6, 7]; // 3 bits each
      const encoded = bitPackEncode(data);
      assert.equal(encoded.bitWidth, 3);
      assert.deepEqual(bitPackDecode(encoded), data);
    });

    it('compression: 4-bit values in Uint8', () => {
      const data = Array.from({ length: 100 }, (_, i) => i % 16); // 0-15 = 4 bits
      const encoded = bitPackEncode(data);
      assert.equal(encoded.bitWidth, 4);
      // 100 values × 4 bits = 50 bytes (vs 400 bytes for Int32Array)
      assert.equal(encoded.packed.length, 50);
      assert.deepEqual(bitPackDecode(encoded), data);
    });

    it('roundtrip 1000 values', () => {
      const data = Array.from({ length: 1000 }, () => Math.floor(Math.random() * 64));
      const encoded = bitPackEncode(data);
      assert.deepEqual(bitPackDecode(encoded), data);
    });
  });

  describe('Compression Ratio', () => {
    it('RLE on sorted low-cardinality', () => {
      const data = [];
      for (let i = 0; i < 10; i++) for (let j = 0; j < 1000; j++) data.push(i);
      const encoded = rleEncode(data);
      const ratio = compressionRatio(data.length * 4, encoded.length * 8);
      console.log(`    RLE ratio: ${ratio}x (10K values → ${encoded.length} runs)`);
      assert.ok(parseFloat(ratio) > 100);
    });

    it('bit-packing on small integers', () => {
      const data = Array.from({ length: 10000 }, (_, i) => i % 8);
      const encoded = bitPackEncode(data);
      const ratio = compressionRatio(data.length * 4, encoded.packed.length);
      console.log(`    Bit-packing ratio: ${ratio}x (${data.length * 4}B → ${encoded.packed.length}B)`);
      assert.ok(parseFloat(ratio) > 5);
    });
  });
});
