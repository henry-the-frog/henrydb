// lz77.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LZ77 } from './lz77.js';

describe('LZ77', () => {
  const lz = new LZ77();

  it('roundtrip: simple string', () => {
    const input = 'ABCABCABCABC';
    const tokens = lz.compress(input);
    const output = lz.decompress(tokens);
    assert.equal(output.toString(), input);
  });

  it('roundtrip: repeated pattern gets compressed', () => {
    const input = 'AAAAAAAAAAAAAAAAAAAAAAAAA'; // 25 A's
    const tokens = lz.compress(input);
    // Should have references (not 25 literals)
    const refs = tokens.filter(t => t.type === 'ref');
    assert.ok(refs.length > 0, 'Expected back-references for repeated data');
    assert.equal(lz.decompress(tokens).toString(), input);
  });

  it('roundtrip: no repeats = all literals', () => {
    const input = 'ABCDEFGHIJ';
    const tokens = lz.compress(input);
    // All unique chars within minMatch, so mostly literals
    assert.equal(lz.decompress(tokens).toString(), input);
  });

  it('roundtrip: binary data', () => {
    const input = Buffer.from([1, 2, 3, 1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6]);
    const tokens = lz.compress(input);
    const output = lz.decompress(tokens);
    assert.deepEqual(output, input);
  });

  it('roundtrip: empty input', () => {
    const tokens = lz.compress('');
    assert.equal(tokens.length, 0);
    assert.equal(lz.decompress(tokens).length, 0);
  });

  it('serialize/deserialize roundtrip', () => {
    const input = 'the quick brown fox the quick brown dog';
    const tokens = lz.compress(input);
    const binary = lz.serialize(tokens);
    const restored = lz.deserialize(binary);
    assert.deepEqual(restored, tokens);
    assert.equal(lz.decompress(restored).toString(), input);
  });

  it('compressToBinary / decompressFromBinary convenience', () => {
    const input = 'hello world hello world hello world';
    const binary = lz.compressToBinary(input);
    const output = lz.decompressFromBinary(binary);
    assert.equal(output.toString(), input);
  });

  it('compression ratio for repetitive data', () => {
    // Simulate a database page with repeated column values
    const rows = Array(100).fill('status=active,type=user,role=member\n').join('');
    const stats = lz.stats(rows);
    console.log(`  LZ77 stats: ${stats.originalSize}B → ${stats.compressedSize}B (${stats.ratio}x), ${stats.references} refs`);
    assert.ok(parseFloat(stats.ratio) > 1.5, `Expected compression, got ${stats.ratio}x`);
  });

  it('handles overlapping back-references', () => {
    // "ABABABAB" — the pattern AB repeats, and refs can overlap
    const input = 'ABABABABABABABAB';
    const tokens = lz.compress(input);
    assert.equal(lz.decompress(tokens).toString(), input);
  });

  it('custom window size', () => {
    const small = new LZ77(8); // Very small window
    const input = 'XYZXYZ' + 'A'.repeat(20) + 'XYZXYZ';
    // With window=8, the second XYZXYZ can't see the first one
    const tokens = small.compress(input);
    assert.equal(small.decompress(tokens).toString(), input);
  });

  it('large input roundtrip', () => {
    // Generate pseudo-random but compressible data
    const parts = [];
    for (let i = 0; i < 200; i++) {
      parts.push(`row_${i % 20}:value_${i % 5}\n`);
    }
    const input = parts.join('');
    const output = lz.decompressFromBinary(lz.compressToBinary(input));
    assert.equal(output.toString(), input);
  });
});
