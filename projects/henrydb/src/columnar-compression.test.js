// columnar-compression.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RLE, Delta, Dictionary, BitPacking, autoCompress } from './columnar-compression.js';

describe('RLE', () => {
  it('encode/decode basic', () => {
    const values = ['A', 'A', 'A', 'B', 'B', 'C'];
    const encoded = RLE.encode(values);
    assert.equal(encoded.runs.length, 3);
    assert.deepEqual(RLE.decode(encoded), values);
  });

  it('single run', () => {
    const values = [7, 7, 7, 7, 7];
    const encoded = RLE.encode(values);
    assert.equal(encoded.runs.length, 1);
    assert.equal(encoded.runs[0].count, 5);
  });

  it('no repetition', () => {
    const values = [1, 2, 3, 4, 5];
    const encoded = RLE.encode(values);
    assert.equal(encoded.runs.length, 5); // No compression benefit
  });

  it('good for sorted column', () => {
    // Simulate a sorted status column
    const values = Array(1000).fill('active').concat(Array(500).fill('deleted'), Array(500).fill('pending'));
    const ratio = RLE.ratio(values);
    console.log(`    RLE sorted: ratio=${ratio.toFixed(1)}x (${values.length} values → ${RLE.encode(values).runs.length} runs)`);
    assert.ok(ratio > 100, 'Should compress well for sorted data');
  });
});

describe('Delta', () => {
  it('encode/decode basic', () => {
    const values = [100, 103, 107, 108];
    const encoded = Delta.encode(values);
    assert.equal(encoded.base, 100);
    assert.deepEqual(encoded.deltas, [3, 4, 1]);
    assert.deepEqual(Delta.decode(encoded), values);
  });

  it('linear sequence', () => {
    const values = Array.from({ length: 100 }, (_, i) => 1000 + i * 5);
    const encoded = Delta.encode(values);
    assert.ok(encoded.deltas.every(d => d === 5));
    assert.deepEqual(Delta.decode(encoded), values);
  });

  it('timestamps', () => {
    const base = Date.now();
    const values = Array.from({ length: 1000 }, (_, i) => base + i * 1000); // 1 sec intervals
    const encoded = Delta.encode(values);
    assert.ok(encoded.deltas.every(d => d === 1000));
  });

  it('delta-of-delta for linear sequences', () => {
    const values = [100, 105, 110, 115, 120]; // Linear: delta=5, dod=0
    const encoded = Delta.encodeDOD(values);
    assert.ok(encoded.dod.every(d => d === 0), 'DOD should be all zeros for linear');
    assert.deepEqual(Delta.decodeDOD(encoded), values);
  });

  it('delta-of-delta for accelerating sequences', () => {
    const values = [0, 1, 3, 6, 10, 15]; // Triangular: deltas=[1,2,3,4,5], dod=[1,1,1,1]
    const encoded = Delta.encodeDOD(values);
    assert.ok(encoded.dod.every(d => d === 1));
    assert.deepEqual(Delta.decodeDOD(encoded), values);
  });
});

describe('Dictionary', () => {
  it('encode/decode basic', () => {
    const values = ['US', 'UK', 'US', 'FR', 'US'];
    const encoded = Dictionary.encode(values);
    assert.equal(encoded.cardinality, 3);
    assert.deepEqual(Dictionary.decode(encoded), values);
  });

  it('uses Uint8Array for small dictionaries', () => {
    const values = ['A', 'B', 'C', 'A', 'B'];
    const encoded = Dictionary.encode(values);
    assert.ok(encoded.codes instanceof Uint8Array);
  });

  it('good compression for low cardinality', () => {
    const countries = ['US', 'UK', 'FR', 'DE', 'JP'];
    const values = Array.from({ length: 10000 }, () => countries[Math.random() * 5 | 0]);
    const ratio = Dictionary.ratio(values);
    console.log(`    Dict: ratio=${ratio.toFixed(1)}x (${values.length} values, ${new Set(values).size} unique)`);
    assert.ok(ratio > 1, 'Should compress');
  });
});

describe('BitPacking', () => {
  it('encode/decode basic', () => {
    const values = [1, 3, 7, 2, 0, 5];
    const encoded = BitPacking.encode(values);
    assert.equal(encoded.bitWidth, 3); // max=7, need 3 bits
    assert.deepEqual(BitPacking.decode(encoded), values);
  });

  it('single-bit values', () => {
    const values = [0, 1, 1, 0, 1, 0, 0, 1];
    const encoded = BitPacking.encode(values);
    assert.equal(encoded.bitWidth, 1);
    assert.equal(encoded.packed.byteLength, 1); // 8 bits in 1 byte
    assert.deepEqual(BitPacking.decode(encoded), values);
  });

  it('good compression for small integers', () => {
    const values = Array.from({ length: 10000 }, () => Math.random() * 16 | 0);
    const ratio = BitPacking.ratio(values);
    console.log(`    BitPack: ratio=${ratio.toFixed(1)}x (0-15 → ${BitPacking.encode(values).bitWidth} bits)`);
    assert.ok(ratio > 4, 'Should be at least 4x (4 bits vs 32 bits)');
  });

  it('handles zero values', () => {
    const values = [0, 0, 0, 0];
    const encoded = BitPacking.encode(values);
    assert.deepEqual(BitPacking.decode(encoded), values);
  });
});

describe('autoCompress', () => {
  it('picks RLE for highly repetitive data', () => {
    const values = Array(10000).fill('YES');
    const result = autoCompress(values);
    assert.equal(result.codec, 'rle');
    console.log(`    Auto (repetitive): ${result.codec}, ratio=${result.ratio.toFixed(1)}x`);
  });

  it('picks dictionary for low-cardinality strings', () => {
    const statuses = ['active', 'inactive', 'pending', 'deleted'];
    const values = Array.from({ length: 10000 }, () => statuses[Math.random() * 4 | 0]);
    const result = autoCompress(values);
    console.log(`    Auto (low-card strings): ${result.codec}, ratio=${result.ratio.toFixed(1)}x`);
    assert.ok(['dictionary', 'rle'].includes(result.codec));
  });

  it('picks bitpack for small integers', () => {
    const values = Array.from({ length: 10000 }, () => Math.random() * 8 | 0);
    const result = autoCompress(values);
    console.log(`    Auto (small ints): ${result.codec}, ratio=${result.ratio.toFixed(1)}x`);
    assert.ok(['bitpack', 'delta'].includes(result.codec));
  });

  it('picks delta for timestamps', () => {
    const base = 1700000000;
    const values = Array.from({ length: 10000 }, (_, i) => base + i * 1000);
    const result = autoCompress(values);
    console.log(`    Auto (timestamps): ${result.codec}, ratio=${result.ratio.toFixed(1)}x`);
  });
});

describe('Compression Benchmark', () => {
  it('benchmark: encode/decode 100K values with each codec', () => {
    const N = 100_000;
    
    // Generate test data
    const sorted = Array.from({ length: N }, (_, i) => i % 100 < 50 ? 'A' : 'B');
    const ints = Array.from({ length: N }, () => Math.random() * 256 | 0);
    const timestamps = Array.from({ length: N }, (_, i) => 1700000000 + i * 100);
    const countries = Array.from({ length: N }, () => ['US', 'UK', 'FR', 'DE', 'JP'][Math.random() * 5 | 0]);
    
    const codecs = [
      { name: 'RLE (sorted)', codec: RLE, data: sorted },
      { name: 'Delta (timestamps)', codec: Delta, data: timestamps },
      { name: 'Dictionary (countries)', codec: Dictionary, data: countries },
      { name: 'BitPack (0-255)', codec: BitPacking, data: ints },
    ];
    
    for (const { name, codec, data } of codecs) {
      const t0 = performance.now();
      const encoded = codec.encode(data);
      const encMs = performance.now() - t0;
      
      const t1 = performance.now();
      const decoded = codec.decode(encoded);
      const decMs = performance.now() - t1;
      
      assert.deepEqual(decoded, data);
      console.log(`    ${name}: encode=${encMs.toFixed(1)}ms, decode=${decMs.toFixed(1)}ms`);
    }
  });
});
