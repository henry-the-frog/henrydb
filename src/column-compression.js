// column-compression.js — Column compression: RLE, delta encoding, bit-packing
// These are the core compression techniques used in columnar databases
// (Parquet, ORC, Arrow). They exploit sorted or low-cardinality data.

/**
 * Run-Length Encoding: [a,a,a,b,b,c] → [{value:a,count:3},{value:b,count:2},{value:c,count:1}]
 */
export function rleEncode(values) {
  if (values.length === 0) return [];
  const runs = [];
  let current = values[0], count = 1;
  for (let i = 1; i < values.length; i++) {
    if (values[i] === current) {
      count++;
    } else {
      runs.push({ value: current, count });
      current = values[i];
      count = 1;
    }
  }
  runs.push({ value: current, count });
  return runs;
}

export function rleDecode(runs) {
  const result = [];
  for (const { value, count } of runs) {
    for (let i = 0; i < count; i++) result.push(value);
  }
  return result;
}

/**
 * Delta encoding: [100, 102, 105, 103] → {base: 100, deltas: [0, 2, 5, 3]}
 */
export function deltaEncode(values) {
  if (values.length === 0) return { base: 0, deltas: [] };
  const base = values[0];
  const deltas = values.map(v => v - base);
  return { base, deltas };
}

export function deltaDecode({ base, deltas }) {
  return deltas.map(d => base + d);
}

/**
 * Frame-of-reference + delta: first delta from base, then delta-of-deltas
 * [100, 102, 105, 109] → {base: 100, deltas: [2, 3, 4]}
 */
export function forDeltaEncode(values) {
  if (values.length <= 1) return { base: values[0] || 0, deltas: [] };
  const base = values[0];
  const deltas = [];
  for (let i = 1; i < values.length; i++) {
    deltas.push(values[i] - values[i - 1]);
  }
  return { base, deltas };
}

export function forDeltaDecode({ base, deltas }) {
  const result = [base];
  let current = base;
  for (const d of deltas) {
    current += d;
    result.push(current);
  }
  return result;
}

/**
 * Bit-packing: pack small integers into minimal bits.
 * E.g., values 0-15 need only 4 bits each instead of 32.
 */
export function bitPackEncode(values) {
  if (values.length === 0) return { bitWidth: 0, packed: new Uint8Array(0), count: 0 };
  
  const max = Math.max(...values);
  const bitWidth = max === 0 ? 1 : Math.ceil(Math.log2(max + 1));
  const totalBits = values.length * bitWidth;
  const packed = new Uint8Array(Math.ceil(totalBits / 8));
  
  let bitPos = 0;
  for (const val of values) {
    for (let b = 0; b < bitWidth; b++) {
      if (val & (1 << b)) {
        const byteIdx = Math.floor(bitPos / 8);
        const bitIdx = bitPos % 8;
        packed[byteIdx] |= (1 << bitIdx);
      }
      bitPos++;
    }
  }
  
  return { bitWidth, packed, count: values.length };
}

export function bitPackDecode({ bitWidth, packed, count }) {
  const result = [];
  let bitPos = 0;
  
  for (let i = 0; i < count; i++) {
    let val = 0;
    for (let b = 0; b < bitWidth; b++) {
      const byteIdx = Math.floor(bitPos / 8);
      const bitIdx = bitPos % 8;
      if (packed[byteIdx] & (1 << bitIdx)) {
        val |= (1 << b);
      }
      bitPos++;
    }
    result.push(val);
  }
  
  return result;
}

/**
 * Compute compression ratio.
 */
export function compressionRatio(originalBytes, compressedBytes) {
  return compressedBytes > 0 ? (originalBytes / compressedBytes).toFixed(2) : Infinity;
}
