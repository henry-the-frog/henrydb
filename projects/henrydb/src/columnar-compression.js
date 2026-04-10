// columnar-compression.js — Compression codecs for columnar data
//
// Four codecs commonly used in columnar databases (Parquet, ORC, Arrow):
//   1. RLE (Run-Length Encoding) — repeated values
//   2. Delta Encoding — monotonically increasing sequences
//   3. Dictionary Encoding — low-cardinality strings
//   4. Bit-Packing — small integers packed into minimal bits
//
// Each codec: encode(values) → compressed, decode(compressed) → values

// ============================================================
// 1. RLE — Run-Length Encoding
// ============================================================
// Great for sorted columns or columns with many repeated values.
// Example: [A, A, A, B, B, C] → [(A,3), (B,2), (C,1)]

export const RLE = {
  encode(values) {
    if (values.length === 0) return { runs: [], originalLength: 0 };
    const runs = [];
    let current = values[0];
    let count = 1;
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
    return { runs, originalLength: values.length };
  },

  decode(compressed) {
    const values = [];
    for (const { value, count } of compressed.runs) {
      for (let i = 0; i < count; i++) values.push(value);
    }
    return values;
  },

  /** Compression ratio: original_size / compressed_size */
  ratio(values) {
    const encoded = this.encode(values);
    const originalSize = values.length;
    const compressedSize = encoded.runs.length * 2; // value + count per run
    return originalSize / compressedSize;
  },
};

// ============================================================
// 2. Delta Encoding
// ============================================================
// Great for timestamps, auto-increment IDs, sorted numeric columns.
// Example: [100, 103, 107, 108] → {base: 100, deltas: [3, 4, 1]}

export const Delta = {
  encode(values) {
    if (values.length === 0) return { base: 0, deltas: [] };
    const base = values[0];
    const deltas = new Array(values.length - 1);
    for (let i = 1; i < values.length; i++) {
      deltas[i - 1] = values[i] - values[i - 1];
    }
    return { base, deltas };
  },

  decode(compressed) {
    const values = [compressed.base];
    let current = compressed.base;
    for (const d of compressed.deltas) {
      current += d;
      values.push(current);
    }
    return values;
  },

  /** Enhanced: delta-of-delta for linear sequences */
  encodeDOD(values) {
    if (values.length <= 2) return this.encode(values);
    const base = values[0];
    const baseSlope = values[1] - values[0];
    const dod = new Array(values.length - 2);
    let prevDelta = baseSlope;
    for (let i = 2; i < values.length; i++) {
      const delta = values[i] - values[i - 1];
      dod[i - 2] = delta - prevDelta;
      prevDelta = delta;
    }
    return { base, baseSlope, dod };
  },

  decodeDOD(compressed) {
    const values = [compressed.base, compressed.base + compressed.baseSlope];
    let prevDelta = compressed.baseSlope;
    for (const dd of compressed.dod) {
      prevDelta += dd;
      values.push(values[values.length - 1] + prevDelta);
    }
    return values;
  },
};

// ============================================================
// 3. Dictionary Encoding
// ============================================================
// Great for string columns with low cardinality (country, status, category).
// Example: ["US", "UK", "US", "FR", "US"] → {dict: ["US","UK","FR"], codes: [0,1,0,2,0]}

export const Dictionary = {
  encode(values) {
    const dict = [];
    const dictMap = new Map();
    const codes = new Array(values.length);
    
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (!dictMap.has(v)) {
        dictMap.set(v, dict.length);
        dict.push(v);
      }
      codes[i] = dictMap.get(v);
    }
    
    // Use smallest possible integer type
    const maxCode = dict.length - 1;
    let typedCodes;
    if (maxCode < 256) {
      typedCodes = new Uint8Array(codes);
    } else if (maxCode < 65536) {
      typedCodes = new Uint16Array(codes);
    } else {
      typedCodes = new Uint32Array(codes);
    }
    
    return { dict, codes: typedCodes, cardinality: dict.length };
  },

  decode(compressed) {
    const values = new Array(compressed.codes.length);
    for (let i = 0; i < compressed.codes.length; i++) {
      values[i] = compressed.dict[compressed.codes[i]];
    }
    return values;
  },

  /** Compute compression ratio */
  ratio(values) {
    const encoded = this.encode(values);
    const originalSize = values.reduce((s, v) => s + (typeof v === 'string' ? v.length : 4), 0);
    const dictSize = encoded.dict.reduce((s, v) => s + (typeof v === 'string' ? v.length : 4), 0);
    const codeSize = encoded.codes.byteLength;
    return originalSize / (dictSize + codeSize);
  },
};

// ============================================================
// 4. Bit-Packing
// ============================================================
// Pack small integers using minimum required bits.
// Example: values 0-7 need only 3 bits each instead of 32.
// [1, 3, 7, 2, 0, 5] → packed into ceil(6*3/8) = 3 bytes

export const BitPacking = {
  encode(values) {
    if (values.length === 0) return { packed: new Uint8Array(0), bitWidth: 0, count: 0 };
    
    // Determine bit width needed
    let maxVal = 0;
    for (const v of values) if (v > maxVal) maxVal = v;
    const bitWidth = maxVal === 0 ? 1 : Math.ceil(Math.log2(maxVal + 1));
    
    // Pack values into bytes
    const totalBits = values.length * bitWidth;
    const packed = new Uint8Array(Math.ceil(totalBits / 8));
    let bitPos = 0;
    
    for (const v of values) {
      // Write bitWidth bits of v starting at bitPos
      let remaining = bitWidth;
      let val = v;
      while (remaining > 0) {
        const byteIdx = bitPos >>> 3;
        const bitOffset = bitPos & 7;
        const bitsToWrite = Math.min(remaining, 8 - bitOffset);
        const mask = (1 << bitsToWrite) - 1;
        packed[byteIdx] |= (val & mask) << bitOffset;
        val >>>= bitsToWrite;
        bitPos += bitsToWrite;
        remaining -= bitsToWrite;
      }
    }
    
    return { packed, bitWidth, count: values.length };
  },

  decode(compressed) {
    const { packed, bitWidth, count } = compressed;
    const values = new Array(count);
    let bitPos = 0;
    
    for (let i = 0; i < count; i++) {
      let val = 0;
      let remaining = bitWidth;
      let shift = 0;
      while (remaining > 0) {
        const byteIdx = bitPos >>> 3;
        const bitOffset = bitPos & 7;
        const bitsToRead = Math.min(remaining, 8 - bitOffset);
        const mask = (1 << bitsToRead) - 1;
        val |= ((packed[byteIdx] >>> bitOffset) & mask) << shift;
        shift += bitsToRead;
        bitPos += bitsToRead;
        remaining -= bitsToRead;
      }
      values[i] = val;
    }
    
    return values;
  },

  /** Compression ratio */
  ratio(values) {
    const encoded = this.encode(values);
    return (values.length * 4) / (encoded.packed.byteLength || 1); // vs 32-bit ints
  },
};

// ============================================================
// Codec selector — auto-choose best compression for a column
// ============================================================

export function autoCompress(values, columnType = 'auto') {
  if (values.length === 0) return { codec: 'none', data: values, ratio: 1 };
  
  const candidates = [];
  
  // Try RLE
  const rleResult = RLE.encode(values);
  const rleRatio = values.length / (rleResult.runs.length * 2);
  candidates.push({ codec: 'rle', ratio: rleRatio, data: rleResult });
  
  // Try Dictionary (for strings or low-cardinality)
  if (typeof values[0] === 'string') {
    const dictResult = Dictionary.encode(values);
    const dictRatio = Dictionary.ratio(values);
    candidates.push({ codec: 'dictionary', ratio: dictRatio, data: dictResult });
  }
  
  // Try Delta (for numbers)
  if (typeof values[0] === 'number') {
    const deltaResult = Delta.encode(values);
    // Estimate ratio: deltas are often smaller than original values
    const maxDelta = Math.max(...deltaResult.deltas.map(Math.abs), 1);
    const deltaBits = Math.ceil(Math.log2(maxDelta + 1));
    const deltaRatio = (values.length * 32) / (32 + deltaResult.deltas.length * deltaBits);
    candidates.push({ codec: 'delta', ratio: deltaRatio, data: deltaResult });
    
    // Try Bit-Packing (for small non-negative integers)
    if (values.every(v => v >= 0 && Number.isInteger(v))) {
      const bpResult = BitPacking.encode(values);
      const bpRatio = (values.length * 4) / (bpResult.packed.byteLength || 1);
      candidates.push({ codec: 'bitpack', ratio: bpRatio, data: bpResult });
    }
  }
  
  // Pick best compression ratio
  candidates.sort((a, b) => b.ratio - a.ratio);
  return candidates[0];
}
