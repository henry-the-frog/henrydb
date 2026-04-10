// lz77.js — LZ77 sliding window compression
// Used in databases for page compression, WAL compression, backup compression.
// This is the core algorithm behind gzip/deflate (without Huffman coding layer).

export class LZ77 {
  /**
   * @param {number} windowSize - Sliding window size (default 4096)
   * @param {number} minMatch - Minimum match length to encode as reference (default 3)
   * @param {number} maxMatch - Maximum match length (default 258, like deflate)
   */
  constructor(windowSize = 4096, minMatch = 3, maxMatch = 258) {
    this.windowSize = windowSize;
    this.minMatch = minMatch;
    this.maxMatch = maxMatch;
  }

  /**
   * Compress a buffer/string into LZ77 tokens.
   * Each token is either:
   *   { type: 'lit', value: byte }        — literal byte
   *   { type: 'ref', offset, length }     — back-reference (offset back, length bytes)
   * 
   * @param {string|Buffer|Uint8Array} input
   * @returns {Array<{type: string, value?: number, offset?: number, length?: number}>}
   */
  compress(input) {
    const data = typeof input === 'string' 
      ? Buffer.from(input) 
      : (input instanceof Uint8Array ? input : Buffer.from(input));
    const tokens = [];
    let pos = 0;

    while (pos < data.length) {
      const windowStart = Math.max(0, pos - this.windowSize);
      let bestOffset = 0;
      let bestLength = 0;

      // Search for longest match in the sliding window
      for (let j = windowStart; j < pos; j++) {
        let matchLen = 0;
        while (
          matchLen < this.maxMatch &&
          pos + matchLen < data.length &&
          data[j + matchLen] === data[pos + matchLen]
        ) {
          matchLen++;
        }
        if (matchLen > bestLength) {
          bestLength = matchLen;
          bestOffset = pos - j;
          if (matchLen >= this.maxMatch) break; // Can't do better
        }
      }

      if (bestLength >= this.minMatch) {
        tokens.push({ type: 'ref', offset: bestOffset, length: bestLength });
        pos += bestLength;
      } else {
        tokens.push({ type: 'lit', value: data[pos] });
        pos++;
      }
    }

    return tokens;
  }

  /**
   * Decompress LZ77 tokens back to a buffer.
   * @param {Array} tokens
   * @returns {Buffer}
   */
  decompress(tokens) {
    const output = [];

    for (const token of tokens) {
      if (token.type === 'lit') {
        output.push(token.value);
      } else if (token.type === 'ref') {
        const start = output.length - token.offset;
        // Copy byte-by-byte to handle overlapping refs (e.g., repeating patterns)
        for (let i = 0; i < token.length; i++) {
          output.push(output[start + i]);
        }
      }
    }

    return Buffer.from(output);
  }

  /**
   * Serialize tokens to a compact binary format.
   * Format per token:
   *   Literal:   0x00 <byte>                       (2 bytes)
   *   Reference: 0x01 <offset:uint16> <length:uint16> (5 bytes)
   * 
   * @param {Array} tokens
   * @returns {Buffer}
   */
  serialize(tokens) {
    const parts = [];
    for (const t of tokens) {
      if (t.type === 'lit') {
        const buf = Buffer.alloc(2);
        buf[0] = 0x00;
        buf[1] = t.value;
        parts.push(buf);
      } else {
        const buf = Buffer.alloc(5);
        buf[0] = 0x01;
        buf.writeUInt16LE(t.offset, 1);
        buf.writeUInt16LE(t.length, 3);
        parts.push(buf);
      }
    }
    return Buffer.concat(parts);
  }

  /**
   * Deserialize binary format back to tokens.
   * @param {Buffer} data
   * @returns {Array}
   */
  deserialize(data) {
    const tokens = [];
    let pos = 0;
    while (pos < data.length) {
      if (data[pos] === 0x00) {
        tokens.push({ type: 'lit', value: data[pos + 1] });
        pos += 2;
      } else if (data[pos] === 0x01) {
        tokens.push({
          type: 'ref',
          offset: data.readUInt16LE(pos + 1),
          length: data.readUInt16LE(pos + 3),
        });
        pos += 5;
      } else {
        throw new Error(`Invalid token type: 0x${data[pos].toString(16)} at pos ${pos}`);
      }
    }
    return tokens;
  }

  /**
   * Convenience: compress input to binary.
   * @param {string|Buffer} input
   * @returns {Buffer}
   */
  compressToBinary(input) {
    return this.serialize(this.compress(input));
  }

  /**
   * Convenience: decompress from binary.
   * @param {Buffer} data
   * @returns {Buffer}
   */
  decompressFromBinary(data) {
    return this.decompress(this.deserialize(data));
  }

  /**
   * Compute compression statistics.
   * @param {string|Buffer} input
   * @returns {{ originalSize: number, compressedSize: number, ratio: string, tokens: number, literals: number, references: number }}
   */
  stats(input) {
    const data = typeof input === 'string' ? Buffer.from(input) : input;
    const tokens = this.compress(input);
    const binary = this.serialize(tokens);
    const literals = tokens.filter(t => t.type === 'lit').length;
    const references = tokens.filter(t => t.type === 'ref').length;
    return {
      originalSize: data.length,
      compressedSize: binary.length,
      ratio: (data.length / binary.length).toFixed(2),
      tokens: tokens.length,
      literals,
      references,
    };
  }
}
