// fm-index.js — FM-Index for substring search via Burrows-Wheeler Transform
// Supports O(m) substring search where m is pattern length.

export class FMIndex {
  constructor(text) {
    this.text = text + '$'; // Sentinel
    this.n = this.text.length;
    
    // Build suffix array
    const suffixes = Array.from({ length: this.n }, (_, i) => i);
    suffixes.sort((a, b) => {
      for (let k = 0; k < this.n; k++) {
        const ca = this.text[(a + k) % this.n];
        const cb = this.text[(b + k) % this.n];
        if (ca < cb) return -1;
        if (ca > cb) return 1;
      }
      return 0;
    });
    this.sa = suffixes;
    
    // BWT = last column of sorted rotations
    this.bwt = suffixes.map(i => this.text[(i + this.n - 1) % this.n]);
    
    // Build occurrence table (C array and Occ table)
    this.alphabet = [...new Set(this.bwt)].sort();
    
    // C[c] = number of characters in BWT that are lexicographically smaller than c
    this.C = {};
    const counts = {};
    for (const c of this.bwt) counts[c] = (counts[c] || 0) + 1;
    let cumulative = 0;
    for (const c of this.alphabet) {
      this.C[c] = cumulative;
      cumulative += counts[c] || 0;
    }
    
    // Occ[c][i] = number of occurrences of c in bwt[0..i)
    this.Occ = {};
    for (const c of this.alphabet) {
      this.Occ[c] = new Uint32Array(this.n + 1);
      for (let i = 0; i < this.n; i++) {
        this.Occ[c][i + 1] = this.Occ[c][i] + (this.bwt[i] === c ? 1 : 0);
      }
    }
  }

  /** Count occurrences of pattern in text */
  count(pattern) {
    let lo = 0, hi = this.n;
    
    for (let i = pattern.length - 1; i >= 0; i--) {
      const c = pattern[i];
      if (this.C[c] === undefined) return 0;
      lo = this.C[c] + this.Occ[c][lo];
      hi = this.C[c] + this.Occ[c][hi];
      if (lo >= hi) return 0;
    }
    
    return hi - lo;
  }

  /** Locate all occurrences (positions in original text) */
  locate(pattern) {
    let lo = 0, hi = this.n;
    
    for (let i = pattern.length - 1; i >= 0; i--) {
      const c = pattern[i];
      if (this.C[c] === undefined) return [];
      lo = this.C[c] + this.Occ[c][lo];
      hi = this.C[c] + this.Occ[c][hi];
      if (lo >= hi) return [];
    }
    
    return Array.from({ length: hi - lo }, (_, i) => this.sa[lo + i]);
  }
}

/**
 * Sparse Index — one entry per data block for efficient range scans.
 */
export class SparseIndex {
  constructor(blockSize = 100) {
    this.blockSize = blockSize;
    this._entries = []; // [{minKey, maxKey, offset, count}]
  }

  /** Build from sorted data */
  static build(sortedData, keyFn, blockSize = 100) {
    const idx = new SparseIndex(blockSize);
    for (let i = 0; i < sortedData.length; i += blockSize) {
      const block = sortedData.slice(i, i + blockSize);
      idx._entries.push({
        minKey: keyFn(block[0]),
        maxKey: keyFn(block[block.length - 1]),
        offset: i,
        count: block.length,
      });
    }
    return idx;
  }

  /** Find blocks that might contain key */
  lookup(key) {
    return this._entries.filter(e => key >= e.minKey && key <= e.maxKey);
  }

  /** Find blocks that overlap with range [lo, hi] */
  rangeBlocks(lo, hi) {
    return this._entries.filter(e => e.maxKey >= lo && e.minKey <= hi);
  }

  get blockCount() { return this._entries.length; }
}
