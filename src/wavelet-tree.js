// wavelet-tree.js — Wavelet Tree for rank/select/access on sequences
// Supports O(log σ) rank, select, and access queries.

export class WaveletTree {
  constructor(sequence, alphabet = null) {
    this.n = sequence.length;
    if (!alphabet) {
      alphabet = [...new Set(sequence)].sort();
    }
    this.alphabet = alphabet;
    this.root = this._build(sequence, alphabet);
  }

  _build(seq, alpha) {
    if (alpha.length <= 1) return { char: alpha[0], count: seq.length };
    
    const mid = Math.floor(alpha.length / 2);
    const leftAlpha = alpha.slice(0, mid);
    const rightAlpha = alpha.slice(mid);
    const leftSet = new Set(leftAlpha);
    
    // Bitmap: 0 for left alphabet, 1 for right
    const bitmap = new Uint8Array(seq.length);
    const leftSeq = [], rightSeq = [];
    
    for (let i = 0; i < seq.length; i++) {
      if (leftSet.has(seq[i])) {
        bitmap[i] = 0;
        leftSeq.push(seq[i]);
      } else {
        bitmap[i] = 1;
        rightSeq.push(seq[i]);
      }
    }

    // Precompute rank0/rank1 prefix sums
    const rank0 = new Uint32Array(seq.length + 1);
    const rank1 = new Uint32Array(seq.length + 1);
    for (let i = 0; i < seq.length; i++) {
      rank0[i + 1] = rank0[i] + (bitmap[i] === 0 ? 1 : 0);
      rank1[i + 1] = rank1[i] + (bitmap[i] === 1 ? 1 : 0);
    }

    return {
      bitmap,
      rank0,
      rank1,
      left: leftSeq.length > 0 ? this._build(leftSeq, leftAlpha) : null,
      right: rightSeq.length > 0 ? this._build(rightSeq, rightAlpha) : null,
      leftAlpha,
      rightAlpha,
    };
  }

  /** Access: return character at position i */
  access(i) {
    return this._access(this.root, i);
  }

  _access(node, i) {
    if (node.char !== undefined) return node.char;
    if (node.bitmap[i] === 0) {
      return this._access(node.left, node.rank0[i + 1] - 1);
    } else {
      return this._access(node.right, node.rank1[i + 1] - 1);
    }
  }

  /** Rank: count occurrences of c in seq[0..i) */
  rank(c, i) {
    return this._rank(this.root, c, i);
  }

  _rank(node, c, i) {
    if (i <= 0) return 0;
    if (node.char !== undefined) return node.char === c ? i : 0;
    
    const leftSet = new Set(node.leftAlpha);
    if (leftSet.has(c)) {
      return this._rank(node.left, c, node.rank0[i]);
    } else {
      return this._rank(node.right, c, node.rank1[i]);
    }
  }

  /** Select: find position of k-th occurrence of c (1-indexed) */
  select(c, k) {
    if (k <= 0) return -1;
    return this._select(this.root, c, k);
  }

  _select(node, c, k) {
    if (node.char !== undefined) return node.char === c && k <= node.count ? k - 1 : -1;
    
    const leftSet = new Set(node.leftAlpha);
    if (leftSet.has(c)) {
      const pos = this._select(node.left, c, k);
      if (pos === -1) return -1;
      // Map back: find (pos+1)-th 0 in bitmap
      return this._selectBit(node.rank0, 0, pos + 1);
    } else {
      const pos = this._select(node.right, c, k);
      if (pos === -1) return -1;
      return this._selectBit(node.rank1, 1, pos + 1);
    }
  }

  _selectBit(rankArr, bit, k) {
    // Binary search for position where rank[pos] = k
    let lo = 0, hi = rankArr.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (rankArr[mid] < k) lo = mid + 1;
      else hi = mid;
    }
    return lo - 1;
  }

  /** Count distinct characters in range */
  countDistinct(lo, hi) {
    const seen = new Set();
    for (let i = lo; i <= hi && i < this.n; i++) seen.add(this.access(i));
    return seen.size;
  }
}
