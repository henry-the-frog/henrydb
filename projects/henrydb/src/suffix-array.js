// suffix-array.js — Sorted array of all suffixes for pattern matching
// Used in bioinformatics, full-text search, data compression.
// O(n log²n) build, O(m log n) search where m = pattern length.

export class SuffixArray {
  constructor(text) {
    this._text = text;
    this._sa = this._build(text);
  }

  get length() { return this._sa.length; }

  /** Find all occurrences of pattern. O(m log n). */
  search(pattern) {
    const positions = [];
    let lo = 0, hi = this._sa.length - 1;
    
    // Find leftmost match
    let left = this._sa.length;
    let l = 0, r = this._sa.length - 1;
    while (l <= r) {
      const mid = (l + r) >>> 1;
      const suffix = this._text.substring(this._sa[mid], this._sa[mid] + pattern.length);
      if (suffix >= pattern) { left = mid; r = mid - 1; }
      else l = mid + 1;
    }

    // Find rightmost match
    let right = -1;
    l = 0; r = this._sa.length - 1;
    while (l <= r) {
      const mid = (l + r) >>> 1;
      const suffix = this._text.substring(this._sa[mid], this._sa[mid] + pattern.length);
      if (suffix <= pattern) { right = mid; l = mid + 1; }
      else r = mid - 1;
    }

    for (let i = left; i <= right; i++) positions.push(this._sa[i]);
    return positions.sort((a, b) => a - b);
  }

  /** Get the i-th suffix in sorted order. */
  getSuffix(i) { return this._text.substring(this._sa[i]); }

  /** Get longest common prefix between adjacent sorted suffixes. */
  getLCPArray() {
    const n = this._text.length;
    const rank = new Array(n);
    for (let i = 0; i < n; i++) rank[this._sa[i]] = i;
    
    const lcp = new Array(n).fill(0);
    let h = 0;
    for (let i = 0; i < n; i++) {
      if (rank[i] > 0) {
        const j = this._sa[rank[i] - 1];
        while (i + h < n && j + h < n && this._text[i + h] === this._text[j + h]) h++;
        lcp[rank[i]] = h;
        if (h > 0) h--;
      }
    }
    return lcp;
  }

  _build(text) {
    const n = text.length;
    const sa = Array.from({ length: n }, (_, i) => i);
    sa.sort((a, b) => {
      const sa_ = text.substring(a);
      const sb_ = text.substring(b);
      return sa_ < sb_ ? -1 : sa_ > sb_ ? 1 : 0;
    });
    return sa;
  }
}
