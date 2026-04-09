// exponential-histogram.js — Sliding window approximate counting
// DGIM algorithm: maintain count of 1-bits in last W positions of a binary stream.
// O(log²W) space for (1±ε) approximation.

export class ExponentialHistogram {
  constructor(windowSize, epsilon = 0.5) {
    this._W = windowSize;
    this._epsilon = epsilon;
    this._k = Math.ceil(1 / epsilon);
    this._buckets = []; // {size, timestamp}
    this._time = 0;
    this._total = 0;
  }

  /** Add a bit (0 or 1) to the stream. */
  add(bit) {
    this._time++;
    this._expire();
    
    if (bit) {
      this._buckets.unshift({ size: 1, timestamp: this._time });
      this._merge();
      this._total++;
    }
  }

  /** Estimate count of 1s in last W elements. */
  estimate() {
    this._expire();
    if (this._buckets.length === 0) return 0;
    
    let sum = 0;
    for (let i = 0; i < this._buckets.length - 1; i++) {
      sum += this._buckets[i].size;
    }
    // Last bucket: count half (uncertainty)
    sum += Math.ceil(this._buckets[this._buckets.length - 1].size / 2);
    return sum;
  }

  _expire() {
    while (this._buckets.length > 0 && this._buckets[this._buckets.length - 1].timestamp <= this._time - this._W) {
      this._buckets.pop();
    }
  }

  _merge() {
    // Merge buckets of same size when count exceeds k+1
    let i = 0;
    while (i < this._buckets.length - 1) {
      let count = 0;
      let j = i;
      while (j < this._buckets.length && this._buckets[j].size === this._buckets[i].size) {
        count++;
        j++;
      }
      if (count > this._k + 1) {
        // Merge last two of this size
        const mergeIdx = j - 2;
        this._buckets[mergeIdx].size *= 2;
        this._buckets.splice(mergeIdx + 1, 1);
        // Don't advance i, check again
      } else {
        i = j;
      }
    }
  }

  getStats() {
    return {
      buckets: this._buckets.length,
      time: this._time,
      windowSize: this._W,
      epsilon: this._epsilon,
    };
  }
}
