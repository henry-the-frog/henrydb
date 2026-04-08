// histogram.js — Equi-depth histogram + Most Common Values for selectivity estimation

export class Histogram {
  constructor(values, numBuckets = 10) {
    this.numBuckets = numBuckets;
    this.totalRows = values.length;
    this.nullCount = 0;
    
    const nonNull = values.filter(v => v != null);
    this.nullCount = values.length - nonNull.length;
    
    if (nonNull.length === 0) {
      this.buckets = [];
      this.mcv = [];
      this.ndv = 0;
      return;
    }

    const sorted = [...nonNull].sort((a, b) => a - b);
    this.min = sorted[0];
    this.max = sorted[sorted.length - 1];
    this.ndv = new Set(sorted).size;

    // Most Common Values (top 10)
    const freq = new Map();
    for (const v of sorted) freq.set(v, (freq.get(v) || 0) + 1);
    this.mcv = [...freq.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([value, count]) => ({ value, count, frequency: count / this.totalRows }));

    // Equi-depth buckets
    const bucketSize = Math.ceil(sorted.length / numBuckets);
    this.buckets = [];
    for (let i = 0; i < sorted.length; i += bucketSize) {
      const slice = sorted.slice(i, i + bucketSize);
      this.buckets.push({
        lo: slice[0],
        hi: slice[slice.length - 1],
        count: slice.length,
        ndv: new Set(slice).size,
      });
    }
  }

  /** Estimate selectivity of equality predicate */
  estimateEQ(value) {
    const mcvEntry = this.mcv.find(m => m.value === value);
    if (mcvEntry) return mcvEntry.frequency;
    // Uniform assumption for non-MCV values
    return this.ndv > 0 ? 1 / this.ndv : 0;
  }

  /** Estimate selectivity of range predicate */
  estimateRange(lo, hi) {
    let total = 0;
    for (const b of this.buckets) {
      if (b.hi < lo || b.lo > hi) continue; // No overlap
      if (b.lo >= lo && b.hi <= hi) { total += b.count; continue; } // Fully contained
      // Partial overlap — linear interpolation
      const range = b.hi - b.lo || 1;
      const overlapLo = Math.max(lo, b.lo);
      const overlapHi = Math.min(hi, b.hi);
      const fraction = (overlapHi - overlapLo) / range;
      total += b.count * Math.max(0, Math.min(1, fraction));
    }
    return this.totalRows > 0 ? total / this.totalRows : 0;
  }

  /** Estimate selectivity of > predicate */
  estimateGT(value) { return this.estimateRange(value + 1, this.max); }
  
  /** Estimate selectivity of < predicate */
  estimateLT(value) { return this.estimateRange(this.min, value - 1); }

  /** Null fraction */
  get nullFraction() { return this.totalRows > 0 ? this.nullCount / this.totalRows : 0; }
}
