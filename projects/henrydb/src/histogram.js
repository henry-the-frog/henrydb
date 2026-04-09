// histogram.js — Equi-width and equi-height histograms for statistics
export class EquiWidthHistogram {
  constructor(data, buckets = 10) {
    const min = Math.min(...data), max = Math.max(...data);
    const width = (max - min) / buckets;
    this._buckets = Array.from({ length: buckets }, (_, i) => ({
      lo: min + i * width,
      hi: min + (i + 1) * width,
      count: 0,
    }));
    for (const v of data) {
      const idx = Math.min(Math.floor((v - min) / width), buckets - 1);
      this._buckets[idx].count++;
    }
    this._total = data.length;
  }

  /** Estimate selectivity for value range [lo, hi]. */
  selectivity(lo, hi) {
    let count = 0;
    for (const b of this._buckets) {
      if (b.hi <= lo || b.lo >= hi) continue;
      const overlap = Math.min(b.hi, hi) - Math.max(b.lo, lo);
      const bucketWidth = b.hi - b.lo;
      count += b.count * (overlap / bucketWidth);
    }
    return count / this._total;
  }

  get buckets() { return this._buckets; }
}
