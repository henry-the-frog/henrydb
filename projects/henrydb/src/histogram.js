// histogram.js — Equi-depth histograms for selectivity estimation
// Based on PostgreSQL's approach: sort values, divide into equal-frequency buckets

/**
 * Build an equi-depth histogram from column values.
 * @param {any[]} values - Column values (may include nulls)
 * @param {number} numBuckets - Target number of buckets (default 10)
 * @returns {{ buckets: Array<{lo: any, hi: any, ndv: number, freq: number}>, mcv: Array<{value: any, freq: number}> }}
 */
export function buildHistogram(values, numBuckets = 10) {
  // Separate nulls and non-nulls
  const nonNull = values.filter(v => v !== null && v !== undefined);
  const nullCount = values.length - nonNull.length;
  
  if (nonNull.length === 0) {
    return { buckets: [], mcv: [], nullFraction: values.length > 0 ? nullCount / values.length : 0 };
  }
  
  // Most Common Values (MCV) list — top values by frequency
  // PostgreSQL tracks the top N most frequent values separately from the histogram
  const freqMap = new Map();
  for (const v of nonNull) {
    freqMap.set(v, (freqMap.get(v) || 0) + 1);
  }
  
  // Sort by frequency descending, take top 10
  const sorted = [...freqMap.entries()].sort((a, b) => b[1] - a[1]);
  const mcvLimit = Math.min(10, Math.floor(freqMap.size * 0.5)); // At most half of distinct values
  const mcv = sorted.slice(0, mcvLimit).map(([value, count]) => ({
    value,
    freq: count / values.length, // fraction of total (including nulls)
  }));
  
  // Remove MCV values from histogram input (PostgreSQL does this too)
  const mcvValues = new Set(mcv.map(m => m.value));
  const remaining = nonNull.filter(v => !mcvValues.has(v));
  
  if (remaining.length === 0) {
    return { 
      buckets: [], 
      mcv, 
      nullFraction: values.length > 0 ? nullCount / values.length : 0,
      totalRows: values.length,
    };
  }
  
  // Sort remaining values
  const sortedVals = [...remaining].sort((a, b) => {
    if (typeof a === 'number' && typeof b === 'number') return a - b;
    return String(a).localeCompare(String(b));
  });
  
  // Build equi-depth buckets
  const actualBuckets = Math.min(numBuckets, sortedVals.length);
  const bucketSize = Math.ceil(sortedVals.length / actualBuckets);
  const buckets = [];
  
  for (let i = 0; i < actualBuckets; i++) {
    const start = i * bucketSize;
    const end = Math.min(start + bucketSize, sortedVals.length);
    const slice = sortedVals.slice(start, end);
    
    // Count distinct values in this bucket
    const distinctInBucket = new Set(slice).size;
    
    buckets.push({
      lo: slice[0],
      hi: slice[slice.length - 1],
      ndv: distinctInBucket,
      freq: slice.length / values.length, // fraction of total
      count: slice.length,
    });
  }
  
  return {
    buckets,
    mcv,
    nullFraction: values.length > 0 ? nullCount / values.length : 0,
    totalRows: values.length,
  };
}

/**
 * Estimate selectivity for an equality predicate using histogram + MCV.
 * @param {object} histogram - From buildHistogram()
 * @param {any} value - The comparison value
 * @returns {number} Estimated fraction of rows matching (0-1)
 */
export function estimateEquality(histogram, value) {
  if (!histogram) return null;
  
  // Check MCV first — exact match
  const mcvMatch = histogram.mcv.find(m => m.value === value);
  if (mcvMatch) return mcvMatch.freq;
  
  // Not in MCV — estimate from histogram
  // Assume uniform distribution within each bucket
  for (const bucket of histogram.buckets) {
    if (value >= bucket.lo && value <= bucket.hi) {
      // Value falls in this bucket — estimate as 1/ndv of the bucket's frequency
      return bucket.ndv > 0 ? bucket.freq / bucket.ndv : 0;
    }
  }
  
  // Value not in any bucket — likely not in the table
  return 0;
}

/**
 * Estimate selectivity for a range predicate using histogram.
 * @param {object} histogram - From buildHistogram()
 * @param {string} op - 'GT', 'GE', 'LT', 'LE'
 * @param {any} value - The comparison value
 * @returns {number} Estimated fraction of rows matching (0-1)
 */
export function estimateRange(histogram, op, value) {
  if (!histogram || histogram.buckets.length === 0) return null;
  
  let fraction = 0;
  
  // Add MCV contributions
  for (const m of histogram.mcv) {
    const v = m.value;
    if ((op === 'GT' && v > value) ||
        (op === 'GE' && v >= value) ||
        (op === 'LT' && v < value) ||
        (op === 'LE' && v <= value)) {
      fraction += m.freq;
    }
  }
  
  // Add bucket contributions
  for (const bucket of histogram.buckets) {
    if ((op === 'GT' || op === 'GE') && bucket.lo > value) {
      // Entire bucket is above threshold
      fraction += bucket.freq;
    } else if ((op === 'LT' || op === 'LE') && bucket.hi < value) {
      // Entire bucket is below threshold
      fraction += bucket.freq;
    } else if (value >= bucket.lo && value <= bucket.hi) {
      // Partial bucket — linear interpolation
      if (typeof bucket.lo === 'number' && typeof bucket.hi === 'number' && bucket.hi > bucket.lo) {
        const range = bucket.hi - bucket.lo;
        if (op === 'GT' || op === 'GE') {
          fraction += bucket.freq * ((bucket.hi - value) / range);
        } else {
          fraction += bucket.freq * ((value - bucket.lo) / range);
        }
      } else {
        // Non-numeric or single-value bucket — estimate 50%
        fraction += bucket.freq * 0.5;
      }
    }
  }
  
  return Math.max(0, Math.min(1, fraction));
}

/**
 * Estimate selectivity for BETWEEN predicate using histogram.
 */
export function estimateBetween(histogram, lo, hi) {
  if (!histogram) return null;
  const geFraction = estimateRange(histogram, 'GE', lo);
  const leFraction = estimateRange(histogram, 'LE', hi);
  // P(lo <= x <= hi) = P(x >= lo) + P(x <= hi) - 1
  // But that can go negative, so clamp
  if (geFraction === null || leFraction === null) return null;
  return Math.max(0, Math.min(1, geFraction + leFraction - 1));
}

/**
 * Class wrapper for equi-width histogram (for backward compatibility with tests).
 */
export class EquiWidthHistogram {
  constructor(values, numBuckets = 10) {
    // For small datasets, skip MCV to preserve bucket count
    const skipMcv = values.length <= numBuckets * 3;
    
    if (skipMcv) {
      // Build simple equi-width histogram without MCV extraction
      const nonNull = values.filter(v => v !== null && v !== undefined);
      const sorted = [...nonNull].sort((a, b) => {
        if (typeof a === 'number' && typeof b === 'number') return a - b;
        return String(a).localeCompare(String(b));
      });
      const actualBuckets = Math.min(numBuckets, sorted.length);
      const bucketSize = Math.ceil(sorted.length / actualBuckets);
      this.buckets = [];
      for (let i = 0; i < actualBuckets; i++) {
        const start = i * bucketSize;
        const end = Math.min(start + bucketSize, sorted.length);
        const slice = sorted.slice(start, end);
        this.buckets.push({
          lo: slice[0], hi: slice[slice.length - 1],
          ndv: new Set(slice).size,
          freq: slice.length / values.length,
          count: slice.length
        });
      }
      this.mcv = [];
      this.nullFraction = (values.length - nonNull.length) / (values.length || 1);
      this.totalRows = values.length;
    } else {
      const hist = buildHistogram(values, numBuckets);
      this.buckets = hist.buckets;
      this.mcv = hist.mcv;
      this.nullFraction = hist.nullFraction;
      this.totalRows = hist.totalRows || values.length;
    }
  }

  estimateSelectivity(value) {
    return estimateEquality(this, value);
  }

  selectivity(lo, hi) {
    return estimateBetween(this, lo, hi);
  }

  estimateRange(op, value) {
    return estimateRange(this, op, value);
  }
}
