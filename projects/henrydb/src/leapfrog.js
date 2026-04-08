// leapfrog.js — Leapfrog Triejoin for worst-case optimal multi-way joins
// Joins multiple sorted relations by "leapfrogging" through them.

export class LeapfrogIterator {
  constructor(sortedArray) {
    this._data = sortedArray;
    this._pos = 0;
  }

  get current() { return this._pos < this._data.length ? this._data[this._pos] : null; }
  get atEnd() { return this._pos >= this._data.length; }

  /** Seek to first element >= target */
  seek(target) {
    // Binary search
    let lo = this._pos, hi = this._data.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      this._data[mid] < target ? lo = mid + 1 : hi = mid;
    }
    this._pos = lo;
  }

  next() { this._pos++; }
  reset() { this._pos = 0; }
}

/**
 * Leapfrog join — multi-way intersection of sorted iterators.
 */
export class LeapfrogJoin {
  constructor(iterators) {
    this.iters = iterators;
    this.k = iterators.length;
  }

  /** Find all values present in ALL iterators */
  *join() {
    if (this.k === 0) return;
    if (this.k === 1) {
      const it = this.iters[0];
      while (!it.atEnd) { yield it.current; it.next(); }
      return;
    }

    while (true) {
      // Find max current value
      let maxVal = -Infinity;
      let allEqual = true;
      let firstVal = null;
      
      for (const it of this.iters) {
        if (it.atEnd) return;
        if (firstVal === null) firstVal = it.current;
        else if (it.current !== firstVal) allEqual = false;
        if (it.current > maxVal) maxVal = it.current;
      }

      if (allEqual) {
        yield firstVal;
        for (const it of this.iters) it.next();
        continue;
      }

      // Seek all iterators to at least maxVal
      for (const it of this.iters) {
        if (it.current < maxVal) it.seek(maxVal);
        if (it.atEnd) return;
      }
    }
  }
}

/**
 * Database Cracking — adaptive indexing.
 * First query physically partitions the data; subsequent queries refine.
 */
export class CrackerColumn {
  constructor(data) {
    this._data = [...data];
    this._cracks = new Map(); // pivot → index position
  }

  /** Crack the column at a given value, returning all values < pivot */
  crack(pivot) {
    if (this._cracks.has(pivot)) {
      return this._data.slice(0, this._cracks.get(pivot));
    }

    // Partition: move values < pivot to left
    let lo = 0;
    let hi = this._data.length - 1;
    
    while (lo <= hi) {
      while (lo <= hi && this._data[lo] < pivot) lo++;
      while (lo <= hi && this._data[hi] >= pivot) hi--;
      if (lo < hi) {
        [this._data[lo], this._data[hi]] = [this._data[hi], this._data[lo]];
        lo++;
        hi--;
      }
    }

    this._cracks.set(pivot, lo);
    return this._data.slice(0, lo);
  }

  /** Range query using existing cracks */
  rangeQuery(lo, hi) {
    return this._data.filter(v => v >= lo && v <= hi);
  }

  get crackCount() { return this._cracks.size; }
  get length() { return this._data.length; }
}
