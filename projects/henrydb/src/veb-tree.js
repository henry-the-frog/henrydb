// veb-tree.js — van Emde Boas tree: O(log log u) predecessor/successor queries
// Universe size u must be a power of 2.

export class VEBTree {
  constructor(u) {
    this.u = u;
    this.min = null;
    this.max = null;
    
    if (u <= 2) {
      this.summary = null;
      this.clusters = null;
    } else {
      const sqrtU = Math.ceil(Math.sqrt(u));
      this.sqrtU = sqrtU;
      this.clusters = new Array(sqrtU).fill(null);
      this.summary = null;
    }
  }

  _high(x) { return Math.floor(x / this.sqrtU); }
  _low(x) { return x % this.sqrtU; }
  _index(h, l) { return h * this.sqrtU + l; }

  insert(x) {
    if (this.min === null) { this.min = this.max = x; return; }
    if (x < this.min) { const tmp = x; x = this.min; this.min = tmp; }
    if (x > this.max) this.max = x;
    
    if (this.u <= 2) return;
    
    const h = this._high(x);
    const l = this._low(x);
    
    if (!this.clusters[h]) this.clusters[h] = new VEBTree(this.sqrtU);
    
    if (this.clusters[h].min === null) {
      if (!this.summary) this.summary = new VEBTree(this.sqrtU);
      this.summary.insert(h);
    }
    this.clusters[h].insert(l);
  }

  has(x) {
    if (x === this.min || x === this.max) return true;
    if (this.u <= 2) return false;
    const h = this._high(x);
    if (!this.clusters[h]) return false;
    return this.clusters[h].has(this._low(x));
  }

  successor(x) {
    if (this.u <= 2) {
      if (x === 0 && this.max === 1) return 1;
      return null;
    }
    if (this.min !== null && x < this.min) return this.min;
    
    const h = this._high(x);
    const l = this._low(x);
    
    // Check current cluster
    if (this.clusters[h] && this.clusters[h].max !== null && l < this.clusters[h].max) {
      const offset = this.clusters[h].successor(l);
      if (offset !== null) return this._index(h, offset);
    }
    
    // Check summary for next non-empty cluster
    if (this.summary) {
      const nextH = this.summary.successor(h);
      if (nextH !== null && this.clusters[nextH]) {
        return this._index(nextH, this.clusters[nextH].min);
      }
    }
    return null;
  }

  predecessor(x) {
    if (this.u <= 2) {
      if (x === 1 && this.min === 0) return 0;
      return null;
    }
    if (this.max !== null && x > this.max) return this.max;
    
    const h = this._high(x);
    const l = this._low(x);
    
    if (this.clusters[h] && this.clusters[h].min !== null && l > this.clusters[h].min) {
      const offset = this.clusters[h].predecessor(l);
      if (offset !== null) return this._index(h, offset);
    }
    
    if (this.summary) {
      const prevH = this.summary.predecessor(h);
      if (prevH !== null && this.clusters[prevH]) {
        return this._index(prevH, this.clusters[prevH].max);
      }
    }
    
    if (this.min !== null && x > this.min) return this.min;
    return null;
  }
}
