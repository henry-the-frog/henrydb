// sampling.js — Reservoir sampling + MinHash similarity

/** Reservoir Sampling: uniform random sample of size k from a stream */
export class ReservoirSampler {
  constructor(k) { this.k = k; this._reservoir = []; this._count = 0; }

  add(item) {
    this._count++;
    if (this._reservoir.length < this.k) { this._reservoir.push(item); return; }
    const j = Math.floor(Math.random() * this._count);
    if (j < this.k) this._reservoir[j] = item;
  }

  get sample() { return [...this._reservoir]; }
  get count() { return this._count; }
}

/** MinHash: estimate Jaccard similarity between sets */
export class MinHash {
  constructor(numHashes = 128) {
    this.numHashes = numHashes;
    this._seeds = Array.from({ length: numHashes }, (_, i) => i * 0x9e3779b9 + 0x12345);
  }

  /** Compute MinHash signature for a set of elements */
  signature(elements) {
    const sig = new Uint32Array(this.numHashes).fill(0xFFFFFFFF);
    for (const elem of elements) {
      for (let i = 0; i < this.numHashes; i++) {
        const h = this._hash(String(elem), this._seeds[i]);
        if (h < sig[i]) sig[i] = h;
      }
    }
    return sig;
  }

  /** Estimate Jaccard similarity from two signatures */
  similarity(sigA, sigB) {
    let matches = 0;
    for (let i = 0; i < this.numHashes; i++) {
      if (sigA[i] === sigB[i]) matches++;
    }
    return matches / this.numHashes;
  }

  _hash(str, seed) {
    let h = seed;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = Math.imul(h, 0x01000193);
    }
    return h >>> 0;
  }
}
