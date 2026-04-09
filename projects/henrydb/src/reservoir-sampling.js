// reservoir-sampling.js — Uniform random sample from a stream
// O(k) space, O(1) per element. No need to know stream size in advance.
// Algorithm R (Vitter, 1985).

export class ReservoirSampler {
  constructor(k) {
    this._k = k;
    this._reservoir = [];
    this._count = 0;
  }

  get size() { return this._reservoir.length; }

  /** Add element from stream. */
  add(element) {
    this._count++;
    if (this._reservoir.length < this._k) {
      this._reservoir.push(element);
    } else {
      const j = Math.floor(Math.random() * this._count);
      if (j < this._k) this._reservoir[j] = element;
    }
  }

  /** Get current sample. */
  getSample() { return [...this._reservoir]; }
}
