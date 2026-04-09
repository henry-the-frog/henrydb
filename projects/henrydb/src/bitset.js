// bitset.js — Efficient bit manipulation for set operations
// Uses Uint32Array for compact storage. 32x more space-efficient than boolean arrays.

export class BitSet {
  constructor(size = 256) {
    this._words = new Uint32Array(Math.ceil(size / 32));
    this._size = size;
  }

  set(bit) { this._words[bit >>> 5] |= 1 << (bit & 31); }
  clear(bit) { this._words[bit >>> 5] &= ~(1 << (bit & 31)); }
  get(bit) { return (this._words[bit >>> 5] >>> (bit & 31)) & 1; }
  toggle(bit) { this._words[bit >>> 5] ^= 1 << (bit & 31); }

  and(other) {
    const result = new BitSet(this._size);
    for (let i = 0; i < this._words.length; i++) result._words[i] = this._words[i] & other._words[i];
    return result;
  }

  or(other) {
    const result = new BitSet(this._size);
    for (let i = 0; i < this._words.length; i++) result._words[i] = this._words[i] | other._words[i];
    return result;
  }

  xor(other) {
    const result = new BitSet(this._size);
    for (let i = 0; i < this._words.length; i++) result._words[i] = this._words[i] ^ other._words[i];
    return result;
  }

  not() {
    const result = new BitSet(this._size);
    for (let i = 0; i < this._words.length; i++) result._words[i] = ~this._words[i];
    return result;
  }

  popcount() {
    let count = 0;
    for (const word of this._words) {
      let v = word;
      v = v - ((v >> 1) & 0x55555555);
      v = (v & 0x33333333) + ((v >> 2) & 0x33333333);
      count += ((v + (v >> 4) & 0xF0F0F0F) * 0x1010101) >> 24;
    }
    return count;
  }

  toArray() {
    const bits = [];
    for (let i = 0; i < this._size; i++) if (this.get(i)) bits.push(i);
    return bits;
  }

  get bytesUsed() { return this._words.byteLength; }
}
