// bitwise-trie.js — Integer-keyed trie using bit partitioning
// Each level processes N bits of the key (like a radix tree for integers).
// O(W/B) lookup where W=word width, B=bits per level.
// Used in: Clojure/Scala persistent vectors, IP routing, HAMT.

const BITS = 5;             // Bits per level
const WIDTH = 1 << BITS;    // Children per node (32)
const MASK = WIDTH - 1;     // Bit mask

export class BitwiseTrie {
  constructor() {
    this._root = new Array(WIDTH).fill(null);
    this._size = 0;
  }

  get size() { return this._size; }

  set(key, value) {
    let node = this._root;
    let k = key >>> 0; // Ensure unsigned 32-bit
    
    // Walk down 5 bits at a time (6 levels for 32-bit keys)
    for (let shift = 30; shift > 0; shift -= BITS) {
      const idx = (k >>> shift) & MASK;
      if (!node[idx]) node[idx] = new Array(WIDTH).fill(null);
      node = node[idx];
    }
    
    const idx = k & MASK;
    if (node[idx] === null || node[idx] === undefined) this._size++;
    node[idx] = { value };
  }

  get(key) {
    let node = this._root;
    let k = key >>> 0;
    
    for (let shift = 30; shift > 0; shift -= BITS) {
      const idx = (k >>> shift) & MASK;
      if (!node[idx]) return undefined;
      node = node[idx];
    }
    
    const entry = node[k & MASK];
    return entry ? entry.value : undefined;
  }

  has(key) { return this.get(key) !== undefined; }

  delete(key) {
    let node = this._root;
    let k = key >>> 0;
    
    for (let shift = 30; shift > 0; shift -= BITS) {
      const idx = (k >>> shift) & MASK;
      if (!node[idx]) return false;
      node = node[idx];
    }
    
    const idx = k & MASK;
    if (node[idx]) {
      node[idx] = null;
      this._size--;
      return true;
    }
    return false;
  }
}
