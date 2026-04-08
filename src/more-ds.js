// more-ds.js — More data structures and algorithms

/** Topological Sort — Kahn's algorithm for DAG ordering */
export function topologicalSort(graph) {
  // graph: Map<node, Set<dependency>> (edges point to dependencies)
  const inDegree = new Map();
  const adj = new Map();
  
  for (const [node, deps] of graph) {
    if (!inDegree.has(node)) inDegree.set(node, 0);
    if (!adj.has(node)) adj.set(node, []);
    for (const dep of deps) {
      if (!adj.has(dep)) adj.set(dep, []);
      adj.get(dep).push(node);
      inDegree.set(node, (inDegree.get(node) || 0) + 1);
      if (!inDegree.has(dep)) inDegree.set(dep, 0);
    }
  }

  const queue = [];
  for (const [node, deg] of inDegree) { if (deg === 0) queue.push(node); }

  const order = [];
  while (queue.length > 0) {
    const node = queue.shift();
    order.push(node);
    for (const next of (adj.get(node) || [])) {
      inDegree.set(next, inDegree.get(next) - 1);
      if (inDegree.get(next) === 0) queue.push(next);
    }
  }

  return order.length === inDegree.size ? order : null; // null = cycle
}

/** LFU Cache — evict least frequently used */
export class LFUCache {
  constructor(capacity) {
    this.capacity = capacity;
    this._cache = new Map(); // key → { value, freq }
    this._freqMap = new Map(); // freq → Set<key>
    this._minFreq = 0;
  }
  get(key) {
    if (!this._cache.has(key)) return undefined;
    const entry = this._cache.get(key);
    this._incrementFreq(key, entry);
    return entry.value;
  }
  set(key, value) {
    if (this.capacity === 0) return;
    if (this._cache.has(key)) {
      const entry = this._cache.get(key);
      entry.value = value;
      this._incrementFreq(key, entry);
      return;
    }
    if (this._cache.size >= this.capacity) this._evict();
    this._cache.set(key, { value, freq: 1 });
    if (!this._freqMap.has(1)) this._freqMap.set(1, new Set());
    this._freqMap.get(1).add(key);
    this._minFreq = 1;
  }
  _incrementFreq(key, entry) {
    const oldFreq = entry.freq;
    this._freqMap.get(oldFreq)?.delete(key);
    if (this._freqMap.get(oldFreq)?.size === 0) {
      this._freqMap.delete(oldFreq);
      if (this._minFreq === oldFreq) this._minFreq++;
    }
    entry.freq++;
    if (!this._freqMap.has(entry.freq)) this._freqMap.set(entry.freq, new Set());
    this._freqMap.get(entry.freq).add(key);
  }
  _evict() {
    const keys = this._freqMap.get(this._minFreq);
    if (!keys || keys.size === 0) return;
    const victim = keys.values().next().value;
    keys.delete(victim);
    if (keys.size === 0) this._freqMap.delete(this._minFreq);
    this._cache.delete(victim);
  }
  get size() { return this._cache.size; }
}

/** KMP String Matching — O(n+m) pattern search */
export function kmpSearch(text, pattern) {
  if (pattern.length === 0) return [];
  const lps = computeLPS(pattern);
  const positions = [];
  let i = 0, j = 0;
  while (i < text.length) {
    if (text[i] === pattern[j]) { i++; j++; }
    if (j === pattern.length) { positions.push(i - j); j = lps[j - 1]; }
    else if (i < text.length && text[i] !== pattern[j]) {
      if (j > 0) j = lps[j - 1];
      else i++;
    }
  }
  return positions;
}

function computeLPS(pattern) {
  const lps = new Array(pattern.length).fill(0);
  let len = 0, i = 1;
  while (i < pattern.length) {
    if (pattern[i] === pattern[len]) { len++; lps[i] = len; i++; }
    else { if (len > 0) len = lps[len - 1]; else { lps[i] = 0; i++; } }
  }
  return lps;
}

/** Murmur3 Hash — fast non-cryptographic 32-bit hash */
export function murmur3(key, seed = 0) {
  const data = typeof key === 'string' ? Buffer.from(key) : key;
  let h = seed;
  const nBlocks = Math.floor(data.length / 4);
  
  for (let i = 0; i < nBlocks; i++) {
    let k = data.readUInt32LE(i * 4);
    k = Math.imul(k, 0xcc9e2d51);
    k = (k << 15) | (k >>> 17);
    k = Math.imul(k, 0x1b873593);
    h ^= k;
    h = (h << 13) | (h >>> 19);
    h = Math.imul(h, 5) + 0xe6546b64;
  }

  let k = 0;
  const tail = nBlocks * 4;
  switch (data.length & 3) {
    case 3: k ^= data[tail + 2] << 16;
    case 2: k ^= data[tail + 1] << 8;
    case 1: k ^= data[tail];
      k = Math.imul(k, 0xcc9e2d51);
      k = (k << 15) | (k >>> 17);
      k = Math.imul(k, 0x1b873593);
      h ^= k;
  }

  h ^= data.length;
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}
