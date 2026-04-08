// hashing2.js — Jump consistent hashing, Maglev hashing, Rendezvous hashing

/**
 * Jump Consistent Hashing — O(1) memory, O(ln n) time.
 * Maps keys to buckets with minimal disruption when bucket count changes.
 */
export function jumpHash(key, numBuckets) {
  let b = -1, j = 0;
  let k = BigInt(typeof key === 'number' ? key : fnv1a(String(key)));
  while (j < numBuckets) {
    b = j;
    k = (k * 2862933555777941757n + 1n) & 0xFFFFFFFFFFFFFFFFn;
    j = Math.floor((Number(b) + 1) * (Number(1n << 31n) / Number((k >> 33n) + 1n)));
  }
  return Number(b);
}

function fnv1a(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return h >>> 0;
}

/**
 * Rendezvous Hashing (HRW) — highest random weight.
 * Each key hashes with each node; node with highest hash wins.
 */
export class RendezvousHashing {
  constructor(nodes = []) {
    this.nodes = [...nodes];
  }

  addNode(node) { this.nodes.push(node); }
  removeNode(node) { this.nodes = this.nodes.filter(n => n !== node); }

  getNode(key) {
    if (this.nodes.length === 0) return null;
    let best = null, bestHash = -1;
    for (const node of this.nodes) {
      const h = fnv1a(`${key}:${node}`);
      if (h > bestHash) { bestHash = h; best = node; }
    }
    return best;
  }

  /** Get top-k nodes for replication */
  getNodes(key, k = 1) {
    return this.nodes
      .map(node => ({ node, hash: fnv1a(`${key}:${node}`) }))
      .sort((a, b) => b.hash - a.hash)
      .slice(0, k)
      .map(x => x.node);
  }
}

/**
 * Maglev Hashing — consistent load balancing.
 * Builds a lookup table for O(1) key-to-node mapping.
 */
export class MaglevHashing {
  constructor(nodes, tableSize = 65537) {
    this.nodes = nodes;
    this.tableSize = tableSize;
    this.table = this._buildTable();
  }

  _buildTable() {
    const n = this.nodes.length;
    if (n === 0) return [];
    
    const table = new Array(this.tableSize).fill(-1);
    const next = new Array(n).fill(0);
    const offsets = this.nodes.map(node => fnv1a(node + '_offset') % this.tableSize);
    const skips = this.nodes.map(node => (fnv1a(node + '_skip') % (this.tableSize - 1)) + 1);
    
    let filled = 0;
    while (filled < this.tableSize) {
      for (let i = 0; i < n; i++) {
        let c = (offsets[i] + next[i] * skips[i]) % this.tableSize;
        while (table[c] !== -1) { next[i]++; c = (offsets[i] + next[i] * skips[i]) % this.tableSize; }
        table[c] = i;
        next[i]++;
        filled++;
        if (filled >= this.tableSize) break;
      }
    }
    return table;
  }

  lookup(key) {
    if (this.nodes.length === 0) return null;
    const h = fnv1a(String(key)) % this.tableSize;
    return this.nodes[this.table[h]];
  }
}
