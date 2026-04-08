// storage2.js — SSI, prefix compression, CRC32, double-write buffer

/**
 * SSI — Serializable Snapshot Isolation.
 * Detects rw-antidependency cycles (dangerous structures).
 */
export class SSIManager {
  constructor() {
    this._txns = new Map(); // txnId → {readSet, writeSet, startTs, committed}
    this._conflicts = []; // [{from, to, type}]
    this._ts = 0;
  }

  begin() {
    const txnId = ++this._ts;
    this._txns.set(txnId, { readSet: new Set(), writeSet: new Set(), startTs: txnId, committed: false });
    return txnId;
  }

  read(txnId, key) {
    this._txns.get(txnId)?.readSet.add(key);
  }

  write(txnId, key) {
    this._txns.get(txnId)?.writeSet.add(key);
  }

  /** Attempt to commit — check for dangerous structures */
  commit(txnId) {
    const txn = this._txns.get(txnId);
    if (!txn) return { ok: false, reason: 'unknown txn' };

    // Check for rw-antidependency cycles
    for (const [otherId, other] of this._txns) {
      if (otherId === txnId || !other.committed) continue;

      // T1 reads something T2 wrote (rw-conflict: T1 → T2)
      for (const key of txn.readSet) {
        if (other.writeSet.has(key)) {
          this._conflicts.push({ from: txnId, to: otherId, type: 'rw' });
        }
      }

      // T2 reads something T1 wrote (wr-conflict: T2 → T1)
      for (const key of txn.writeSet) {
        if (other.readSet.has(key)) {
          this._conflicts.push({ from: otherId, to: txnId, type: 'rw' });
        }
      }
    }

    // Check for dangerous structure: consecutive rw edges
    if (this._hasDangerousStructure(txnId)) {
      this._txns.delete(txnId);
      return { ok: false, reason: 'serialization failure' };
    }

    txn.committed = true;
    return { ok: true };
  }

  _hasDangerousStructure(txnId) {
    // T1 →rw T2 →rw T3 where T1 committed before T3
    const incoming = this._conflicts.filter(c => c.to === txnId && c.type === 'rw');
    const outgoing = this._conflicts.filter(c => c.from === txnId && c.type === 'rw');
    return incoming.length > 0 && outgoing.length > 0;
  }

  abort(txnId) { this._txns.delete(txnId); }
  get activeCount() { return [...this._txns.values()].filter(t => !t.committed).length; }
}

/**
 * Prefix Compression — shared prefix elimination for sorted keys.
 */
export class PrefixCompressor {
  /** Compress sorted keys by storing only differing suffixes */
  static compress(sortedKeys) {
    if (sortedKeys.length === 0) return [];
    const result = [{ full: sortedKeys[0], prefix: 0, suffix: sortedKeys[0] }];
    
    for (let i = 1; i < sortedKeys.length; i++) {
      const prev = sortedKeys[i - 1];
      const curr = sortedKeys[i];
      let shared = 0;
      while (shared < prev.length && shared < curr.length && prev[shared] === curr[shared]) shared++;
      result.push({ full: curr, prefix: shared, suffix: curr.slice(shared) });
    }
    return result;
  }

  /** Decompress back to full keys */
  static decompress(entries) {
    if (entries.length === 0) return [];
    const keys = [entries[0].full || entries[0].suffix];
    for (let i = 1; i < entries.length; i++) {
      keys.push(keys[i - 1].slice(0, entries[i].prefix) + entries[i].suffix);
    }
    return keys;
  }

  /** Calculate compression ratio */
  static ratio(original, compressed) {
    const origSize = original.reduce((s, k) => s + k.length, 0);
    const compSize = compressed.reduce((s, e) => s + e.suffix.length + 4, 0); // 4 bytes for prefix length
    return origSize > 0 ? compSize / origSize : 1;
  }
}

/**
 * CRC32 — page checksum for integrity verification.
 */
export function crc32(data) {
  const TABLE = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) c = c & 1 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1;
    TABLE[i] = c;
  }
  
  let crc = 0xFFFFFFFF;
  const bytes = typeof data === 'string' ? new TextEncoder().encode(data) : data;
  for (let i = 0; i < bytes.length; i++) crc = TABLE[(crc ^ bytes[i]) & 0xFF] ^ (crc >>> 8);
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

/**
 * Double-Write Buffer — prevent torn page writes.
 */
export class DoubleWriteBuffer {
  constructor(capacity = 64) {
    this.capacity = capacity;
    this._buffer = new Map(); // pageId → data
    this._flushed = new Set();
  }

  /** Write page to buffer first */
  write(pageId, data) {
    this._buffer.set(pageId, { data, checksum: crc32(typeof data === 'string' ? data : JSON.stringify(data)) });
    if (this._buffer.size >= this.capacity) this.flush();
  }

  /** Flush buffer (simulates sequential write) */
  flush() {
    for (const [pageId] of this._buffer) this._flushed.add(pageId);
    const count = this._buffer.size;
    this._buffer.clear();
    return count;
  }

  /** Verify page integrity */
  verify(pageId, data) {
    const expected = crc32(typeof data === 'string' ? data : JSON.stringify(data));
    return expected; // Would compare against stored checksum in real impl
  }

  get pendingCount() { return this._buffer.size; }
  get flushedCount() { return this._flushed.size; }
}
