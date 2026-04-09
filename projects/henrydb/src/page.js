// page.js — Page-based storage engine for HenryDB
// Fixed-size 4KB pages with slotted page layout

export const PAGE_SIZE = 4096;
const HEADER_SIZE = 16; // pageId(4) + numSlots(2) + freeSpaceEnd(2) + flags(2) + reserved(6)

// ===== Page =====
// Layout: [header][slot directory (grows down)] ... [free space] ... [tuple data (grows up from end)]
// Each slot: offset(2) + length(2) = 4 bytes

export class Page {
  constructor(id = 0) {
    this.buffer = new ArrayBuffer(PAGE_SIZE);
    this.view = new DataView(this.buffer);
    this.data = new Uint8Array(this.buffer);
    this.id = id;
    this.setPageId(id);
    this.setNumSlots(0);
    this.setFreeSpaceEnd(PAGE_SIZE);
  }

  // Header accessors
  getPageId() { return this.view.getUint32(0); }
  setPageId(v) { this.view.setUint32(0, v); }
  getNumSlots() { return this.view.getUint16(4); }
  setNumSlots(v) { this.view.setUint16(4, v); }
  getFreeSpaceEnd() { return this.view.getUint16(6); }
  setFreeSpaceEnd(v) { this.view.setUint16(6, v); }

  // Slot directory
  _slotOffset(slotIdx) { return HEADER_SIZE + slotIdx * 4; }
  getSlot(slotIdx) {
    const off = this._slotOffset(slotIdx);
    return { offset: this.view.getUint16(off), length: this.view.getUint16(off + 2) };
  }
  setSlot(slotIdx, offset, length) {
    const off = this._slotOffset(slotIdx);
    this.view.setUint16(off, offset);
    this.view.setUint16(off + 2, length);
  }

  // Free space available
  freeSpace() {
    const slotDirEnd = HEADER_SIZE + this.getNumSlots() * 4;
    return this.getFreeSpaceEnd() - slotDirEnd - 4; // 4 for new slot entry
  }

  // Insert a tuple (returns slot index, or -1 if no space)
  insertTuple(tupleBytes) {
    if (tupleBytes.length > this.freeSpace()) return -1;

    const numSlots = this.getNumSlots();
    const freeEnd = this.getFreeSpaceEnd();
    const tupleStart = freeEnd - tupleBytes.length;

    // Write tuple data
    this.data.set(tupleBytes, tupleStart);

    // Write slot entry
    this.setSlot(numSlots, tupleStart, tupleBytes.length);
    this.setNumSlots(numSlots + 1);
    this.setFreeSpaceEnd(tupleStart);

    return numSlots;
  }

  // Read a tuple by slot index
  getTuple(slotIdx) {
    if (slotIdx >= this.getNumSlots()) return null;
    const slot = this.getSlot(slotIdx);
    if (slot.length === 0) return null; // deleted
    return new Uint8Array(this.buffer, slot.offset, slot.length);
  }

  // Delete a tuple (mark slot as empty — doesn't reclaim space yet)
  deleteTuple(slotIdx) {
    if (slotIdx >= this.getNumSlots()) return false;
    this.setSlot(slotIdx, 0, 0);
    return true;
  }

  // Update a tuple in place (delete old, insert new at same slot)
  updateTuple(slotIdx, newData) {
    if (slotIdx >= this.getNumSlots()) return false;
    // Delete old tuple
    this.setSlot(slotIdx, 0, 0);
    // Insert new data
    const freeEnd = this.getFreeSpaceEnd();
    const offset = freeEnd - newData.length;
    if (offset < 4 + this.getNumSlots() * 8 + 8) return false; // No space
    this.data.set(newData, offset);
    this.setSlot(slotIdx, offset, newData.length);
    this.setFreeSpaceEnd(offset);
    return true;
  }

  // Scan all live tuples
  *scanTuples() {
    const n = this.getNumSlots();
    for (let i = 0; i < n; i++) {
      const tuple = this.getTuple(i);
      if (tuple) yield { slotIdx: i, data: tuple };
    }
  }

  // Serialize to bytes
  toBytes() { return new Uint8Array(this.buffer); }

  // Deserialize from bytes
  static fromBytes(bytes) {
    const page = new Page();
    page.data.set(bytes);
    page.id = page.getPageId();
    return page;
  }
}

// ===== Tuple Encoding =====
// Simple row format: [numCols(2)][col1_type(1)][col1_len(2)][col1_data]...
// Types: 0=null, 1=int32, 2=float64, 3=string, 4=bool

const TYPE_NULL = 0;
const TYPE_INT = 1;
const TYPE_FLOAT = 2;
const TYPE_STRING = 3;
const TYPE_BOOL = 4;

export function encodeTuple(values) {
  const parts = [];
  const numCols = values.length;

  // Header: numCols
  parts.push(new Uint8Array([(numCols >> 8) & 0xFF, numCols & 0xFF]));

  for (const val of values) {
    if (val === null || val === undefined) {
      parts.push(new Uint8Array([TYPE_NULL, 0, 0]));
    } else if (typeof val === 'number' && Number.isInteger(val)) {
      const buf = new ArrayBuffer(4);
      new DataView(buf).setInt32(0, val);
      parts.push(new Uint8Array([TYPE_INT, 0, 4]));
      parts.push(new Uint8Array(buf));
    } else if (typeof val === 'number') {
      const buf = new ArrayBuffer(8);
      new DataView(buf).setFloat64(0, val);
      parts.push(new Uint8Array([TYPE_FLOAT, 0, 8]));
      parts.push(new Uint8Array(buf));
    } else if (typeof val === 'string') {
      const encoded = new TextEncoder().encode(val);
      parts.push(new Uint8Array([TYPE_STRING, (encoded.length >> 8) & 0xFF, encoded.length & 0xFF]));
      parts.push(encoded);
    } else if (typeof val === 'boolean') {
      parts.push(new Uint8Array([TYPE_BOOL, 0, 1, val ? 1 : 0]));
    }
  }

  // Concat
  const totalLen = parts.reduce((s, p) => s + p.length, 0);
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

export function decodeTuple(bytes) {
  const data = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const numCols = (data[0] << 8) | data[1];
  let offset = 2;
  const values = [];

  for (let i = 0; i < numCols; i++) {
    const type = data[offset];
    const len = (data[offset + 1] << 8) | data[offset + 2];
    offset += 3;

    switch (type) {
      case TYPE_NULL: values.push(null); break;
      case TYPE_INT: {
        const view = new DataView(data.buffer, data.byteOffset + offset, 4);
        values.push(view.getInt32(0));
        offset += 4;
        break;
      }
      case TYPE_FLOAT: {
        const view = new DataView(data.buffer, data.byteOffset + offset, 8);
        values.push(view.getFloat64(0));
        offset += 8;
        break;
      }
      case TYPE_STRING: {
        const str = new TextDecoder().decode(data.slice(offset, offset + len));
        values.push(str);
        offset += len;
        break;
      }
      case TYPE_BOOL: {
        values.push(data[offset] === 1);
        offset += 1;
        break;
      }
      default: values.push(null); offset += len;
    }
  }

  return values;
}

// ===== Buffer Pool =====
// LRU cache of pages with dirty tracking

export class BufferPool {
  constructor(maxPages = 64) {
    this.maxPages = maxPages;
    this.pages = new Map(); // pageId -> { page, dirty, pinCount }
    this.accessOrder = []; // LRU tracking
  }

  getPage(pageId) {
    const entry = this.pages.get(pageId);
    if (!entry) return null;
    // Move to front of LRU
    this.accessOrder = this.accessOrder.filter(id => id !== pageId);
    this.accessOrder.push(pageId);
    return entry.page;
  }

  putPage(page, dirty = false) {
    const pageId = page.id;
    if (this.pages.has(pageId)) {
      const entry = this.pages.get(pageId);
      entry.page = page;
      entry.dirty = entry.dirty || dirty;
      this.accessOrder = this.accessOrder.filter(id => id !== pageId);
      this.accessOrder.push(pageId);
      return null; // no eviction
    }

    // Evict if necessary
    let evicted = null;
    if (this.pages.size >= this.maxPages) {
      evicted = this._evict();
    }

    this.pages.set(pageId, { page, dirty, pinCount: 0 });
    this.accessOrder.push(pageId);
    return evicted;
  }

  markDirty(pageId) {
    const entry = this.pages.get(pageId);
    if (entry) entry.dirty = true;
  }

  pin(pageId) {
    const entry = this.pages.get(pageId);
    if (entry) entry.pinCount++;
  }

  unpin(pageId) {
    const entry = this.pages.get(pageId);
    if (entry && entry.pinCount > 0) entry.pinCount--;
  }

  _evict() {
    // Find least recently used unpinned page
    for (const pageId of this.accessOrder) {
      const entry = this.pages.get(pageId);
      if (entry.pinCount === 0) {
        this.pages.delete(pageId);
        this.accessOrder = this.accessOrder.filter(id => id !== pageId);
        return entry.dirty ? entry.page : null; // return dirty page for flushing
      }
    }
    throw new Error('All pages pinned, cannot evict');
  }

  getDirtyPages() {
    const dirty = [];
    for (const [id, entry] of this.pages) {
      if (entry.dirty) dirty.push(entry.page);
    }
    return dirty;
  }

  flushAll() {
    const dirty = this.getDirtyPages();
    for (const [id, entry] of this.pages) entry.dirty = false;
    return dirty;
  }

  get size() { return this.pages.size; }
}

// ===== Free Space Map =====
// Tracks approximate free space per page for efficient insert targeting
// Uses a simple array: freeBytes[pageId] = approximate free bytes

export class FreeSpaceMap {
  constructor() {
    this.entries = []; // pageId → free bytes (approximate)
  }

  // Update free space for a page
  update(pageId, freeBytes) {
    while (this.entries.length <= pageId) this.entries.push(0);
    this.entries[pageId] = freeBytes;
  }

  // Find a page with at least `needed` bytes free
  // Returns pageId or -1 if none found
  findPage(needed) {
    for (let i = 0; i < this.entries.length; i++) {
      if (this.entries[i] >= needed) return i;
    }
    return -1;
  }

  // Get free space for a page
  getFreeSpace(pageId) {
    return this.entries[pageId] || 0;
  }

  get pageCount() { return this.entries.length; }
}

// ===== Heap File =====
// Collection of pages representing a table

export class HeapFile {
  constructor(name) {
    this.name = name;
    this.pages = [];
    this.nextPageId = 0;
    this.fsm = new FreeSpaceMap();
    this._rowCount = 0;
  }

  get rowCount() { return this._rowCount; }
  get tupleCount() { return this._rowCount; }

  _allocPage() {
    const page = new Page(this.nextPageId++);
    this.pages.push(page);
    this.fsm.update(page.id, page.freeSpace());
    return page;
  }

  insert(values) {
    const tupleBytes = encodeTuple(values);
    
    // Try FSM first for targeted insert
    const targetPageId = this.fsm.findPage(tupleBytes.length + 4); // +4 for slot entry
    if (targetPageId >= 0) {
      const page = this.pages.find(p => p.id === targetPageId);
      if (page) {
        const slotIdx = page.insertTuple(tupleBytes);
        if (slotIdx >= 0) {
          this.fsm.update(page.id, page.freeSpace());
          this._rowCount++;
          return { pageId: page.id, slotIdx };
        }
      }
    }
    
    // Fall back to scanning pages
    for (const page of this.pages) {
      const slotIdx = page.insertTuple(tupleBytes);
      if (slotIdx >= 0) {
        this.fsm.update(page.id, page.freeSpace());
        return { pageId: page.id, slotIdx };
      }
    }
    // Allocate new page
    const page = this._allocPage();
    const slotIdx = page.insertTuple(tupleBytes);
    this.fsm.update(page.id, page.freeSpace());
    this._rowCount++;
    return { pageId: page.id, slotIdx };
  }

  get(pageId, slotIdx) {
    const page = this.pages.find(p => p.id === pageId);
    if (!page) return null;
    const tuple = page.getTuple(slotIdx);
    if (!tuple) return null;
    return decodeTuple(tuple);
  }

  delete(pageId, slotIdx) {
    const page = this.pages.find(p => p.id === pageId);
    if (!page) return false;
    const result = page.deleteTuple(slotIdx);
    if (result) this._rowCount--;
    return result;
  }

  *scan() {
    for (const page of this.pages) {
      for (const { slotIdx, data } of page.scanTuples()) {
        yield { pageId: page.id, slotIdx, values: decodeTuple(data) };
      }
    }
  }

  get tupleCount() {
    let count = 0;
    for (const page of this.pages) {
      for (const _ of page.scanTuples()) count++;
    }
    return count;
  }

  get pageCount() { return this.pages.length; }
}
