// slotted-page.js — Slotted page for variable-length records
// Layout: [header | slot directory | ... free space ... | records]
// Header: pageId, slotCount, freeSpaceStart, freeSpaceEnd
// Slot directory: grows forward from header
// Records: grow backward from end of page
// Supports: insert, read, delete, update, compaction

export class SlottedPage {
  constructor(pageId = 0, pageSize = 4096) {
    this.pageId = pageId;
    this.pageSize = pageSize;
    this._data = Buffer.alloc(pageSize);
    this._slots = []; // [{offset, length, deleted}]
    this._freeEnd = pageSize; // Records grow backward from here
  }

  /**
   * Insert a record. Returns slot index or -1 if no space.
   */
  insert(record) {
    const buf = Buffer.isBuffer(record) ? record : Buffer.from(JSON.stringify(record));
    const needed = buf.length;

    // Check available space (slot entry overhead + record)
    const slotOverhead = 8; // offset(4) + length(4) per slot
    const headerSize = 16;
    const usedBySlots = headerSize + (this._slots.length + 1) * slotOverhead;
    
    if (usedBySlots + needed > this._freeEnd) {
      // Try compaction first
      this._compact();
      if (usedBySlots + needed > this._freeEnd) return -1; // Still no space
    }

    // Allocate from the end
    this._freeEnd -= needed;
    buf.copy(this._data, this._freeEnd);

    const slotIdx = this._slots.length;
    this._slots.push({ offset: this._freeEnd, length: needed, deleted: false });
    return slotIdx;
  }

  /**
   * Read a record by slot index.
   */
  read(slotIdx) {
    if (slotIdx < 0 || slotIdx >= this._slots.length) return null;
    const slot = this._slots[slotIdx];
    if (slot.deleted) return null;
    
    const buf = this._data.subarray(slot.offset, slot.offset + slot.length);
    try {
      return JSON.parse(buf.toString());
    } catch {
      return buf;
    }
  }

  /**
   * Delete a record (marks slot as deleted).
   */
  delete(slotIdx) {
    if (slotIdx < 0 || slotIdx >= this._slots.length) return false;
    if (this._slots[slotIdx].deleted) return false;
    this._slots[slotIdx].deleted = true;
    return true;
  }

  /**
   * Update a record. If new record fits in old slot, in-place. Otherwise, delete+insert.
   */
  update(slotIdx, record) {
    if (slotIdx < 0 || slotIdx >= this._slots.length) return -1;
    
    const buf = Buffer.isBuffer(record) ? record : Buffer.from(JSON.stringify(record));
    const slot = this._slots[slotIdx];
    
    if (!slot.deleted && buf.length <= slot.length) {
      // In-place update
      buf.copy(this._data, slot.offset);
      slot.length = buf.length;
      return slotIdx;
    }

    // Delete old and insert new
    this.delete(slotIdx);
    return this.insert(record);
  }

  /**
   * Compact: defragment the page by moving records together.
   */
  _compact() {
    const liveSlots = this._slots
      .map((s, i) => ({ ...s, originalIdx: i }))
      .filter(s => !s.deleted);

    // Sort by offset descending (records closer to end first)
    liveSlots.sort((a, b) => b.offset - a.offset);

    let writePos = this.pageSize;
    for (const slot of liveSlots) {
      const recordData = Buffer.from(this._data.subarray(slot.offset, slot.offset + slot.length));
      writePos -= slot.length;
      recordData.copy(this._data, writePos);
      this._slots[slot.originalIdx].offset = writePos;
    }

    this._freeEnd = writePos;
  }

  get slotCount() { return this._slots.length; }
  get liveRecords() { return this._slots.filter(s => !s.deleted).length; }
  
  get freeSpace() {
    const headerSize = 16;
    const slotOverhead = this._slots.length * 8;
    return this._freeEnd - headerSize - slotOverhead;
  }

  get fillFactor() {
    return ((this.pageSize - this.freeSpace) / this.pageSize * 100).toFixed(1);
  }

  /** Iterate all live records */
  *[Symbol.iterator]() {
    for (let i = 0; i < this._slots.length; i++) {
      if (!this._slots[i].deleted) {
        yield { slotIdx: i, record: this.read(i) };
      }
    }
  }
}
