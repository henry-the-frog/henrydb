// heap-file.js — Heap file organization with free space map
// A heap file is an unordered collection of pages.
// Free Space Map (FSM) tracks available space per page for fast inserts.
// Record IDs are (pageId, slotId) pairs.

import { SlottedPage } from './slotted-page.js';

export class HeapFile {
  constructor(options = {}) {
    this.pageSize = options.pageSize || 4096;
    this._pages = []; // Array of SlottedPage
    this._fsm = []; // Free space per page (bytes)
    this._recordCount = 0;
  }

  /**
   * Insert a record. Returns record ID: { pageId, slotId }.
   */
  insert(record) {
    const buf = Buffer.from(JSON.stringify(record));
    
    // Find a page with enough space
    let targetPage = this._findPageWithSpace(buf.length + 16); // +16 for slot overhead
    
    if (targetPage === null) {
      targetPage = this._allocatePage();
    }

    const page = this._pages[targetPage];
    const slotId = page.insert(record);
    
    if (slotId < 0) {
      // Page was full after all — allocate new
      const newPageId = this._allocatePage();
      const newPage = this._pages[newPageId];
      const newSlotId = newPage.insert(record);
      this._updateFSM(newPageId);
      this._recordCount++;
      return { pageId: newPageId, slotId: newSlotId };
    }

    this._updateFSM(targetPage);
    this._recordCount++;
    return { pageId: targetPage, slotId };
  }

  /**
   * Read a record by ID.
   */
  read(rid) {
    if (rid.pageId >= this._pages.length) return null;
    return this._pages[rid.pageId].read(rid.slotId);
  }

  /**
   * Delete a record.
   */
  delete(rid) {
    if (rid.pageId >= this._pages.length) return false;
    const ok = this._pages[rid.pageId].delete(rid.slotId);
    if (ok) {
      this._updateFSM(rid.pageId);
      this._recordCount--;
    }
    return ok;
  }

  /**
   * Update a record.
   */
  update(rid, record) {
    if (rid.pageId >= this._pages.length) return null;
    const newSlotId = this._pages[rid.pageId].update(rid.slotId, record);
    if (newSlotId >= 0) {
      this._updateFSM(rid.pageId);
      return { pageId: rid.pageId, slotId: newSlotId };
    }
    // No space on same page — delete and insert elsewhere
    this.delete(rid);
    this._recordCount++; // Undo delete decrement
    return this.insert(record);
  }

  /**
   * Full table scan — iterate all records.
   */
  *scan() {
    for (let pageId = 0; pageId < this._pages.length; pageId++) {
      for (const { slotIdx, record } of this._pages[pageId]) {
        yield { rid: { pageId, slotId: slotIdx }, record };
      }
    }
  }

  _findPageWithSpace(needed) {
    for (let i = 0; i < this._fsm.length; i++) {
      if (this._fsm[i] >= needed) return i;
    }
    return null;
  }

  _allocatePage() {
    const pageId = this._pages.length;
    this._pages.push(new SlottedPage(pageId, this.pageSize));
    this._fsm.push(this.pageSize - 16); // Initial free space
    return pageId;
  }

  _updateFSM(pageId) {
    this._fsm[pageId] = this._pages[pageId].freeSpace;
  }

  get pageCount() { return this._pages.length; }
  get recordCount() { return this._recordCount; }

  getStats() {
    const totalFree = this._fsm.reduce((s, f) => s + f, 0);
    const totalSpace = this._pages.length * this.pageSize;
    return {
      pages: this._pages.length,
      records: this._recordCount,
      utilization: totalSpace > 0 ? ((1 - totalFree / totalSpace) * 100).toFixed(1) + '%' : '0%',
    };
  }
}
