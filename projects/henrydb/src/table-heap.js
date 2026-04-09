// data-page.js — Fixed-size data page with header
export class DataPage {
  constructor(pageId, size = 4096) {
    this.pageId = pageId;
    this.dirty = false;
    this.pinCount = 0;
    this.data = Buffer.alloc(size);
    this.lsn = 0;
  }
  
  pin() { this.pinCount++; }
  unpin() { if (this.pinCount > 0) this.pinCount--; }
  markDirty() { this.dirty = true; }
  get isPinned() { return this.pinCount > 0; }
}

// table-heap.js — Heap file: unordered collection of pages
export class TableHeap {
  constructor(pageSize = 4096) {
    this._pages = [];
    this._pageSize = pageSize;
    this._rowCount = 0;
  }

  insert(row) {
    const data = JSON.stringify(row);
    this._rowCount++;
    // Simple: just append to last page or create new one
    if (this._pages.length === 0 || this._pages[this._pages.length - 1].data.length > this._pageSize * 0.9) {
      this._pages.push({ rows: [] });
    }
    this._pages[this._pages.length - 1].rows.push(row);
    return { pageId: this._pages.length - 1, slotId: this._pages[this._pages.length - 1].rows.length - 1 };
  }

  *scan() { for (const page of this._pages) for (const row of page.rows) yield row; }
  get rowCount() { return this._rowCount; }
  get pageCount() { return this._pages.length; }
}
