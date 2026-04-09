// freelist.js — Free page/block list management
// Tracks allocated and free pages in a storage engine.
export class FreeList {
  constructor(maxPages = 1000) {
    this._free = new Set(Array.from({ length: maxPages }, (_, i) => i));
    this._allocated = new Set();
    this._maxPages = maxPages;
  }

  allocate() {
    if (this._free.size === 0) return -1;
    const page = this._free.values().next().value;
    this._free.delete(page);
    this._allocated.add(page);
    return page;
  }

  deallocate(page) {
    this._allocated.delete(page);
    this._free.add(page);
  }

  get freeCount() { return this._free.size; }
  get allocatedCount() { return this._allocated.size; }
  isAllocated(page) { return this._allocated.has(page); }
}
