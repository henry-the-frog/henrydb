// page-table.js — Virtual-to-physical page mapping with TLB cache
// Simulates OS-level memory management concepts used in database buffer pools.

export class PageTable {
  constructor(pageSize = 4096) {
    this._pageSize = pageSize;
    this._mappings = new Map(); // virtualPage → physicalPage
    this._nextPhysical = 0;
    this._tlb = new Map(); // TLB cache: virtualPage → physicalPage
    this._tlbSize = 64;
    this._stats = { hits: 0, misses: 0, faults: 0 };
  }

  /** Map a virtual page to physical memory. */
  map(virtualPage, physicalPage) {
    this._mappings.set(virtualPage, physicalPage ?? this._nextPhysical++);
    this._tlb.delete(virtualPage); // Invalidate TLB entry
  }

  /** Translate virtual address to physical. */
  translate(virtualAddr) {
    const page = Math.floor(virtualAddr / this._pageSize);
    const offset = virtualAddr % this._pageSize;
    
    // Check TLB first
    if (this._tlb.has(page)) {
      this._stats.hits++;
      return this._tlb.get(page) * this._pageSize + offset;
    }
    
    this._stats.misses++;
    
    // Page table walk
    if (this._mappings.has(page)) {
      const phys = this._mappings.get(page);
      this._tlbInsert(page, phys);
      return phys * this._pageSize + offset;
    }
    
    // Page fault
    this._stats.faults++;
    return null;
  }

  /** Unmap a virtual page. */
  unmap(virtualPage) {
    this._mappings.delete(virtualPage);
    this._tlb.delete(virtualPage);
  }

  _tlbInsert(virtual, physical) {
    if (this._tlb.size >= this._tlbSize) {
      // Evict oldest TLB entry
      const first = this._tlb.keys().next().value;
      this._tlb.delete(first);
    }
    this._tlb.set(virtual, physical);
  }

  getStats() {
    return {
      ...this._stats,
      mappings: this._mappings.size,
      tlbEntries: this._tlb.size,
      hitRate: this._stats.hits / (this._stats.hits + this._stats.misses) || 0,
    };
  }
}
