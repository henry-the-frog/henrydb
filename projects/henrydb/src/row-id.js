// row-id.js — Row identifier (RID) for tuple addressing
export class RowId {
  constructor(pageId, slotId) { this.pageId = pageId; this.slotId = slotId; }
  equals(other) { return this.pageId === other.pageId && this.slotId === other.slotId; }
  toString() { return `${this.pageId}:${this.slotId}`; }
  static parse(str) { const [p, s] = str.split(':').map(Number); return new RowId(p, s); }
  toKey() { return (this.pageId << 16) | this.slotId; }
}
