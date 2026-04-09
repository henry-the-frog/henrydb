// page-layout.js — Slotted page layout for row storage
// Each page has: header | slot directory | free space | row data
// Rows grow from end toward front, slots grow from front toward end.

export class SlottedPage {
  constructor(pageSize = 4096) {
    this._buf = Buffer.alloc(pageSize);
    this._pageSize = pageSize;
    this._slotCount = 0;
    this._freeOffset = pageSize; // Rows grow from end
    this._headerSize = 8;       // slot_count(2) + free_offset(2) + reserved(4)
    this._buf.writeUInt16LE(0, 0);
    this._buf.writeUInt16LE(pageSize, 2);
  }

  get slotCount() { return this._slotCount; }
  get freeSpace() { return this._freeOffset - this._headerSize - this._slotCount * 4; }

  insertRow(data) {
    const rowBuf = Buffer.from(JSON.stringify(data));
    if (rowBuf.length + 4 > this.freeSpace) return -1; // No space
    
    this._freeOffset -= rowBuf.length;
    rowBuf.copy(this._buf, this._freeOffset);
    
    // Add slot: offset(2) + length(2)
    const slotOffset = this._headerSize + this._slotCount * 4;
    this._buf.writeUInt16LE(this._freeOffset, slotOffset);
    this._buf.writeUInt16LE(rowBuf.length, slotOffset + 2);
    this._slotCount++;
    
    this._buf.writeUInt16LE(this._slotCount, 0);
    this._buf.writeUInt16LE(this._freeOffset, 2);
    
    return this._slotCount - 1; // Slot ID
  }

  getRow(slotId) {
    if (slotId >= this._slotCount) return null;
    const slotOffset = this._headerSize + slotId * 4;
    const rowOffset = this._buf.readUInt16LE(slotOffset);
    const rowLength = this._buf.readUInt16LE(slotOffset + 2);
    return JSON.parse(this._buf.subarray(rowOffset, rowOffset + rowLength).toString());
  }
}
