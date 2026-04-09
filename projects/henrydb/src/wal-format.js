// wal-format.js — Structured binary WAL format with CRC checksums
// Each WAL record has: [length(4)] [type(1)] [txId(4)] [crc32(4)] [data(N)]
// This is the on-disk format; the in-memory WAL in wal.js is higher-level.

const RECORD_TYPES = {
  BEGIN: 1,
  INSERT: 2,
  UPDATE: 3,
  DELETE: 4,
  COMMIT: 5,
  ROLLBACK: 6,
  CHECKPOINT: 7,
};

const HEADER_SIZE = 13; // 4 + 1 + 4 + 4

/**
 * CRC32 (IEEE polynomial) for data integrity.
 */
function crc32(buf) {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    crc ^= buf[i];
    for (let j = 0; j < 8; j++) {
      crc = (crc >>> 1) ^ (crc & 1 ? 0xEDB88320 : 0);
    }
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

/**
 * WALWriter — Serializes WAL records to a buffer.
 */
export class WALWriter {
  constructor(capacity = 1024 * 1024) {
    this._buf = Buffer.alloc(capacity);
    this._offset = 0;
    this._recordCount = 0;
  }

  get offset() { return this._offset; }
  get recordCount() { return this._recordCount; }

  /**
   * Write a WAL record.
   */
  writeRecord(type, txId, data) {
    const dataBuf = Buffer.isBuffer(data) ? data : Buffer.from(JSON.stringify(data));
    const totalLen = HEADER_SIZE + dataBuf.length;
    
    // Grow if needed
    while (this._offset + totalLen > this._buf.length) {
      const newBuf = Buffer.alloc(this._buf.length * 2);
      this._buf.copy(newBuf);
      this._buf = newBuf;
    }

    const checkCrc = crc32(dataBuf);

    this._buf.writeUInt32LE(dataBuf.length, this._offset);
    this._buf.writeUInt8(type, this._offset + 4);
    this._buf.writeUInt32LE(txId, this._offset + 5);
    this._buf.writeUInt32LE(checkCrc, this._offset + 9);
    dataBuf.copy(this._buf, this._offset + HEADER_SIZE);
    
    this._offset += totalLen;
    this._recordCount++;
    return { offset: this._offset - totalLen, length: totalLen };
  }

  /**
   * Get the buffer up to current offset.
   */
  getBuffer() {
    return this._buf.subarray(0, this._offset);
  }
}

/**
 * WALReader — Reads WAL records from a buffer.
 */
export class WALReader {
  constructor(buf) {
    this._buf = buf;
    this._offset = 0;
  }

  get remaining() { return this._buf.length - this._offset; }

  /**
   * Read next record. Returns null if no more.
   */
  readRecord() {
    if (this._offset + HEADER_SIZE > this._buf.length) return null;

    const dataLen = this._buf.readUInt32LE(this._offset);
    const type = this._buf.readUInt8(this._offset + 4);
    const txId = this._buf.readUInt32LE(this._offset + 5);
    const storedCrc = this._buf.readUInt32LE(this._offset + 9);

    if (this._offset + HEADER_SIZE + dataLen > this._buf.length) return null;

    const dataBuf = this._buf.subarray(this._offset + HEADER_SIZE, this._offset + HEADER_SIZE + dataLen);
    const actualCrc = crc32(dataBuf);

    if (storedCrc !== actualCrc) {
      return { type: 'CORRUPTED', offset: this._offset, expected: storedCrc, actual: actualCrc };
    }

    this._offset += HEADER_SIZE + dataLen;

    let data;
    try { data = JSON.parse(dataBuf.toString()); }
    catch { data = dataBuf; }

    return { type, txId, data, crc: storedCrc };
  }

  /**
   * Read all records.
   */
  readAll() {
    const records = [];
    let rec;
    while ((rec = this.readRecord()) !== null) {
      records.push(rec);
    }
    return records;
  }
}

export { RECORD_TYPES, crc32, HEADER_SIZE };
