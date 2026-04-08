// tuple-descriptor.js — Schema-aware row serialization/deserialization
// Fixed-width types: INT32 (4 bytes), FLOAT64 (8 bytes), BOOL (1 byte)
// Variable-width: VARCHAR (4-byte length prefix + data)
// NULL bitmap: 1 bit per column, packed into bytes at the start

const TYPES = {
  INT32: { id: 1, size: 4, name: 'INT32' },
  FLOAT64: { id: 2, size: 8, name: 'FLOAT64' },
  BOOL: { id: 3, size: 1, name: 'BOOL' },
  VARCHAR: { id: 4, size: -1, name: 'VARCHAR' }, // Variable
};

export class TupleDescriptor {
  constructor(columns) {
    // columns: [{ name, type: 'INT32'|'FLOAT64'|'BOOL'|'VARCHAR', nullable: bool }]
    this.columns = columns.map(c => ({
      name: c.name,
      type: TYPES[c.type] || TYPES.VARCHAR,
      nullable: c.nullable !== false,
    }));
    this._nullBitmapBytes = Math.ceil(this.columns.length / 8);
  }

  /**
   * Serialize a row object to a Buffer.
   */
  serialize(row) {
    const parts = [];
    
    // Null bitmap
    const nullBitmap = Buffer.alloc(this._nullBitmapBytes);
    for (let i = 0; i < this.columns.length; i++) {
      if (row[this.columns[i].name] == null) {
        nullBitmap[i >>> 3] |= (1 << (i & 7));
      }
    }
    parts.push(nullBitmap);

    // Column values
    for (let i = 0; i < this.columns.length; i++) {
      const col = this.columns[i];
      const value = row[col.name];
      
      if (value == null) continue; // Skip null values (already in bitmap)

      switch (col.type.id) {
        case 1: { // INT32
          const buf = Buffer.alloc(4);
          buf.writeInt32LE(value);
          parts.push(buf);
          break;
        }
        case 2: { // FLOAT64
          const buf = Buffer.alloc(8);
          buf.writeDoubleLE(value);
          parts.push(buf);
          break;
        }
        case 3: { // BOOL
          parts.push(Buffer.from([value ? 1 : 0]));
          break;
        }
        case 4: { // VARCHAR
          const str = String(value);
          const strBuf = Buffer.from(str, 'utf8');
          const lenBuf = Buffer.alloc(4);
          lenBuf.writeUInt32LE(strBuf.length);
          parts.push(lenBuf, strBuf);
          break;
        }
      }
    }

    return Buffer.concat(parts);
  }

  /**
   * Deserialize a Buffer back to a row object.
   */
  deserialize(buf) {
    const row = {};
    let offset = 0;

    // Read null bitmap
    const nullBitmap = buf.subarray(0, this._nullBitmapBytes);
    offset = this._nullBitmapBytes;

    for (let i = 0; i < this.columns.length; i++) {
      const col = this.columns[i];
      const isNull = (nullBitmap[i >>> 3] & (1 << (i & 7))) !== 0;

      if (isNull) {
        row[col.name] = null;
        continue;
      }

      switch (col.type.id) {
        case 1: // INT32
          row[col.name] = buf.readInt32LE(offset);
          offset += 4;
          break;
        case 2: // FLOAT64
          row[col.name] = buf.readDoubleLE(offset);
          offset += 8;
          break;
        case 3: // BOOL
          row[col.name] = buf[offset] !== 0;
          offset += 1;
          break;
        case 4: { // VARCHAR
          const len = buf.readUInt32LE(offset);
          offset += 4;
          row[col.name] = buf.subarray(offset, offset + len).toString('utf8');
          offset += len;
          break;
        }
      }
    }

    return row;
  }

  /**
   * Calculate the serialized size of a row (without actually serializing).
   */
  estimateSize(row) {
    let size = this._nullBitmapBytes;
    for (const col of this.columns) {
      if (row[col.name] == null) continue;
      if (col.type.size > 0) {
        size += col.type.size;
      } else {
        size += 4 + Buffer.byteLength(String(row[col.name]), 'utf8');
      }
    }
    return size;
  }

  get columnNames() { return this.columns.map(c => c.name); }
}

export { TYPES };
