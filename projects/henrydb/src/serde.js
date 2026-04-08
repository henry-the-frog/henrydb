// serde.js — Pluggable serialization framework
// JSON, compact binary (MessagePack-like), and CSV formats.
// Each format implements serialize(row) → Buffer and deserialize(buf) → row.

export class JSONSerde {
  serialize(row) { return Buffer.from(JSON.stringify(row)); }
  deserialize(buf) { return JSON.parse(buf.toString()); }
  get name() { return 'json'; }
}

export class BinarySerde {
  // Compact binary: type tags + values. No schema needed.
  // Tags: 0=null, 1=bool, 2=int32, 3=float64, 4=string, 5=object, 6=array
  serialize(row) {
    const parts = [];
    this._encodeValue(row, parts);
    return Buffer.concat(parts);
  }

  deserialize(buf) { return this._decodeValue(buf, { offset: 0 }); }

  _encodeValue(val, parts) {
    if (val === null || val === undefined) {
      parts.push(Buffer.from([0]));
    } else if (typeof val === 'boolean') {
      parts.push(Buffer.from([1, val ? 1 : 0]));
    } else if (typeof val === 'number' && Number.isInteger(val) && val >= -2147483648 && val <= 2147483647) {
      const b = Buffer.alloc(5); b[0] = 2; b.writeInt32LE(val, 1); parts.push(b);
    } else if (typeof val === 'number') {
      const b = Buffer.alloc(9); b[0] = 3; b.writeDoubleLE(val, 1); parts.push(b);
    } else if (typeof val === 'string') {
      const strBuf = Buffer.from(val, 'utf8');
      const header = Buffer.alloc(5); header[0] = 4; header.writeUInt32LE(strBuf.length, 1);
      parts.push(header, strBuf);
    } else if (Array.isArray(val)) {
      const header = Buffer.alloc(5); header[0] = 6; header.writeUInt32LE(val.length, 1);
      parts.push(header);
      for (const item of val) this._encodeValue(item, parts);
    } else if (typeof val === 'object') {
      const keys = Object.keys(val);
      const header = Buffer.alloc(5); header[0] = 5; header.writeUInt32LE(keys.length, 1);
      parts.push(header);
      for (const key of keys) {
        this._encodeValue(key, parts);
        this._encodeValue(val[key], parts);
      }
    }
  }

  _decodeValue(buf, ctx) {
    const tag = buf[ctx.offset++];
    switch (tag) {
      case 0: return null;
      case 1: return buf[ctx.offset++] !== 0;
      case 2: { const v = buf.readInt32LE(ctx.offset); ctx.offset += 4; return v; }
      case 3: { const v = buf.readDoubleLE(ctx.offset); ctx.offset += 8; return v; }
      case 4: {
        const len = buf.readUInt32LE(ctx.offset); ctx.offset += 4;
        const str = buf.subarray(ctx.offset, ctx.offset + len).toString('utf8'); ctx.offset += len;
        return str;
      }
      case 5: {
        const count = buf.readUInt32LE(ctx.offset); ctx.offset += 4;
        const obj = {};
        for (let i = 0; i < count; i++) {
          const key = this._decodeValue(buf, ctx);
          obj[key] = this._decodeValue(buf, ctx);
        }
        return obj;
      }
      case 6: {
        const count = buf.readUInt32LE(ctx.offset); ctx.offset += 4;
        const arr = [];
        for (let i = 0; i < count; i++) arr.push(this._decodeValue(buf, ctx));
        return arr;
      }
      default: return null;
    }
  }

  get name() { return 'binary'; }
}

export class CSVSerde {
  constructor(columns) { this.columns = columns; }

  serialize(row) {
    const values = this.columns.map(c => {
      const v = row[c];
      if (v === null || v === undefined) return '';
      const s = String(v);
      return s.includes(',') || s.includes('"') || s.includes('\n')
        ? '"' + s.replace(/"/g, '""') + '"' : s;
    });
    return Buffer.from(values.join(','));
  }

  deserialize(buf) {
    const line = buf.toString();
    const values = this._parseCSVLine(line);
    const row = {};
    for (let i = 0; i < this.columns.length; i++) {
      row[this.columns[i]] = values[i] === '' ? null : isNaN(values[i]) ? values[i] : Number(values[i]);
    }
    return row;
  }

  _parseCSVLine(line) {
    const values = [];
    let current = '', inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      if (inQuotes) {
        if (line[i] === '"' && line[i + 1] === '"') { current += '"'; i++; }
        else if (line[i] === '"') inQuotes = false;
        else current += line[i];
      } else {
        if (line[i] === '"') inQuotes = true;
        else if (line[i] === ',') { values.push(current); current = ''; }
        else current += line[i];
      }
    }
    values.push(current);
    return values;
  }

  get name() { return 'csv'; }
}
