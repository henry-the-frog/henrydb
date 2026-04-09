// serializer.js — Binary serialization/deserialization for database values
export function serialize(value) {
  if (value === null) return Buffer.from([0]);
  if (typeof value === 'number') { const b = Buffer.alloc(9); b[0] = 1; b.writeDoubleBE(value, 1); return b; }
  if (typeof value === 'boolean') return Buffer.from([2, value ? 1 : 0]);
  const str = String(value);
  const b = Buffer.alloc(5 + str.length);
  b[0] = 3;
  b.writeUInt32BE(str.length, 1);
  b.write(str, 5);
  return b;
}

export function deserialize(buf, offset = 0) {
  const type = buf[offset];
  if (type === 0) return { value: null, bytesRead: 1 };
  if (type === 1) return { value: buf.readDoubleBE(offset + 1), bytesRead: 9 };
  if (type === 2) return { value: buf[offset + 1] === 1, bytesRead: 2 };
  if (type === 3) {
    const len = buf.readUInt32BE(offset + 1);
    return { value: buf.toString('utf8', offset + 5, offset + 5 + len), bytesRead: 5 + len };
  }
  return { value: undefined, bytesRead: 1 };
}
