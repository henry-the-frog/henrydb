// varint.js — Variable-length integer encoding (protobuf/SQLite style)
// Small integers use fewer bytes. Used in database wire protocols and storage formats.

export function encodeVarint(value) {
  const bytes = [];
  let v = value >>> 0;
  while (v > 0x7F) {
    bytes.push((v & 0x7F) | 0x80);
    v >>>= 7;
  }
  bytes.push(v);
  return Buffer.from(bytes);
}

export function decodeVarint(buf, offset = 0) {
  let value = 0, shift = 0;
  for (let i = offset; i < buf.length; i++) {
    value |= (buf[i] & 0x7F) << shift;
    if ((buf[i] & 0x80) === 0) return { value: value >>> 0, bytesRead: i - offset + 1 };
    shift += 7;
  }
  return { value: value >>> 0, bytesRead: buf.length - offset };
}

export function varintSize(value) {
  if (value < 128) return 1;
  if (value < 16384) return 2;
  if (value < 2097152) return 3;
  if (value < 268435456) return 4;
  return 5;
}
