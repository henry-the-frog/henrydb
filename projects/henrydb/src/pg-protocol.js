// pg-protocol.js — PostgreSQL wire protocol v3 message encoder/decoder
// Implements enough of the protocol for psql to connect and run queries.

// Type OIDs (for RowDescription)
const PG_TYPES = {
  INT4: 23,
  TEXT: 25,
  FLOAT8: 701,
  BOOL: 16,
  VARCHAR: 1043,
};

// ===== Message Writer =====

/** Write AuthenticationOk (R) — authentication succeeded */
export function writeAuthenticationOk() {
  const buf = Buffer.alloc(9);
  buf[0] = 0x52; // 'R'
  buf.writeInt32BE(8, 1);  // length
  buf.writeInt32BE(0, 5);  // auth type 0 = OK
  return buf;
}

/** Write ParameterStatus (S) — server parameter */
export function writeParameterStatus(name, value) {
  const nameBytes = Buffer.from(name + '\0', 'utf8');
  const valueBytes = Buffer.from(value + '\0', 'utf8');
  const len = 4 + nameBytes.length + valueBytes.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x53; // 'S'
  buf.writeInt32BE(len, 1);
  nameBytes.copy(buf, 5);
  valueBytes.copy(buf, 5 + nameBytes.length);
  return buf;
}

/** Write BackendKeyData (K) — process ID and secret key */
export function writeBackendKeyData(processId, secretKey) {
  const buf = Buffer.alloc(13);
  buf[0] = 0x4B; // 'K'
  buf.writeInt32BE(12, 1);
  buf.writeInt32BE(processId, 5);
  buf.writeInt32BE(secretKey, 9);
  return buf;
}

/** Write ReadyForQuery (Z) — server is ready for next query */
export function writeReadyForQuery(txStatus = 'I') {
  const buf = Buffer.alloc(6);
  buf[0] = 0x5A; // 'Z'
  buf.writeInt32BE(5, 1);
  buf[5] = txStatus.charCodeAt(0); // 'I' = idle, 'T' = in transaction, 'E' = error
  return buf;
}

/** Write RowDescription (T) — column metadata */
export function writeRowDescription(columns) {
  // Calculate total size
  let size = 4 + 2; // length + field count
  for (const col of columns) {
    size += Buffer.byteLength(col.name, 'utf8') + 1; // name + null
    size += 4 + 2 + 4 + 2 + 4 + 2; // tableOid, colIdx, typeOid, typeSize, typeMod, format
  }
  
  const buf = Buffer.alloc(1 + size);
  let offset = 0;
  buf[offset++] = 0x54; // 'T'
  buf.writeInt32BE(size, offset); offset += 4;
  buf.writeInt16BE(columns.length, offset); offset += 2;
  
  for (const col of columns) {
    // Column name (null-terminated)
    const nameLen = Buffer.byteLength(col.name, 'utf8');
    buf.write(col.name, offset, 'utf8'); offset += nameLen;
    buf[offset++] = 0;
    
    // Table OID (0 = not from a table)
    buf.writeInt32BE(0, offset); offset += 4;
    // Column index
    buf.writeInt16BE(0, offset); offset += 2;
    // Type OID
    buf.writeInt32BE(col.typeOid || PG_TYPES.TEXT, offset); offset += 4;
    // Type size (-1 for variable length)
    buf.writeInt16BE(col.typeSize || -1, offset); offset += 2;
    // Type modifier (-1 = no modifier)
    buf.writeInt32BE(-1, offset); offset += 4;
    // Format code (0 = text)
    buf.writeInt16BE(0, offset); offset += 2;
  }
  
  return buf;
}

/** Write DataRow (D) — one row of query results */
export function writeDataRow(values) {
  // Calculate total size
  let size = 4 + 2; // length + field count
  const encoded = values.map(v => {
    if (v === null || v === undefined) return null;
    return Buffer.from(String(v), 'utf8');
  });
  
  for (const v of encoded) {
    size += 4; // field length (or -1 for null)
    if (v !== null) size += v.length;
  }
  
  const buf = Buffer.alloc(1 + size);
  let offset = 0;
  buf[offset++] = 0x44; // 'D'
  buf.writeInt32BE(size, offset); offset += 4;
  buf.writeInt16BE(encoded.length, offset); offset += 2;
  
  for (const v of encoded) {
    if (v === null) {
      buf.writeInt32BE(-1, offset); offset += 4;
    } else {
      buf.writeInt32BE(v.length, offset); offset += 4;
      v.copy(buf, offset); offset += v.length;
    }
  }
  
  return buf;
}

/** Write CommandComplete (C) — command execution result */
export function writeCommandComplete(tag) {
  const tagBytes = Buffer.from(tag + '\0', 'utf8');
  const len = 4 + tagBytes.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x43; // 'C'
  buf.writeInt32BE(len, 1);
  tagBytes.copy(buf, 5);
  return buf;
}

/** Write ErrorResponse (E) — error message */
export function writeErrorResponse(severity, code, message) {
  // Fields: S (severity), V (verbose severity), C (code), M (message), null terminator
  const fields = [
    Buffer.from('S' + severity + '\0', 'utf8'),
    Buffer.from('V' + severity + '\0', 'utf8'),
    Buffer.from('C' + code + '\0', 'utf8'),
    Buffer.from('M' + message + '\0', 'utf8'),
    Buffer.from('\0', 'utf8'), // terminator
  ];
  
  const totalFieldSize = fields.reduce((s, f) => s + f.length, 0);
  const len = 4 + totalFieldSize;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x45; // 'E'
  buf.writeInt32BE(len, 1);
  
  let offset = 5;
  for (const f of fields) {
    f.copy(buf, offset);
    offset += f.length;
  }
  
  return buf;
}

/** Write NoticeResponse (N) — notice message */
export function writeNotice(message) {
  return writeErrorResponse('NOTICE', '00000', message);
}

// ===== Message Reader =====

/**
 * Parse a StartupMessage from the client.
 * Format: [Int32 length] [Int32 protocol version] [key\0value\0 pairs] [\0]
 */
export function parseStartupMessage(buf) {
  const len = buf.readInt32BE(0);
  const protocolVersion = buf.readInt32BE(4);
  
  const major = protocolVersion >> 16;
  const minor = protocolVersion & 0xFFFF;
  
  // Parse key-value parameters
  const params = {};
  let offset = 8;
  while (offset < len - 1) {
    const keyEnd = buf.indexOf(0, offset);
    if (keyEnd === -1 || keyEnd >= len) break;
    const key = buf.toString('utf8', offset, keyEnd);
    offset = keyEnd + 1;
    
    const valEnd = buf.indexOf(0, offset);
    if (valEnd === -1 || valEnd >= len) break;
    const value = buf.toString('utf8', offset, valEnd);
    offset = valEnd + 1;
    
    if (key) params[key] = value;
  }
  
  return { protocolVersion: { major, minor }, params };
}

/**
 * Parse a Query message from the client.
 * Format: [byte 'Q'] [Int32 length] [string\0]
 */
export function parseQueryMessage(buf) {
  // buf starts after the 'Q' byte
  const len = buf.readInt32BE(0);
  const query = buf.toString('utf8', 4, 4 + len - 4 - 1); // Exclude null terminator
  return query;
}

/**
 * Determine the PG type OID for a JavaScript value.
 */
export function inferTypeOid(value) {
  if (value === null || value === undefined) return PG_TYPES.TEXT;
  if (typeof value === 'number') {
    return Number.isInteger(value) ? PG_TYPES.INT4 : PG_TYPES.FLOAT8;
  }
  if (typeof value === 'boolean') return PG_TYPES.BOOL;
  return PG_TYPES.TEXT;
}

export { PG_TYPES };
