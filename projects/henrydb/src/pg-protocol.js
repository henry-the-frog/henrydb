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

// ===== Extended Query Protocol Messages =====

/**
 * Parse a Parse message from the client (extended query).
 * Format: [byte 'P'] [Int32 length] [string name\0] [string query\0] [Int16 numParams] [Int32 paramOids...]
 */
export function parseParseMessage(buf) {
  let offset = 4; // skip length
  
  // Statement name (null-terminated)
  const nameEnd = buf.indexOf(0, offset);
  const name = buf.toString('utf8', offset, nameEnd);
  offset = nameEnd + 1;
  
  // Query string (null-terminated)
  const queryEnd = buf.indexOf(0, offset);
  const query = buf.toString('utf8', offset, queryEnd);
  offset = queryEnd + 1;
  
  // Number of parameter type OIDs
  const numParams = buf.readInt16BE(offset);
  offset += 2;
  
  const paramTypes = [];
  for (let i = 0; i < numParams; i++) {
    paramTypes.push(buf.readInt32BE(offset));
    offset += 4;
  }
  
  return { name, query, paramTypes };
}

/**
 * Parse a Bind message from the client.
 * Format: [byte 'B'] [Int32 length] [string portal\0] [string stmt\0] 
 *         [Int16 numFormats] [Int16 formats...] [Int16 numParams] [Int32 len + bytes...]
 *         [Int16 numResultFormats] [Int16 formats...]
 */
export function parseBindMessage(buf) {
  let offset = 4; // skip length
  
  // Portal name
  const portalEnd = buf.indexOf(0, offset);
  const portal = buf.toString('utf8', offset, portalEnd);
  offset = portalEnd + 1;
  
  // Statement name
  const stmtEnd = buf.indexOf(0, offset);
  const statement = buf.toString('utf8', offset, stmtEnd);
  offset = stmtEnd + 1;
  
  // Parameter format codes
  const numFormats = buf.readInt16BE(offset);
  offset += 2;
  const paramFormats = [];
  for (let i = 0; i < numFormats; i++) {
    paramFormats.push(buf.readInt16BE(offset));
    offset += 2;
  }
  
  // Parameter values
  const numParams = buf.readInt16BE(offset);
  offset += 2;
  const paramValues = [];
  for (let i = 0; i < numParams; i++) {
    const len = buf.readInt32BE(offset);
    offset += 4;
    if (len === -1) {
      paramValues.push(null);
    } else {
      // Format code: 0 = text, 1 = binary
      const format = paramFormats.length === 1 ? paramFormats[0] : (paramFormats[i] || 0);
      if (format === 0) {
        paramValues.push(buf.toString('utf8', offset, offset + len));
      } else {
        paramValues.push(buf.subarray(offset, offset + len));
      }
      offset += len;
    }
  }
  
  // Result format codes
  const numResultFormats = buf.readInt16BE(offset);
  offset += 2;
  const resultFormats = [];
  for (let i = 0; i < numResultFormats; i++) {
    resultFormats.push(buf.readInt16BE(offset));
    offset += 2;
  }
  
  return { portal, statement, paramFormats, paramValues, resultFormats };
}

/**
 * Parse a Describe message from the client.
 * Format: [byte 'D'] [Int32 length] [byte type] [string name\0]
 * type: 'S' = prepared statement, 'P' = portal
 */
export function parseDescribeMessage(buf) {
  const type = String.fromCharCode(buf[4]); // 'S' or 'P'
  const nameEnd = buf.indexOf(0, 5);
  const name = buf.toString('utf8', 5, nameEnd);
  return { type, name };
}

/**
 * Parse an Execute message from the client.
 * Format: [byte 'E'] [Int32 length] [string portal\0] [Int32 maxRows]
 */
export function parseExecuteMessage(buf) {
  const portalEnd = buf.indexOf(0, 4);
  const portal = buf.toString('utf8', 4, portalEnd);
  const maxRows = buf.readInt32BE(portalEnd + 1);
  return { portal, maxRows };
}

/**
 * Parse a Close message from the client.
 * Format: [byte 'C'] [Int32 length] [byte type] [string name\0]
 */
export function parseCloseMessage(buf) {
  const type = String.fromCharCode(buf[4]);
  const nameEnd = buf.indexOf(0, 5);
  const name = buf.toString('utf8', 5, nameEnd);
  return { type, name };
}

/** Write ParseComplete (1) */
export function writeParseComplete() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x31; // '1'
  buf.writeInt32BE(4, 1);
  return buf;
}

/** Write BindComplete (2) */
export function writeBindComplete() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x32; // '2'
  buf.writeInt32BE(4, 1);
  return buf;
}

/** Write CloseComplete (3) */
export function writeCloseComplete() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x33; // '3'
  buf.writeInt32BE(4, 1);
  return buf;
}

/** Write NoData (n) — no result columns */
export function writeNoData() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x6E; // 'n'
  buf.writeInt32BE(4, 1);
  return buf;
}

/** Write ParameterDescription (t) — parameter types for prepared statement */
export function writeParameterDescription(paramTypes) {
  const len = 4 + 2 + paramTypes.length * 4;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x74; // 't'
  buf.writeInt32BE(len, 1);
  buf.writeInt16BE(paramTypes.length, 5);
  let offset = 7;
  for (const oid of paramTypes) {
    buf.writeInt32BE(oid, offset);
    offset += 4;
  }
  return buf;
}

/** Write EmptyQueryResponse (I) */
export function writeEmptyQueryResponse() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x49; // 'I'
  buf.writeInt32BE(4, 1);
  return buf;
}

/** Write PortalSuspended (s) */
export function writePortalSuspended() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x73; // 's'
  buf.writeInt32BE(4, 1);
  return buf;
}

/**
 * Write NotificationResponse (A) — async notification to a listening client.
 * Format: [byte 'A'] [Int32 length] [Int32 pid] [string channel\0] [string payload\0]
 */
export function writeNotificationResponse(pid, channel, payload = '') {
  const channelBuf = Buffer.from(channel + '\0', 'utf8');
  const payloadBuf = Buffer.from(payload + '\0', 'utf8');
  const len = 4 + 4 + channelBuf.length + payloadBuf.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x41; // 'A'
  buf.writeInt32BE(len, 1);
  buf.writeInt32BE(pid, 5);
  channelBuf.copy(buf, 9);
  payloadBuf.copy(buf, 9 + channelBuf.length);
  return buf;
}

// ===== COPY Protocol Messages =====

/**
 * Write CopyInResponse (G) — server tells client to start sending COPY data.
 * format: 0 = text, 1 = binary
 */
export function writeCopyInResponse(numColumns, format = 0) {
  const len = 4 + 1 + 2 + numColumns * 2;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x47; // 'G'
  buf.writeInt32BE(len, 1);
  buf[5] = format;
  buf.writeInt16BE(numColumns, 6);
  let offset = 8;
  for (let i = 0; i < numColumns; i++) {
    buf.writeInt16BE(format, offset);
    offset += 2;
  }
  return buf;
}

/**
 * Write CopyOutResponse (H) — server tells client it will send COPY data.
 */
export function writeCopyOutResponse(numColumns, format = 0) {
  const len = 4 + 1 + 2 + numColumns * 2;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x48; // 'H'
  buf.writeInt32BE(len, 1);
  buf[5] = format;
  buf.writeInt16BE(numColumns, 6);
  let offset = 8;
  for (let i = 0; i < numColumns; i++) {
    buf.writeInt16BE(format, offset);
    offset += 2;
  }
  return buf;
}

/**
 * Write CopyData (d) — a chunk of COPY data.
 */
export function writeCopyData(data) {
  const dataBuf = typeof data === 'string' ? Buffer.from(data, 'utf8') : data;
  const len = 4 + dataBuf.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x64; // 'd'
  buf.writeInt32BE(len, 1);
  dataBuf.copy(buf, 5);
  return buf;
}

/**
 * Write CopyDone (c) — end of COPY data.
 */
export function writeCopyDone() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x63; // 'c'
  buf.writeInt32BE(4, 1);
  return buf;
}

export { PG_TYPES };
