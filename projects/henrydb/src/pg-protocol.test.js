// pg-protocol.test.js — Tests for PostgreSQL wire protocol messages
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  writeAuthenticationOk, writeParameterStatus, writeBackendKeyData,
  writeReadyForQuery, writeRowDescription, writeDataRow,
  writeCommandComplete, writeErrorResponse,
  parseStartupMessage, parseQueryMessage, inferTypeOid,
  PG_TYPES,
} from './pg-protocol.js';

describe('PG Protocol Writer', () => {
  it('AuthenticationOk', () => {
    const buf = writeAuthenticationOk();
    assert.strictEqual(buf[0], 0x52); // 'R'
    assert.strictEqual(buf.readInt32BE(1), 8);
    assert.strictEqual(buf.readInt32BE(5), 0); // auth OK
  });

  it('ParameterStatus', () => {
    const buf = writeParameterStatus('server_version', '15.0');
    assert.strictEqual(buf[0], 0x53); // 'S'
    assert.ok(buf.includes(Buffer.from('server_version\0')));
    assert.ok(buf.includes(Buffer.from('15.0\0')));
  });

  it('BackendKeyData', () => {
    const buf = writeBackendKeyData(12345, 67890);
    assert.strictEqual(buf[0], 0x4B); // 'K'
    assert.strictEqual(buf.readInt32BE(5), 12345);
    assert.strictEqual(buf.readInt32BE(9), 67890);
  });

  it('ReadyForQuery', () => {
    const buf = writeReadyForQuery('I');
    assert.strictEqual(buf[0], 0x5A); // 'Z'
    assert.strictEqual(buf[5], 0x49); // 'I'
  });

  it('RowDescription', () => {
    const buf = writeRowDescription([
      { name: 'id', typeOid: PG_TYPES.INT4, typeSize: 4 },
      { name: 'name', typeOid: PG_TYPES.TEXT, typeSize: -1 },
    ]);
    assert.strictEqual(buf[0], 0x54); // 'T'
    // Read field count
    const fieldCount = buf.readInt16BE(5);
    assert.strictEqual(fieldCount, 2);
  });

  it('DataRow', () => {
    const buf = writeDataRow([42, 'Alice', null]);
    assert.strictEqual(buf[0], 0x44); // 'D'
    const fieldCount = buf.readInt16BE(5);
    assert.strictEqual(fieldCount, 3);
  });

  it('CommandComplete', () => {
    const buf = writeCommandComplete('SELECT 5');
    assert.strictEqual(buf[0], 0x43); // 'C'
    assert.ok(buf.includes(Buffer.from('SELECT 5\0')));
  });

  it('ErrorResponse', () => {
    const buf = writeErrorResponse('ERROR', '42P01', 'Table not found');
    assert.strictEqual(buf[0], 0x45); // 'E'
    assert.ok(buf.includes(Buffer.from('MTable not found\0')));
  });
});

describe('PG Protocol Reader', () => {
  it('parseStartupMessage', () => {
    // Build a startup message manually
    const params = 'user\0alice\0database\0mydb\0\0';
    const paramsBuf = Buffer.from(params, 'utf8');
    const len = 4 + 4 + paramsBuf.length;
    const buf = Buffer.alloc(len);
    buf.writeInt32BE(len, 0);
    buf.writeInt32BE(196608, 4); // protocol 3.0
    paramsBuf.copy(buf, 8);
    
    const result = parseStartupMessage(buf);
    assert.strictEqual(result.protocolVersion.major, 3);
    assert.strictEqual(result.protocolVersion.minor, 0);
    assert.strictEqual(result.params.user, 'alice');
    assert.strictEqual(result.params.database, 'mydb');
  });

  it('parseQueryMessage', () => {
    const sql = 'SELECT * FROM users';
    const queryBuf = Buffer.from(sql + '\0', 'utf8');
    const len = 4 + queryBuf.length;
    const buf = Buffer.alloc(len);
    buf.writeInt32BE(len, 0);
    queryBuf.copy(buf, 4);
    
    const result = parseQueryMessage(buf);
    assert.strictEqual(result, sql);
  });
});

describe('Type inference', () => {
  it('infers INT4 for integers', () => {
    assert.strictEqual(inferTypeOid(42), PG_TYPES.INT4);
  });
  it('infers FLOAT8 for floats', () => {
    assert.strictEqual(inferTypeOid(3.14), PG_TYPES.FLOAT8);
  });
  it('infers TEXT for strings', () => {
    assert.strictEqual(inferTypeOid('hello'), PG_TYPES.TEXT);
  });
  it('infers BOOL for booleans', () => {
    assert.strictEqual(inferTypeOid(true), PG_TYPES.BOOL);
  });
  it('infers TEXT for null', () => {
    assert.strictEqual(inferTypeOid(null), PG_TYPES.TEXT);
  });
});
