// server-extended.test.js — Tests for PostgreSQL extended query protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { HenryDBServer } from './server.js';

// ===== PG Client with Extended Query Support =====

class PgExtClient {
  constructor(port) {
    this.port = port;
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this._resolve = null;
  }

  async connect() {
    this.socket = await new Promise((resolve, reject) => {
      const s = net.createConnection({ host: '127.0.0.1', port: this.port }, () => resolve(s));
      s.on('error', reject);
    });
    this.socket.on('data', (data) => {
      this.buffer = Buffer.concat([this.buffer, data]);
      if (this._resolve && this._hasReadyForQuery()) {
        const r = this._resolve;
        this._resolve = null;
        r(this._consumeMessages());
      }
    });

    // Startup
    const params = `user\0test\0database\0test\0\0`;
    const paramsBuf = Buffer.from(params, 'utf8');
    const len = 4 + 4 + paramsBuf.length;
    const buf = Buffer.alloc(len);
    buf.writeInt32BE(len, 0);
    buf.writeInt32BE(196608, 4);
    paramsBuf.copy(buf, 8);
    this.socket.write(buf);
    await this._waitReady();
  }

  // Simple query
  async query(sql) {
    const queryBuf = Buffer.from(sql + '\0', 'utf8');
    const len = 4 + queryBuf.length;
    const buf = Buffer.alloc(1 + len);
    buf[0] = 0x51;
    buf.writeInt32BE(len, 1);
    queryBuf.copy(buf, 5);
    this.socket.write(buf);
    return this._waitReady();
  }

  // Extended query: Parse → Bind → Execute → Sync
  async preparedQuery(sql, params = [], stmtName = '') {
    // Parse
    this._sendParse(stmtName, sql, []);
    // Bind (unnamed portal)
    this._sendBind('', stmtName, params);
    // Describe (portal)
    this._sendDescribe('P', '');
    // Execute
    this._sendExecute('', 0);
    // Sync
    this._sendSync();
    return this._waitReady();
  }

  // Parse message
  _sendParse(name, query, paramTypes) {
    const nameBuf = Buffer.from(name + '\0', 'utf8');
    const queryBuf = Buffer.from(query + '\0', 'utf8');
    const len = 4 + nameBuf.length + queryBuf.length + 2 + paramTypes.length * 4;
    const buf = Buffer.alloc(1 + len);
    let offset = 0;
    buf[offset++] = 0x50; // 'P'
    buf.writeInt32BE(len, offset); offset += 4;
    nameBuf.copy(buf, offset); offset += nameBuf.length;
    queryBuf.copy(buf, offset); offset += queryBuf.length;
    buf.writeInt16BE(paramTypes.length, offset); offset += 2;
    for (const oid of paramTypes) {
      buf.writeInt32BE(oid, offset); offset += 4;
    }
    this.socket.write(buf);
  }

  // Bind message
  _sendBind(portal, statement, params) {
    const portalBuf = Buffer.from(portal + '\0', 'utf8');
    const stmtBuf = Buffer.from(statement + '\0', 'utf8');
    
    // Encode parameter values as text
    const encodedParams = params.map(p => {
      if (p === null || p === undefined) return null;
      return Buffer.from(String(p), 'utf8');
    });

    let size = 4 + portalBuf.length + stmtBuf.length;
    size += 2; // numFormats
    size += 2; // format code (1 = all text)
    size += 2; // numParams
    for (const ep of encodedParams) {
      size += 4; // length
      if (ep !== null) size += ep.length;
    }
    size += 2; // numResultFormats
    size += 2; // result format (text)

    const buf = Buffer.alloc(1 + size);
    let offset = 0;
    buf[offset++] = 0x42; // 'B'
    buf.writeInt32BE(size, offset); offset += 4;
    portalBuf.copy(buf, offset); offset += portalBuf.length;
    stmtBuf.copy(buf, offset); offset += stmtBuf.length;
    // Format codes: 1 code, all text (0)
    buf.writeInt16BE(1, offset); offset += 2;
    buf.writeInt16BE(0, offset); offset += 2;
    // Parameter values
    buf.writeInt16BE(encodedParams.length, offset); offset += 2;
    for (const ep of encodedParams) {
      if (ep === null) {
        buf.writeInt32BE(-1, offset); offset += 4;
      } else {
        buf.writeInt32BE(ep.length, offset); offset += 4;
        ep.copy(buf, offset); offset += ep.length;
      }
    }
    // Result format codes: 1 code, text
    buf.writeInt16BE(1, offset); offset += 2;
    buf.writeInt16BE(0, offset); offset += 2;
    this.socket.write(buf);
  }

  // Describe message
  _sendDescribe(type, name) {
    const nameBuf = Buffer.from(name + '\0', 'utf8');
    const len = 4 + 1 + nameBuf.length;
    const buf = Buffer.alloc(1 + len);
    buf[0] = 0x44; // 'D'
    buf.writeInt32BE(len, 1);
    buf[5] = type.charCodeAt(0);
    nameBuf.copy(buf, 6);
    this.socket.write(buf);
  }

  // Execute message
  _sendExecute(portal, maxRows) {
    const portalBuf = Buffer.from(portal + '\0', 'utf8');
    const len = 4 + portalBuf.length + 4;
    const buf = Buffer.alloc(1 + len);
    buf[0] = 0x45; // 'E'
    buf.writeInt32BE(len, 1);
    portalBuf.copy(buf, 5);
    buf.writeInt32BE(maxRows, 5 + portalBuf.length);
    this.socket.write(buf);
  }

  // Sync message
  _sendSync() {
    const buf = Buffer.alloc(5);
    buf[0] = 0x53; // 'S'
    buf.writeInt32BE(4, 1);
    this.socket.write(buf);
  }

  // Close message
  _sendClose(type, name) {
    const nameBuf = Buffer.from(name + '\0', 'utf8');
    const len = 4 + 1 + nameBuf.length;
    const buf = Buffer.alloc(1 + len);
    buf[0] = 0x43; // 'C' (not the CommandComplete 'C')
    buf.writeInt32BE(len, 1);
    buf[5] = type.charCodeAt(0);
    nameBuf.copy(buf, 6);
    this.socket.write(buf);
  }

  close() {
    if (this.socket) {
      const buf = Buffer.alloc(5);
      buf[0] = 0x58;
      buf.writeInt32BE(4, 1);
      this.socket.write(buf);
      this.socket.destroy();
    }
  }

  _hasReadyForQuery() {
    for (let i = 0; i < this.buffer.length; i++) {
      if (this.buffer[i] === 0x5A) return true;
    }
    return false;
  }

  _waitReady() {
    if (this._hasReadyForQuery()) return Promise.resolve(this._consumeMessages());
    return new Promise((resolve) => {
      this._resolve = resolve;
      setTimeout(() => { if (this._resolve === resolve) { this._resolve = null; resolve(this._consumeMessages()); } }, 3000);
    });
  }

  _consumeMessages() {
    const msgs = [];
    let offset = 0;
    while (offset < this.buffer.length) {
      if (offset + 5 > this.buffer.length) break;
      const type = String.fromCharCode(this.buffer[offset]);
      const len = this.buffer.readInt32BE(offset + 1);
      const totalLen = 1 + len;
      if (offset + totalLen > this.buffer.length) break;
      msgs.push({ type, body: this.buffer.subarray(offset + 1, offset + totalLen) });
      offset += totalLen;
    }
    this.buffer = this.buffer.subarray(offset);
    const result = { columns: [], rows: [], error: null, tag: null, txStatus: null, msgTypes: msgs.map(m => m.type) };
    for (const msg of msgs) {
      switch (msg.type) {
        case 'T': result.columns = this._parseColumns(msg.body); break;
        case 'D': result.rows.push(this._parseRow(msg.body)); break;
        case 'C': result.tag = msg.body.toString('utf8', 4).replace(/\0/g, ''); break;
        case 'E': { const m = msg.body.toString('utf8', 4).match(/M([^\0]+)/); result.error = m?.[1]; break; }
        case 'Z': result.txStatus = String.fromCharCode(msg.body[msg.body.length - 1]); break;
      }
    }
    return result;
  }

  _parseColumns(body) {
    const count = body.readInt16BE(4);
    const names = [];
    let off = 6;
    for (let i = 0; i < count; i++) {
      const nameEnd = body.indexOf(0, off);
      names.push(body.toString('utf8', off, nameEnd));
      off = nameEnd + 1 + 4 + 2 + 4 + 2 + 4 + 2;
    }
    return names;
  }

  _parseRow(body) {
    const count = body.readInt16BE(4);
    const values = [];
    let off = 6;
    for (let i = 0; i < count; i++) {
      const fieldLen = body.readInt32BE(off); off += 4;
      if (fieldLen === -1) { values.push(null); }
      else { values.push(body.toString('utf8', off, off + fieldLen)); off += fieldLen; }
    }
    return values;
  }
}

describe('Extended Query Protocol', () => {
  let server;
  const PORT = 15436;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('Parse → Bind → Execute → Sync for simple SELECT', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    // Setup
    await client.query('CREATE TABLE ext_users (id INTEGER, name TEXT, age INTEGER)');
    await client.query("INSERT INTO ext_users VALUES (1, 'Alice', 30)");
    await client.query("INSERT INTO ext_users VALUES (2, 'Bob', 25)");
    await client.query("INSERT INTO ext_users VALUES (3, 'Charlie', 35)");

    // Prepared query with parameter
    const r = await client.preparedQuery('SELECT name, age FROM ext_users WHERE id = $1', [2]);
    assert.ok(!r.error, `Extended query failed: ${r.error}`);
    assert.strictEqual(r.rows.length, 1);
    assert.ok(r.rows[0].includes('Bob'));

    // Check message flow includes ParseComplete(1), BindComplete(2)
    assert.ok(r.msgTypes.includes('1'), 'Missing ParseComplete');
    assert.ok(r.msgTypes.includes('2'), 'Missing BindComplete');

    client.close();
  });

  it('parameterized INSERT', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    const r = await client.preparedQuery("INSERT INTO ext_users VALUES ($1, $2, $3)", [4, 'Diana', 28]);
    assert.ok(!r.error, `Parameterized INSERT failed: ${r.error}`);

    // Verify
    const check = await client.query("SELECT name FROM ext_users WHERE id = 4");
    assert.ok(!check.error);
    assert.strictEqual(check.rows.length, 1);
    assert.ok(check.rows[0].includes('Diana'));

    client.close();
  });

  it('parameterized UPDATE', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    const r = await client.preparedQuery("UPDATE ext_users SET age = $1 WHERE id = $2", [31, 1]);
    assert.ok(!r.error, `Parameterized UPDATE failed: ${r.error}`);

    const check = await client.query("SELECT age FROM ext_users WHERE id = 1");
    assert.strictEqual(check.rows[0][check.columns.indexOf('age')], '31');

    client.close();
  });

  it('parameterized DELETE', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    const r = await client.preparedQuery("DELETE FROM ext_users WHERE id = $1", [4]);
    assert.ok(!r.error);

    const check = await client.query("SELECT COUNT(*) AS cnt FROM ext_users");
    assert.strictEqual(check.rows[0][0], '3');

    client.close();
  });

  it('named prepared statement reuse', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    // Parse once with a name
    client._sendParse('find_user', 'SELECT name FROM ext_users WHERE id = $1', []);
    // Bind with params
    client._sendBind('', 'find_user', [1]);
    client._sendExecute('', 0);
    client._sendSync();
    const r1 = await client._waitReady();
    assert.ok(!r1.error, `First exec failed: ${r1.error}`);
    assert.ok(r1.rows[0]?.includes('Alice'));

    // Reuse the same prepared statement with different params
    client._sendBind('', 'find_user', [3]);
    client._sendExecute('', 0);
    client._sendSync();
    const r2 = await client._waitReady();
    assert.ok(!r2.error, `Second exec failed: ${r2.error}`);
    assert.ok(r2.rows[0]?.includes('Charlie'));

    client.close();
  });

  it('Close prepared statement', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    // Parse
    client._sendParse('temp_stmt', 'SELECT 1 AS x', []);
    client._sendSync();
    const r1 = await client._waitReady();
    assert.ok(r1.msgTypes.includes('1'), 'Missing ParseComplete');

    // Close
    client._sendClose('S', 'temp_stmt');
    client._sendSync();
    const r2 = await client._waitReady();
    assert.ok(r2.msgTypes.includes('3'), 'Missing CloseComplete');

    // Try to bind to closed statement — should fail
    client._sendBind('', 'temp_stmt', []);
    client._sendSync();
    const r3 = await client._waitReady();
    assert.ok(r3.error, 'Should fail on closed statement');

    client.close();
  });

  it('multiple parameters in WHERE clause', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    const r = await client.preparedQuery(
      'SELECT name FROM ext_users WHERE age >= $1 AND age <= $2',
      [25, 31]
    );
    assert.ok(!r.error, `Multi-param query failed: ${r.error}`);
    assert.strictEqual(r.rows.length, 2); // Alice (31) and Bob (25)

    client.close();
  });

  it('NULL parameter handling', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    await client.query('CREATE TABLE ext_nullable (id INTEGER, val TEXT)');
    
    const r = await client.preparedQuery("INSERT INTO ext_nullable VALUES ($1, $2)", [1, null]);
    assert.ok(!r.error, `NULL insert failed: ${r.error}`);

    const check = await client.query("SELECT val FROM ext_nullable WHERE id = 1");
    assert.ok(!check.error);
    assert.strictEqual(check.rows[0][0], null);

    client.close();
  });

  it('aggregate with parameters', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    const r = await client.preparedQuery(
      'SELECT COUNT(*) AS cnt FROM ext_users WHERE age > $1',
      [28]
    );
    assert.ok(!r.error, `Aggregate failed: ${r.error}`);
    assert.strictEqual(r.rows.length, 1);
    // Alice (31) and Charlie (35) are > 28
    assert.strictEqual(r.rows[0][r.columns.indexOf('cnt')], '2');

    client.close();
  });

  it('error during extended query sends error and recovers on Sync', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    const r = await client.preparedQuery('SELECT * FROM nonexistent_ext', []);
    assert.ok(r.error, 'Expected error for missing table');

    // Should recover and accept next query
    const r2 = await client.query('SELECT 1 AS num');
    assert.ok(!r2.error, `Recovery query failed: ${r2.error}`);

    client.close();
  });

  it('mix of simple and extended queries', async () => {
    const client = new PgExtClient(PORT);
    await client.connect();

    // Simple query
    let r = await client.query('SELECT COUNT(*) AS c FROM ext_users');
    assert.ok(!r.error);
    assert.strictEqual(r.rows.length, 1);

    // Extended query
    r = await client.preparedQuery('SELECT name FROM ext_users WHERE id = $1', [2]);
    assert.ok(!r.error);
    assert.ok(r.rows[0]?.includes('Bob'));

    // Simple query again
    r = await client.query("SELECT name FROM ext_users WHERE name = 'Charlie'");
    assert.ok(!r.error);
    assert.strictEqual(r.rows.length, 1);

    client.close();
  });
});
