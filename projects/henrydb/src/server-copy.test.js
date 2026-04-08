// server-copy.test.js — Tests for COPY protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import pg from 'pg';
import { Readable } from 'node:stream';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15491;

// Helper: Low-level COPY client (pg driver's copy support is complex,
// so we test with raw protocol for COPY IN, and pg for queries)
class CopyClient {
  constructor(port) { this.port = port; this.socket = null; this.buffer = Buffer.alloc(0); this._resolve = null; }

  async connect() {
    this.socket = await new Promise((resolve, reject) => {
      const s = net.createConnection({ host: '127.0.0.1', port: this.port }, () => resolve(s));
      s.on('error', reject);
    });
    this.socket.on('data', (data) => {
      this.buffer = Buffer.concat([this.buffer, data]);
      if (this._resolve && this._hasMessage()) { const r = this._resolve; this._resolve = null; r(this._consume()); }
    });
    const params = `user\0test\0database\0test\0\0`;
    const p = Buffer.from(params, 'utf8');
    const len = 4 + 4 + p.length;
    const buf = Buffer.alloc(len);
    buf.writeInt32BE(len, 0);
    buf.writeInt32BE(196608, 4);
    p.copy(buf, 8);
    this.socket.write(buf);
    await this._wait();
  }

  async query(sql) {
    const b = Buffer.from(sql + '\0', 'utf8');
    const l = 4 + b.length;
    const buf = Buffer.alloc(1 + l);
    buf[0] = 0x51;
    buf.writeInt32BE(l, 1);
    b.copy(buf, 5);
    this.socket.write(buf);
    return this._wait();
  }

  // Send COPY data (tab-separated)
  sendCopyData(data) {
    const d = Buffer.from(data, 'utf8');
    const l = 4 + d.length;
    const buf = Buffer.alloc(1 + l);
    buf[0] = 0x64; // 'd'
    buf.writeInt32BE(l, 1);
    d.copy(buf, 5);
    this.socket.write(buf);
  }

  // Signal end of COPY
  async sendCopyDone() {
    const buf = Buffer.alloc(5);
    buf[0] = 0x63; // 'c'
    buf.writeInt32BE(4, 1);
    this.socket.write(buf);
    return this._wait();
  }

  // Signal COPY failure
  async sendCopyFail(message) {
    const m = Buffer.from(message + '\0', 'utf8');
    const l = 4 + m.length;
    const buf = Buffer.alloc(1 + l);
    buf[0] = 0x66; // 'f'
    buf.writeInt32BE(l, 1);
    m.copy(buf, 5);
    this.socket.write(buf);
    return this._wait();
  }

  close() {
    if (this.socket) {
      const buf = Buffer.alloc(5); buf[0] = 0x58; buf.writeInt32BE(4, 1);
      this.socket.write(buf); this.socket.destroy();
    }
  }

  _hasMessage() {
    for (let i = 0; i < this.buffer.length; i++) if (this.buffer[i] === 0x5A) return true;
    // Also check for CopyInResponse (G) or CopyOutResponse (H)
    for (let i = 0; i < this.buffer.length; i++) if (this.buffer[i] === 0x47 || this.buffer[i] === 0x48) return true;
    return false;
  }

  _wait() {
    if (this._hasMessage()) return Promise.resolve(this._consume());
    return new Promise(r => { this._resolve = r; setTimeout(() => { if (this._resolve === r) { this._resolve = null; r(this._consume()); } }, 3000); });
  }

  _consume() {
    const msgs = [];
    let offset = 0;
    while (offset < this.buffer.length && offset + 5 <= this.buffer.length) {
      const type = String.fromCharCode(this.buffer[offset]);
      const len = this.buffer.readInt32BE(offset + 1);
      const totalLen = 1 + len;
      if (offset + totalLen > this.buffer.length) break;
      msgs.push({ type, body: this.buffer.subarray(offset + 1, offset + totalLen) });
      offset += totalLen;
    }
    this.buffer = this.buffer.subarray(offset);
    const result = { msgTypes: msgs.map(m => m.type), error: null, tag: null, copyData: [] };
    for (const msg of msgs) {
      switch (msg.type) {
        case 'C': result.tag = msg.body.toString('utf8', 4).replace(/\0/g, ''); break;
        case 'E': { const m = msg.body.toString('utf8', 4).match(/M([^\0]+)/); result.error = m?.[1]; break; }
        case 'd': result.copyData.push(msg.body.toString('utf8', 4)); break;
      }
    }
    return result;
  }
}

describe('COPY Protocol', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('COPY FROM STDIN — basic bulk load', async () => {
    const client = new CopyClient(PORT);
    await client.connect();

    await client.query('CREATE TABLE copy_test (id INTEGER, name TEXT, score INTEGER)');

    // Start COPY
    const resp = await client.query('COPY copy_test FROM STDIN');
    assert.ok(resp.msgTypes.includes('G'), 'Expected CopyInResponse');

    // Send data (tab-separated)
    client.sendCopyData('1\tAlice\t95\n');
    client.sendCopyData('2\tBob\t87\n');
    client.sendCopyData('3\tCharlie\t92\n');

    // End COPY
    const done = await client.sendCopyDone();
    assert.ok(done.tag?.startsWith('COPY'), `Expected COPY tag, got: ${done.tag}`);

    // Verify data
    const pgClient = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await pgClient.connect();
    const result = await pgClient.query('SELECT * FROM copy_test ORDER BY id');
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(result.rows[0].score, 95);
    assert.strictEqual(result.rows[2].name, 'Charlie');
    await pgClient.end();

    client.close();
  });

  it('COPY FROM STDIN — large batch', async () => {
    const client = new CopyClient(PORT);
    await client.connect();

    await client.query('CREATE TABLE bulk_load (id INTEGER, val TEXT)');
    const resp = await client.query('COPY bulk_load FROM STDIN');
    assert.ok(resp.msgTypes.includes('G'));

    // Send 1000 rows
    let batch = '';
    for (let i = 0; i < 1000; i++) {
      batch += `${i}\tvalue_${i}\n`;
    }
    client.sendCopyData(batch);
    const done = await client.sendCopyDone();
    assert.ok(done.tag?.includes('1000'), `Expected 1000 rows, got: ${done.tag}`);

    // Verify
    const pgClient = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await pgClient.connect();
    const result = await pgClient.query('SELECT COUNT(*) AS cnt FROM bulk_load');
    assert.strictEqual(parseInt(result.rows[0].cnt), 1000);
    await pgClient.end();

    client.close();
  });

  it('COPY FROM STDIN — NULL handling', async () => {
    const client = new CopyClient(PORT);
    await client.connect();

    await client.query('CREATE TABLE null_copy (id INTEGER, val TEXT, num INTEGER)');
    const resp = await client.query('COPY null_copy FROM STDIN');
    assert.ok(resp.msgTypes.includes('G'));

    client.sendCopyData('1\thello\t42\n');
    client.sendCopyData('2\t\\N\t\\N\n'); // NULL values
    client.sendCopyData('3\tworld\t99\n');
    const done = await client.sendCopyDone();
    assert.ok(done.tag?.includes('3'));

    const pgClient = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await pgClient.connect();
    const result = await pgClient.query('SELECT * FROM null_copy WHERE id = $1', [2]);
    assert.strictEqual(result.rows[0].val, null);
    assert.strictEqual(result.rows[0].num, null);
    await pgClient.end();

    client.close();
  });

  it('COPY TO STDOUT — export data', async () => {
    const client = new CopyClient(PORT);
    await client.connect();

    // query returns CopyOutResponse + CopyData messages + CopyDone
    const resp = await client.query('COPY copy_test TO STDOUT');

    // Should have 'H' (CopyOutResponse), 'd' (CopyData), 'c' (CopyDone), 'C' (CommandComplete), 'Z' (ReadyForQuery)
    assert.ok(resp.msgTypes.includes('H'), `Expected CopyOutResponse, got: ${resp.msgTypes}`);
    assert.ok(resp.copyData.length > 0, 'Expected COPY data');
    assert.ok(resp.tag?.startsWith('COPY'), `Expected COPY tag, got: ${resp.tag}`);

    // Verify the data is tab-separated
    const firstLine = resp.copyData[0].split('\n')[0];
    assert.ok(firstLine.includes('\t'), 'Expected tab-separated data');

    client.close();
  });

  it('COPY FAIL — abort mid-copy', async () => {
    const client = new CopyClient(PORT);
    await client.connect();

    await client.query('CREATE TABLE fail_copy (id INTEGER, val TEXT)');
    const resp = await client.query('COPY fail_copy FROM STDIN');
    assert.ok(resp.msgTypes.includes('G'));

    // Send some data then fail
    client.sendCopyData('1\tgood\n');
    const fail = await client.sendCopyFail('Client cancelled');
    assert.ok(fail.error, 'Expected error response');

    // Table should be empty (copy was aborted)
    const pgClient = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await pgClient.connect();
    const result = await pgClient.query('SELECT COUNT(*) AS cnt FROM fail_copy');
    assert.strictEqual(parseInt(result.rows[0].cnt), 0);
    await pgClient.end();

    client.close();
  });

  it('COPY to nonexistent table returns error', async () => {
    const client = new CopyClient(PORT);
    await client.connect();

    const resp = await client.query('COPY nonexistent_table FROM STDIN');
    // Should get an error, not CopyInResponse
    assert.ok(resp.error || !resp.msgTypes.includes('G'), 'Expected error for nonexistent table');

    client.close();
  });

  it('regular queries work after COPY', async () => {
    const client = new CopyClient(PORT);
    await client.connect();

    await client.query('CREATE TABLE post_copy (id INTEGER)');
    const resp = await client.query('COPY post_copy FROM STDIN');
    client.sendCopyData('1\n2\n3\n');
    await client.sendCopyDone();

    // Regular query should still work
    const result = await client.query('SELECT COUNT(*) AS cnt FROM post_copy');
    // Verify result
    assert.ok(!result.error);

    client.close();
  });
});
