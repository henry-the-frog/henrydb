// copy-persistent.test.js — COPY bulk import with persistent storage + crash recovery
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import copyStreams from 'pg-copy-streams';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 29000 + Math.floor(Math.random() * 1000);
}

async function connect(port) {
  const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
  await c.connect();
  return c;
}

async function copyIn(client, table, rows) {
  const stream = client.query(copyStreams.from(`COPY ${table} FROM STDIN`));
  return new Promise((resolve, reject) => {
    const readable = new Readable({
      read() {
        if (rows.length > 0) {
          this.push(rows.shift() + '\n');
        } else {
          this.push(null);
        }
      }
    });
    readable.pipe(stream);
    stream.on('finish', resolve);
    stream.on('error', reject);
  });
}

describe('COPY with Persistent Storage', () => {
  it('COPY data survives server restart', async () => {
    const port = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-copy-persist-'));
    
    try {
      // Session 1: bulk import
      const s1 = new HenryDBServer({ port, dataDir: dir });
      await s1.start();
      const c1 = await connect(port);
      
      await c1.query('CREATE TABLE imported (id INT, name TEXT, val INT)');
      
      const rows = [];
      for (let i = 1; i <= 500; i++) rows.push(`${i}\trow-${i}\t${i * 10}`);
      await copyIn(c1, 'imported', rows);
      
      const r1 = await c1.query('SELECT COUNT(*) as cnt FROM imported');
      assert.equal(String(r1.rows[0].cnt), '500');
      
      await c1.end();
      await s1.stop();
      
      // Session 2: verify data survived
      const s2 = new HenryDBServer({ port, dataDir: dir });
      await s2.start();
      const c2 = await connect(port);
      
      const r2 = await c2.query('SELECT COUNT(*) as cnt FROM imported');
      assert.equal(String(r2.rows[0].cnt), '500');
      
      const r3 = await c2.query('SELECT val FROM imported WHERE id = 250');
      assert.equal(String(r3.rows[0].val), '2500');
      
      await c2.end();
      await s2.stop();
    } finally {
      rmSync(dir, { recursive: true });
    }
  });

  it('COPY + individual INSERT interleaved', async () => {
    const port = getPort();
    const server = new HenryDBServer({ port });
    await server.start();
    const c = await connect(port);
    
    await c.query('CREATE TABLE mixed (id INT, src TEXT, val INT)');
    
    // Individual inserts
    for (let i = 1; i <= 10; i++) {
      await c.query('INSERT INTO mixed VALUES ($1, $2, $3)', [i, 'insert', i * 100]);
    }
    
    // COPY bulk import
    const rows = [];
    for (let i = 11; i <= 30; i++) rows.push(`${i}\tcopy\t${i * 100}`);
    await copyIn(c, 'mixed', rows);
    
    // More individual inserts
    for (let i = 31; i <= 40; i++) {
      await c.query('INSERT INTO mixed VALUES ($1, $2, $3)', [i, 'insert2', i * 100]);
    }
    
    const r = await c.query('SELECT COUNT(*) as cnt FROM mixed');
    assert.equal(String(r.rows[0].cnt), '40');
    
    // Verify sources
    const inserts = await c.query("SELECT COUNT(*) as cnt FROM mixed WHERE src = 'insert' OR src = 'insert2'");
    assert.equal(String(inserts.rows[0].cnt), '20');
    
    const copies = await c.query("SELECT COUNT(*) as cnt FROM mixed WHERE src = 'copy'");
    assert.equal(String(copies.rows[0].cnt), '20');
    
    await c.end();
    await server.stop();
  });

  it('large COPY import (5000 rows)', async () => {
    const port = getPort();
    const server = new HenryDBServer({ port });
    await server.start();
    const c = await connect(port);
    
    await c.query('CREATE TABLE large (id INT, name TEXT, val INT)');
    
    const rows = [];
    for (let i = 1; i <= 5000; i++) rows.push(`${i}\tname-${i}\t${i * 7}`);
    await copyIn(c, 'large', rows);
    
    const r = await c.query('SELECT COUNT(*) as cnt FROM large');
    assert.equal(String(r.rows[0].cnt), '5000');
    
    // Aggregate query on bulk-imported data
    const r2 = await c.query('SELECT SUM(val) as total FROM large');
    const expected = 5000 * 5001 / 2 * 7;
    assert.equal(String(r2.rows[0].total), String(expected));
    
    await c.end();
    await server.stop();
  });

  it('multiple COPY operations on same table', async () => {
    const port = getPort();
    const server = new HenryDBServer({ port });
    await server.start();
    const c = await connect(port);
    
    await c.query('CREATE TABLE batched (id INT, batch INT, val TEXT)');
    
    for (let batch = 0; batch < 5; batch++) {
      const rows = [];
      for (let i = 0; i < 100; i++) {
        const id = batch * 100 + i + 1;
        rows.push(`${id}\t${batch}\tbatch-${batch}-row-${i}`);
      }
      await copyIn(c, 'batched', rows);
    }
    
    const r = await c.query('SELECT COUNT(*) as cnt FROM batched');
    assert.equal(String(r.rows[0].cnt), '500');
    
    const r2 = await c.query('SELECT batch, COUNT(*) as cnt FROM batched GROUP BY batch ORDER BY batch');
    assert.equal(r2.rows.length, 5);
    for (const row of r2.rows) {
      assert.equal(String(row.cnt), '100');
    }
    
    await c.end();
    await server.stop();
  });
});
