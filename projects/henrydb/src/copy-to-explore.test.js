// copy-to-explore.test.js — Testing COPY TO STDOUT
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import copyStreams from 'pg-copy-streams';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const { to: copyTo, from: copyFrom } = copyStreams;

function getPort() {
  return 29000 + Math.floor(Math.random() * 10000);
}

describe('COPY TO STDOUT', () => {
  let server, port, dir;
  
  before(async () => {
    port = getPort();
    dir = mkdtempSync(join(tmpdir(), 'henrydb-copyto-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
    
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    await client.query("CREATE TABLE export_test (id INT, name TEXT, score INT)");
    await client.query("INSERT INTO export_test VALUES (1, 'Alice', 95)");
    await client.query("INSERT INTO export_test VALUES (2, 'Bob', 87)");
    await client.query("INSERT INTO export_test VALUES (3, 'Charlie', 92)");
    await client.end();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('COPY TO STDOUT exports TSV data', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    const stream = client.query(copyTo('COPY export_test TO STDOUT'));
    
    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    
    const data = Buffer.concat(chunks).toString('utf-8');
    console.log('Exported TSV:\n' + data);
    
    const lines = data.trim().split('\n');
    assert.equal(lines.length, 3, 'Should have 3 rows');
    
    // Verify TSV format
    const firstRow = lines[0].split('\t');
    assert.equal(firstRow.length, 3, 'Should have 3 columns');
    
    await client.end();
  });

  it('round-trip: COPY TO then COPY FROM', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    // Export
    const exportStream = client.query(copyTo('COPY export_test TO STDOUT'));
    const chunks = [];
    for await (const chunk of exportStream) {
      chunks.push(chunk);
    }
    const tsv = Buffer.concat(chunks).toString('utf-8');
    
    // Create new table with same schema
    await client.query("CREATE TABLE reimported (id INT, name TEXT, score INT)");
    
    // Import
    const importStream = client.query(copyFrom('COPY reimported FROM STDIN'));
    importStream.write(tsv);
    await new Promise((resolve, reject) => {
      importStream.end();
      importStream.on('finish', resolve);
      importStream.on('error', reject);
    });
    
    // Verify
    const original = await client.query('SELECT * FROM export_test ORDER BY id');
    const reimported = await client.query('SELECT * FROM reimported ORDER BY id');
    
    assert.equal(reimported.rows.length, original.rows.length);
    for (let i = 0; i < original.rows.length; i++) {
      assert.equal(reimported.rows[i].name, original.rows[i].name);
      assert.equal(String(reimported.rows[i].score), String(original.rows[i].score));
    }
    console.log('Round-trip: ✅ Data matches');
    
    await client.end();
  });

  it('COPY specific columns', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    const stream = client.query(copyTo('COPY export_test (name, score) TO STDOUT'));
    
    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    
    const data = Buffer.concat(chunks).toString('utf-8');
    console.log('Specific columns:\n' + data);
    
    const lines = data.trim().split('\n');
    const firstRow = lines[0].split('\t');
    assert.equal(firstRow.length, 2, 'Should have 2 columns (name, score)');
    
    await client.end();
  });

  it('COPY TO performance (1000 rows)', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query("CREATE TABLE big_export (id INT, data TEXT)");
    
    // Bulk load
    const importStream = client.query(copyFrom('COPY big_export FROM STDIN'));
    const lines = [];
    for (let i = 0; i < 1000; i++) {
      lines.push(`${i}\tdata-${i}`);
    }
    importStream.write(lines.join('\n') + '\n');
    await new Promise((resolve, reject) => {
      importStream.end();
      importStream.on('finish', resolve);
      importStream.on('error', reject);
    });
    
    // Export timing
    const start = performance.now();
    const stream = client.query(copyTo('COPY big_export TO STDOUT'));
    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    const elapsed = performance.now() - start;
    
    const data = Buffer.concat(chunks).toString('utf-8');
    const rowCount = data.trim().split('\n').length;
    
    console.log(`COPY TO: ${rowCount} rows in ${elapsed.toFixed(1)}ms (${(elapsed/rowCount).toFixed(3)}ms/row)`);
    assert.equal(rowCount, 1000);
    
    await client.end();
  });
});
