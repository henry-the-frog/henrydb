// copy-explore.test.js — Testing COPY FROM STDIN for bulk data loading
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import copyStreams from 'pg-copy-streams';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const { from: copyFrom } = copyStreams;

function getPort() {
  return 28000 + Math.floor(Math.random() * 10000);
}

describe('COPY FROM STDIN', () => {
  let server, port, dir;
  
  before(async () => {
    port = getPort();
    dir = mkdtempSync(join(tmpdir(), 'henrydb-copy-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('COPY FROM STDIN with TSV data', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query("CREATE TABLE bulk (id INT, name TEXT, score INT)");
    
    const stream = client.query(copyFrom('COPY bulk FROM STDIN'));
    
    const rows = [];
    for (let i = 0; i < 100; i++) {
      rows.push(`${i}\tname-${i}\t${i * 10}`);
    }
    stream.write(rows.join('\n') + '\n');
    
    await new Promise((resolve, reject) => {
      stream.end();
      stream.on('finish', resolve);
      stream.on('error', reject);
    });
    
    const count = await client.query('SELECT COUNT(*) as n FROM bulk');
    console.log('COPY rows:', count.rows[0].n);
    assert.equal(String(count.rows[0].n), '100');
    
    // Spot check
    const check = await client.query("SELECT * FROM bulk WHERE id = 50");
    assert.equal(check.rows.length, 1);
    assert.equal(check.rows[0].name, 'name-50');
    assert.equal(String(check.rows[0].score), '500');
    
    await client.end();
  });

  it('COPY performance vs INSERT (1000 rows)', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    // COPY test
    await client.query("CREATE TABLE copy_perf (id INT, data TEXT, val INT)");
    
    const copyStart = performance.now();
    const stream = client.query(copyFrom('COPY copy_perf FROM STDIN'));
    
    const lines = [];
    for (let i = 0; i < 1000; i++) {
      lines.push(`${i}\tdata-${i}\t${i * 10}`);
    }
    stream.write(lines.join('\n') + '\n');
    
    await new Promise((resolve, reject) => {
      stream.end();
      stream.on('finish', resolve);
      stream.on('error', reject);
    });
    const copyElapsed = performance.now() - copyStart;
    
    // INSERT test
    await client.query("CREATE TABLE insert_perf (id INT, data TEXT, val INT)");
    
    const insertStart = performance.now();
    await client.query('BEGIN');
    for (let i = 0; i < 1000; i++) {
      await client.query('INSERT INTO insert_perf VALUES ($1, $2, $3)', [i, `data-${i}`, i * 10]);
    }
    await client.query('COMMIT');
    const insertElapsed = performance.now() - insertStart;
    
    console.log(`COPY:   1000 rows in ${copyElapsed.toFixed(1)}ms (${(copyElapsed/1000).toFixed(3)}ms/row)`);
    console.log(`INSERT: 1000 rows in ${insertElapsed.toFixed(1)}ms (${(insertElapsed/1000).toFixed(3)}ms/row)`);
    console.log(`Speedup: ${(insertElapsed/copyElapsed).toFixed(1)}x`);
    
    const copyCount = await client.query('SELECT COUNT(*) as n FROM copy_perf');
    const insertCount = await client.query('SELECT COUNT(*) as n FROM insert_perf');
    assert.equal(String(copyCount.rows[0].n), '1000');
    assert.equal(String(insertCount.rows[0].n), '1000');
    
    await client.end();
  });

  it('large COPY (5000 rows)', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query("CREATE TABLE large_copy (id INT, payload TEXT)");
    
    const start = performance.now();
    const stream = client.query(copyFrom('COPY large_copy FROM STDIN'));
    
    for (let chunk = 0; chunk < 50; chunk++) {
      const lines = [];
      for (let i = 0; i < 100; i++) {
        const id = chunk * 100 + i;
        lines.push(`${id}\t${'x'.repeat(100)}`);
      }
      stream.write(lines.join('\n') + '\n');
    }
    
    await new Promise((resolve, reject) => {
      stream.end();
      stream.on('finish', resolve);
      stream.on('error', reject);
    });
    const elapsed = performance.now() - start;
    
    const count = await client.query('SELECT COUNT(*) as n FROM large_copy');
    console.log(`Large COPY: 5000 rows in ${elapsed.toFixed(1)}ms (${(elapsed/5000).toFixed(3)}ms/row)`);
    assert.equal(String(count.rows[0].n), '5000');
    
    await client.end();
  });

  it('COPY NULL handling', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query("CREATE TABLE nulls (id INT, name TEXT, age INT)");
    
    const stream = client.query(copyFrom('COPY nulls FROM STDIN'));
    // \N is PostgreSQL's NULL marker in COPY format
    stream.write('1\tAlice\t30\n2\t\\N\t25\n3\tCharlie\t\\N\n');
    
    await new Promise((resolve, reject) => {
      stream.end();
      stream.on('finish', resolve);
      stream.on('error', reject);
    });
    
    const result = await client.query('SELECT * FROM nulls ORDER BY id');
    console.log('NULL handling:', result.rows);
    assert.equal(result.rows.length, 3);
    assert.equal(result.rows[0].name, 'Alice');
    // Row 2 should have NULL name, row 3 should have NULL age
    assert.equal(result.rows[1].name, null);
    assert.equal(result.rows[2].age, null);
    
    await client.end();
  });
});
