// cursor-explore.test.js — Testing DECLARE CURSOR / FETCH / CLOSE
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 30000 + Math.floor(Math.random() * 10000);
}

describe('Cursors', () => {
  let server, port, dir;
  
  before(async () => {
    port = getPort();
    dir = mkdtempSync(join(tmpdir(), 'henrydb-cursor-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
    
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    await client.query("CREATE TABLE data (id INT, name TEXT)");
    for (let i = 1; i <= 20; i++) {
      await client.query(`INSERT INTO data VALUES (${i}, 'row-${i}')`);
    }
    await client.end();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('DECLARE CURSOR and FETCH', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('BEGIN');
    await client.query('DECLARE my_cursor CURSOR FOR SELECT * FROM data ORDER BY id');
    
    // Fetch first 5
    const batch1 = await client.query('FETCH 5 FROM my_cursor');
    console.log('Batch 1:', batch1.rows.map(r => r.id));
    assert.equal(batch1.rows.length, 5);
    assert.equal(String(batch1.rows[0].id), '1');
    assert.equal(String(batch1.rows[4].id), '5');
    
    // Fetch next 5
    const batch2 = await client.query('FETCH 5 FROM my_cursor');
    console.log('Batch 2:', batch2.rows.map(r => r.id));
    assert.equal(batch2.rows.length, 5);
    assert.equal(String(batch2.rows[0].id), '6');
    
    await client.query('CLOSE my_cursor');
    await client.query('COMMIT');
    await client.end();
  });

  it('FETCH ALL remaining rows', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('BEGIN');
    await client.query('DECLARE cur CURSOR FOR SELECT * FROM data ORDER BY id');
    
    // Fetch first 3
    await client.query('FETCH 3 FROM cur');
    
    // Fetch all remaining
    const remaining = await client.query('FETCH ALL FROM cur');
    console.log('Remaining after 3:', remaining.rows.length);
    assert.equal(remaining.rows.length, 17); // 20 - 3
    assert.equal(String(remaining.rows[0].id), '4');
    
    await client.query('CLOSE cur');
    await client.query('COMMIT');
    await client.end();
  });

  it('FETCH NEXT (one at a time)', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('BEGIN');
    await client.query('DECLARE one_cursor CURSOR FOR SELECT name FROM data ORDER BY id');
    
    const row1 = await client.query('FETCH NEXT FROM one_cursor');
    assert.equal(row1.rows.length, 1);
    assert.equal(row1.rows[0].name, 'row-1');
    
    const row2 = await client.query('FETCH NEXT FROM one_cursor');
    assert.equal(row2.rows[0].name, 'row-2');
    
    await client.query('CLOSE one_cursor');
    await client.query('COMMIT');
    await client.end();
  });

  it('FETCH past end returns empty', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('BEGIN');
    await client.query('DECLARE small_cur CURSOR FOR SELECT * FROM data WHERE id <= 3 ORDER BY id');
    
    const all = await client.query('FETCH ALL FROM small_cur');
    assert.equal(all.rows.length, 3);
    
    // Fetch again — should be empty
    const empty = await client.query('FETCH 5 FROM small_cur');
    assert.equal(empty.rows.length, 0);
    
    await client.query('CLOSE small_cur');
    await client.query('COMMIT');
    await client.end();
  });

  it('multiple cursors simultaneously', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('BEGIN');
    await client.query('DECLARE cur_a CURSOR FOR SELECT * FROM data WHERE id <= 10 ORDER BY id');
    await client.query('DECLARE cur_b CURSOR FOR SELECT * FROM data WHERE id > 10 ORDER BY id');
    
    const a = await client.query('FETCH 3 FROM cur_a');
    const b = await client.query('FETCH 3 FROM cur_b');
    
    console.log('Cursor A:', a.rows.map(r => r.id));
    console.log('Cursor B:', b.rows.map(r => r.id));
    
    assert.equal(String(a.rows[0].id), '1');
    assert.equal(String(b.rows[0].id), '11');
    
    await client.query('CLOSE cur_a');
    await client.query('CLOSE cur_b');
    await client.query('COMMIT');
    await client.end();
  });
});
