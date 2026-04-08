// server-cursor.test.js — Tests for server-side cursors
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15492;

describe('Server-Side Cursors', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    await client.query('CREATE TABLE cursor_data (id INTEGER, name TEXT, score INTEGER)');
    for (let i = 1; i <= 100; i++) {
      await client.query(`INSERT INTO cursor_data VALUES (${i}, 'item_${i}', ${i * 10})`);
    }
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('DECLARE + FETCH ALL', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('DECLARE my_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id');
    const result = await client.query('FETCH ALL FROM my_cursor');
    assert.strictEqual(result.rows.length, 100);
    assert.strictEqual(result.rows[0].id, 1);
    assert.strictEqual(result.rows[99].id, 100);

    await client.query('CLOSE my_cursor');
    await client.end();
  });

  it('FETCH N rows at a time', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('DECLARE batch_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id');

    // Fetch 10 rows
    const r1 = await client.query('FETCH 10 FROM batch_cursor');
    assert.strictEqual(r1.rows.length, 10);
    assert.strictEqual(r1.rows[0].id, 1);
    assert.strictEqual(r1.rows[9].id, 10);

    // Fetch next 10
    const r2 = await client.query('FETCH 10 FROM batch_cursor');
    assert.strictEqual(r2.rows.length, 10);
    assert.strictEqual(r2.rows[0].id, 11);
    assert.strictEqual(r2.rows[9].id, 20);

    // Fetch next 5
    const r3 = await client.query('FETCH 5 FROM batch_cursor');
    assert.strictEqual(r3.rows.length, 5);
    assert.strictEqual(r3.rows[0].id, 21);

    await client.query('CLOSE batch_cursor');
    await client.end();
  });

  it('FETCH NEXT (single row)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('DECLARE one_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id');

    const r1 = await client.query('FETCH NEXT FROM one_cursor');
    assert.strictEqual(r1.rows.length, 1);
    assert.strictEqual(r1.rows[0].id, 1);

    const r2 = await client.query('FETCH NEXT FROM one_cursor');
    assert.strictEqual(r2.rows.length, 1);
    assert.strictEqual(r2.rows[0].id, 2);

    await client.query('CLOSE one_cursor');
    await client.end();
  });

  it('FETCH past end returns empty', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('DECLARE small_cursor CURSOR FOR SELECT * FROM cursor_data WHERE id <= 3 ORDER BY id');

    await client.query('FETCH ALL FROM small_cursor');
    const empty = await client.query('FETCH 10 FROM small_cursor');
    assert.strictEqual(empty.rows.length, 0);

    await client.query('CLOSE small_cursor');
    await client.end();
  });

  it('MOVE skips rows', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('DECLARE move_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id');

    // Move past first 50 rows
    await client.query('MOVE 50 FROM move_cursor');

    // Fetch should start from row 51
    const result = await client.query('FETCH 5 FROM move_cursor');
    assert.strictEqual(result.rows.length, 5);
    assert.strictEqual(result.rows[0].id, 51);

    await client.query('CLOSE move_cursor');
    await client.end();
  });

  it('CLOSE ALL closes all cursors', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('DECLARE c1 CURSOR FOR SELECT 1');
    await client.query('DECLARE c2 CURSOR FOR SELECT 2');
    await client.query('DECLARE c3 CURSOR FOR SELECT 3');

    await client.query('CLOSE ALL');

    // Fetching from closed cursor should fail
    try {
      await client.query('FETCH ALL FROM c1');
      assert.fail('Should have thrown');
    } catch (e) {
      assert.ok(e.message.includes('not found') || e.message.includes('Cursor'));
    }

    await client.end();
  });

  it('cursor with WHERE clause and aggregation-free query', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('DECLARE filtered CURSOR FOR SELECT name, score FROM cursor_data WHERE score > 500 ORDER BY score');
    const result = await client.query('FETCH 10 FROM filtered');
    assert.strictEqual(result.rows.length, 10);
    assert.ok(result.rows[0].score > 500);

    await client.query('CLOSE filtered');
    await client.end();
  });

  it('multiple concurrent cursors', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('DECLARE cur_a CURSOR FOR SELECT * FROM cursor_data WHERE id <= 10 ORDER BY id');
    await client.query('DECLARE cur_b CURSOR FOR SELECT * FROM cursor_data WHERE id > 90 ORDER BY id');

    const a = await client.query('FETCH 5 FROM cur_a');
    const b = await client.query('FETCH 5 FROM cur_b');

    assert.strictEqual(a.rows[0].id, 1);
    assert.strictEqual(b.rows[0].id, 91);

    // Fetch more from A
    const a2 = await client.query('FETCH 5 FROM cur_a');
    assert.strictEqual(a2.rows[0].id, 6);

    // Fetch more from B
    const b2 = await client.query('FETCH 5 FROM cur_b');
    assert.strictEqual(b2.rows[0].id, 96);

    await client.query('CLOSE cur_a');
    await client.query('CLOSE cur_b');
    await client.end();
  });

  it('cursor with FORWARD keyword', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('DECLARE fwd_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id');
    const result = await client.query('FETCH FORWARD 3 FROM fwd_cursor');
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].id, 1);
    assert.strictEqual(result.rows[2].id, 3);

    await client.query('CLOSE fwd_cursor');
    await client.end();
  });
});
