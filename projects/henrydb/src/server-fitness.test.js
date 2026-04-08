// server-fitness.test.js — Fitness tracking data model
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15585;

describe('Fitness Tracking', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE workouts (id INTEGER, user_name TEXT, type TEXT, duration_min INTEGER, calories INTEGER, date TEXT)');
    await client.query('CREATE TABLE daily_steps (id INTEGER, user_name TEXT, steps INTEGER, date TEXT)');
    
    // Workouts
    const types = ['running', 'cycling', 'swimming', 'weights'];
    for (let i = 1; i <= 30; i++) {
      const user = ['alice', 'bob'][i % 2];
      const type = types[i % 4];
      const dur = 20 + (i % 60);
      const cal = dur * (type === 'running' ? 10 : type === 'cycling' ? 8 : type === 'swimming' ? 12 : 6);
      const day = String(1 + (i % 7)).padStart(2, '0');
      await client.query(`INSERT INTO workouts VALUES (${i}, '${user}', '${type}', ${dur}, ${cal}, '2026-04-${day}')`);
    }
    
    // Daily steps
    for (let i = 1; i <= 14; i++) {
      const day = String(1 + (i % 7)).padStart(2, '0');
      await client.query(`INSERT INTO daily_steps VALUES (${i}, 'alice', ${5000 + i * 500}, '2026-04-${day}')`);
      await client.query(`INSERT INTO daily_steps VALUES (${i + 14}, 'bob', ${3000 + i * 400}, '2026-04-${day}')`);
    }
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('weekly workout summary', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT user_name, COUNT(*) AS workouts, SUM(duration_min) AS total_min, SUM(calories) AS total_cal FROM workouts WHERE user_name = 'alice' GROUP BY user_name"
    );
    assert.strictEqual(result.rows.length, 1);
    assert.ok(parseInt(result.rows[0].workouts) > 0);

    await client.end();
  });

  it('calories by workout type', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT type, SUM(calories) AS total_cal, COUNT(*) AS sessions FROM workouts GROUP BY type ORDER BY total_cal DESC'
    );
    assert.strictEqual(result.rows.length, 4);

    await client.end();
  });

  it('step count leaderboard', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT user_name, SUM(steps) AS total_steps, AVG(steps) AS avg_steps FROM daily_steps GROUP BY user_name ORDER BY total_steps DESC'
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('personal records', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT type, MAX(duration_min) AS longest, MAX(calories) AS most_cal FROM workouts WHERE user_name = 'alice' GROUP BY type"
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('daily activity summary', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT date, SUM(steps) AS total_steps FROM daily_steps WHERE user_name = 'alice' GROUP BY date ORDER BY date"
    );
    assert.ok(result.rows.length >= 5);

    await client.end();
  });
});
