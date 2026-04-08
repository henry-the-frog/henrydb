// server-voting.test.js — Election/voting data model
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15589;

describe('Voting/Election System', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE candidates (id INTEGER, name TEXT, party TEXT)');
    await client.query('CREATE TABLE districts (id INTEGER, name TEXT, registered_voters INTEGER)');
    await client.query('CREATE TABLE votes (id INTEGER, candidate_id INTEGER, district_id INTEGER, count INTEGER)');
    
    await client.query("INSERT INTO candidates VALUES (1, 'Alice Adams', 'Blue')");
    await client.query("INSERT INTO candidates VALUES (2, 'Bob Brown', 'Red')");
    await client.query("INSERT INTO candidates VALUES (3, 'Charlie Clark', 'Green')");
    
    await client.query("INSERT INTO districts VALUES (1, 'North', 50000)");
    await client.query("INSERT INTO districts VALUES (2, 'South', 75000)");
    await client.query("INSERT INTO districts VALUES (3, 'East', 60000)");
    
    await client.query('INSERT INTO votes VALUES (1, 1, 1, 18000)');
    await client.query('INSERT INTO votes VALUES (2, 2, 1, 15000)');
    await client.query('INSERT INTO votes VALUES (3, 3, 1, 5000)');
    await client.query('INSERT INTO votes VALUES (4, 1, 2, 25000)');
    await client.query('INSERT INTO votes VALUES (5, 2, 2, 30000)');
    await client.query('INSERT INTO votes VALUES (6, 3, 2, 8000)');
    await client.query('INSERT INTO votes VALUES (7, 1, 3, 22000)');
    await client.query('INSERT INTO votes VALUES (8, 2, 3, 20000)');
    await client.query('INSERT INTO votes VALUES (9, 3, 3, 10000)');
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('total votes by candidate', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT c.name, c.party, SUM(v.count) AS total_votes FROM candidates c JOIN votes v ON c.id = v.candidate_id GROUP BY c.name, c.party ORDER BY total_votes DESC'
    );
    assert.strictEqual(result.rows.length, 3);

    await client.end();
  });

  it('district results', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT d.name AS district, c.name AS candidate, v.count FROM votes v JOIN candidates c ON v.candidate_id = c.id JOIN districts d ON v.district_id = d.id ORDER BY d.name, v.count DESC'
    );
    assert.strictEqual(result.rows.length, 9);

    await client.end();
  });

  it('voter turnout by district', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT d.name, d.registered_voters, SUM(v.count) AS total_votes FROM districts d JOIN votes v ON d.id = v.district_id GROUP BY d.name, d.registered_voters'
    );
    assert.strictEqual(result.rows.length, 3);

    await client.end();
  });

  it('winner per district', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Find max votes per district
    const result = await client.query(
      'SELECT d.name AS district, c.name AS winner, v.count FROM votes v JOIN candidates c ON v.candidate_id = c.id JOIN districts d ON v.district_id = d.id ORDER BY d.name, v.count DESC'
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });
});
