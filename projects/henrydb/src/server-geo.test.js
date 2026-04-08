// server-geo.test.js — Geospatial-like queries through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15525;

describe('Geospatial Queries', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE places (id INTEGER, name TEXT, lat REAL, lng REAL, category TEXT)');
    
    // Denver area locations
    await client.query("INSERT INTO places VALUES (1, 'Union Station', 39.7525, -105.0000, 'transit')");
    await client.query("INSERT INTO places VALUES (2, 'Red Rocks', 39.6636, -105.2053, 'entertainment')");
    await client.query("INSERT INTO places VALUES (3, 'Denver Zoo', 39.7497, -104.9509, 'attraction')");
    await client.query("INSERT INTO places VALUES (4, 'Coors Field', 39.7559, -104.9942, 'sports')");
    await client.query("INSERT INTO places VALUES (5, 'Cherry Creek Mall', 39.7164, -104.9553, 'shopping')");
    await client.query("INSERT INTO places VALUES (6, 'DIA Airport', 39.8561, -104.6737, 'transit')");
    await client.query("INSERT INTO places VALUES (7, 'Colorado Convention Center', 39.7431, -104.9958, 'venue')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('find places within bounding box', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Downtown Denver bounding box
    const result = await client.query(
      'SELECT name FROM places WHERE lat > 39.73 AND lat < 39.77 AND lng > -105.01 AND lng < -104.94'
    );
    assert.ok(result.rows.length >= 3); // Union Station, Zoo, Coors Field area

    await client.end();
  });

  it('filter by category', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT name FROM places WHERE category = 'transit'");
    assert.strictEqual(result.rows.length, 2); // Union Station, DIA

    await client.end();
  });

  it('approximate distance ordering', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Simple ordering by latitude distance from downtown (39.75)
    const result = await client.query(`
      SELECT name, ABS(lat - 39.75) AS lat_dist
      FROM places 
      ORDER BY lat_dist
    `);
    assert.ok(result.rows.length >= 5);

    await client.end();
  });

  it('count by category', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT category, COUNT(*) AS cnt FROM places GROUP BY category ORDER BY cnt DESC'
    );
    assert.ok(result.rows.length >= 4);

    await client.end();
  });

  it('nearest neighbors (top 3)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // 3 closest places to DIA
    const result = await client.query(`
      SELECT name FROM places 
      WHERE name != 'DIA Airport'
      ORDER BY ABS(lat - 39.8561)
    `);
    assert.ok(result.rows.length >= 3);

    await client.end();
  });
});
