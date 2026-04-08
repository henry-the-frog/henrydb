// server-realestate.test.js — Real estate data model
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15588;

describe('Real Estate', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE properties (id INTEGER, address TEXT, city TEXT, type TEXT, bedrooms INTEGER, bathrooms INTEGER, sqft INTEGER, price INTEGER, listed_at TEXT, status TEXT)');
    await client.query('CREATE TABLE agents (id INTEGER, name TEXT, agency TEXT, sales INTEGER)');
    await client.query('CREATE TABLE listings (id INTEGER, property_id INTEGER, agent_id INTEGER, listed_price INTEGER, sold_price INTEGER, days_on_market INTEGER)');
    
    await client.query("INSERT INTO properties VALUES (1, '123 Main St', 'Denver', 'house', 3, 2, 1800, 450000, '2026-03-01', 'sold')");
    await client.query("INSERT INTO properties VALUES (2, '456 Oak Ave', 'Denver', 'condo', 2, 1, 1100, 320000, '2026-03-15', 'active')");
    await client.query("INSERT INTO properties VALUES (3, '789 Pine Rd', 'Boulder', 'house', 4, 3, 2500, 680000, '2026-03-20', 'active')");
    await client.query("INSERT INTO properties VALUES (4, '101 Elm Ct', 'Denver', 'townhouse', 3, 2, 1600, 395000, '2026-04-01', 'sold')");
    await client.query("INSERT INTO properties VALUES (5, '202 Maple Dr', 'Boulder', 'house', 5, 4, 3200, 950000, '2026-04-05', 'active')");
    
    await client.query("INSERT INTO agents VALUES (1, 'Sarah Miller', 'Denver Realty', 25)");
    await client.query("INSERT INTO agents VALUES (2, 'Mike Johnson', 'Boulder Homes', 18)");
    
    await client.query("INSERT INTO listings VALUES (1, 1, 1, 460000, 450000, 30)");
    await client.query("INSERT INTO listings VALUES (2, 2, 1, 320000, NULL, NULL)");
    await client.query("INSERT INTO listings VALUES (3, 3, 2, 680000, NULL, NULL)");
    await client.query("INSERT INTO listings VALUES (4, 4, 1, 400000, 395000, 15)");
    await client.query("INSERT INTO listings VALUES (5, 5, 2, 975000, NULL, NULL)");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('active listings by city', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT city, COUNT(*) AS listings, AVG(price) AS avg_price FROM properties WHERE status = 'active' GROUP BY city ORDER BY avg_price DESC"
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('price per sqft', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT address, city, price, sqft FROM properties ORDER BY price DESC'
    );
    assert.strictEqual(result.rows.length, 5);

    await client.end();
  });

  it('agent performance', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT a.name, COUNT(l.id) AS listings, a.sales FROM agents a JOIN listings l ON a.id = l.agent_id GROUP BY a.name, a.sales ORDER BY listings DESC'
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('sold properties analysis', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT p.address, l.listed_price, l.sold_price, l.days_on_market FROM listings l JOIN properties p ON l.property_id = p.id WHERE p.status = 'sold'"
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('properties by type', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT type, COUNT(*) AS count, AVG(price) AS avg_price FROM properties GROUP BY type ORDER BY avg_price DESC'
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });
});
