// server-recipes.test.js — Recipe database through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15583;

describe('Recipe Database', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE recipes (id INTEGER, name TEXT, cuisine TEXT, prep_time INTEGER, cook_time INTEGER, servings INTEGER, difficulty TEXT)');
    await client.query('CREATE TABLE ingredients (id INTEGER, recipe_id INTEGER, name TEXT, amount TEXT, unit TEXT)');
    
    await client.query("INSERT INTO recipes VALUES (1, 'Pad Thai', 'Thai', 15, 10, 4, 'medium')");
    await client.query("INSERT INTO recipes VALUES (2, 'Caesar Salad', 'American', 10, 0, 2, 'easy')");
    await client.query("INSERT INTO recipes VALUES (3, 'Beef Wellington', 'British', 45, 30, 6, 'hard')");
    await client.query("INSERT INTO recipes VALUES (4, 'Miso Soup', 'Japanese', 5, 10, 4, 'easy')");
    
    await client.query("INSERT INTO ingredients VALUES (1, 1, 'rice noodles', '200', 'g')");
    await client.query("INSERT INTO ingredients VALUES (2, 1, 'shrimp', '300', 'g')");
    await client.query("INSERT INTO ingredients VALUES (3, 1, 'peanuts', '50', 'g')");
    await client.query("INSERT INTO ingredients VALUES (4, 2, 'romaine lettuce', '1', 'head')");
    await client.query("INSERT INTO ingredients VALUES (5, 2, 'parmesan', '50', 'g')");
    await client.query("INSERT INTO ingredients VALUES (6, 3, 'beef tenderloin', '1', 'kg')");
    await client.query("INSERT INTO ingredients VALUES (7, 3, 'puff pastry', '1', 'sheet')");
    await client.query("INSERT INTO ingredients VALUES (8, 4, 'miso paste', '3', 'tbsp')");
    await client.query("INSERT INTO ingredients VALUES (9, 4, 'tofu', '200', 'g')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('recipe with ingredients', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT r.name AS recipe, i.name AS ingredient, i.amount, i.unit FROM recipes r JOIN ingredients i ON r.id = i.recipe_id WHERE r.id = 1 ORDER BY i.name'
    );
    assert.strictEqual(result.rows.length, 3);

    await client.end();
  });

  it('quick recipes (under 20 min total)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT name, prep_time + cook_time AS total_time FROM recipes WHERE prep_time + cook_time <= 20"
    );
    assert.ok(result.rows.length >= 2); // Miso Soup and Caesar Salad

    await client.end();
  });

  it('recipes by cuisine', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT cuisine, COUNT(*) AS count FROM recipes GROUP BY cuisine ORDER BY count DESC');
    assert.ok(result.rows.length >= 3);

    await client.end();
  });

  it('recipes by difficulty', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT name, difficulty FROM recipes WHERE difficulty = 'easy' ORDER BY name");
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('ingredient frequency across recipes', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT name, COUNT(*) AS used_in FROM ingredients GROUP BY name ORDER BY used_in DESC'
    );
    assert.ok(result.rows.length >= 5);

    await client.end();
  });
});
