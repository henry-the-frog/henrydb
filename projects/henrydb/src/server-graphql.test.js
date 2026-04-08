// server-graphql.test.js — Tests for a GraphQL-like query layer over HenryDB
// This tests translating structured queries into SQL through the wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15512;

// Simple GraphQL-to-SQL translator
function graphqlToSql(query) {
  // Parse: { users(where: {age_gt: 25}) { name, age } }
  const match = query.match(/\{\s*(\w+)(?:\(where:\s*\{([^}]*)\}\))?\s*\{([^}]+)\}\s*\}/);
  if (!match) throw new Error('Invalid query');
  
  const table = match[1];
  const whereStr = match[2];
  const fields = match[3].split(',').map(f => f.trim());
  
  let sql = `SELECT ${fields.join(', ')} FROM ${table}`;
  
  if (whereStr) {
    const conditions = whereStr.split(',').map(c => {
      const [key, val] = c.split(':').map(s => s.trim());
      if (key.endsWith('_gt')) return `${key.slice(0, -3)} > ${val}`;
      if (key.endsWith('_lt')) return `${key.slice(0, -3)} < ${val}`;
      if (key.endsWith('_gte')) return `${key.slice(0, -4)} >= ${val}`;
      if (key.endsWith('_lte')) return `${key.slice(0, -4)} <= ${val}`;
      return `${key} = ${val}`;
    });
    sql += ` WHERE ${conditions.join(' AND ')}`;
  }
  
  return sql;
}

describe('GraphQL-like Query Layer', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER, email TEXT)');
    await client.query("INSERT INTO users VALUES (1, 'Alice', 30, 'alice@test.com')");
    await client.query("INSERT INTO users VALUES (2, 'Bob', 25, 'bob@test.com')");
    await client.query("INSERT INTO users VALUES (3, 'Charlie', 35, 'charlie@test.com')");
    
    await client.query('CREATE TABLE posts (id INTEGER, user_id INTEGER, title TEXT, body TEXT)');
    await client.query("INSERT INTO posts VALUES (1, 1, 'Hello World', 'First post')");
    await client.query("INSERT INTO posts VALUES (2, 1, 'Second Post', 'More content')");
    await client.query("INSERT INTO posts VALUES (3, 2, 'Bob writes', 'From Bob')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('simple field selection', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const sql = graphqlToSql('{ users { name, age } }');
    const result = await client.query(sql);
    
    assert.strictEqual(result.rows.length, 3);
    assert.ok(result.rows[0].name);
    assert.ok(result.rows[0].age !== undefined);

    await client.end();
  });

  it('filtered query (where clause)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const sql = graphqlToSql('{ users(where: {age_gt: 28}) { name, age } }');
    const result = await client.query(sql);
    
    assert.strictEqual(result.rows.length, 2); // Alice (30) and Charlie (35)
    for (const row of result.rows) {
      assert.ok(parseInt(row.age) > 28);
    }

    await client.end();
  });

  it('single field query', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const sql = graphqlToSql('{ posts { title } }');
    const result = await client.query(sql);
    
    assert.strictEqual(result.rows.length, 3);
    assert.ok(result.rows[0].title);

    await client.end();
  });

  it('nested relation via JOIN', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Simulate: { users { name, posts { title } } }
    const sql = 'SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id ORDER BY u.name, p.title';
    const result = await client.query(sql);
    
    assert.ok(result.rows.length >= 3);
    const alicePosts = result.rows.filter(r => r.name === 'Alice');
    assert.strictEqual(alicePosts.length, 2);

    await client.end();
  });

  it('aggregate query (count)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Simulate: { usersCount }
    const result = await client.query('SELECT COUNT(*) AS count FROM users');
    assert.strictEqual(parseInt(result.rows[0].count), 3);

    await client.end();
  });

  it('mutation (insert) and re-query', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Simulate mutation
    await client.query("INSERT INTO users VALUES (4, 'Diana', 28, 'diana@test.com')");
    
    const sql = graphqlToSql('{ users(where: {age_gte: 28}) { name } }');
    const result = await client.query(sql);
    assert.ok(result.rows.length >= 3); // Alice, Charlie, Diana

    await client.end();
  });
});
