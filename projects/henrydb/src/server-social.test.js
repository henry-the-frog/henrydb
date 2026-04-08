// server-social.test.js — Social network graph queries through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15518;

describe('Social Network Graph', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE people (id INTEGER, name TEXT, bio TEXT)');
    await client.query('CREATE TABLE friendships (id INTEGER, person_a INTEGER, person_b INTEGER)');
    await client.query('CREATE TABLE social_posts (id INTEGER, author_id INTEGER, content TEXT, likes INTEGER, created_at TEXT)');
    await client.query('CREATE TABLE comments (id INTEGER, post_id INTEGER, author_id INTEGER, content TEXT)');
    
    // People
    const names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank'];
    for (let i = 0; i < names.length; i++) {
      await client.query(`INSERT INTO people VALUES (${i + 1}, '${names[i]}', 'Bio of ${names[i]}')`);
    }
    
    // Friendships (bidirectional)
    const friends = [[1,2],[1,3],[2,3],[2,4],[3,5],[4,5],[5,6],[1,6]];
    for (let i = 0; i < friends.length; i++) {
      await client.query(`INSERT INTO friendships VALUES (${i + 1}, ${friends[i][0]}, ${friends[i][1]})`);
    }
    
    // Posts
    await client.query("INSERT INTO social_posts VALUES (1, 1, 'Hello world!', 10, '2026-04-01')");
    await client.query("INSERT INTO social_posts VALUES (2, 2, 'Great day!', 5, '2026-04-02')");
    await client.query("INSERT INTO social_posts VALUES (3, 1, 'Working on HenryDB', 25, '2026-04-03')");
    await client.query("INSERT INTO social_posts VALUES (4, 3, 'Coffee time', 3, '2026-04-04')");
    await client.query("INSERT INTO social_posts VALUES (5, 5, 'Weekend plans', 8, '2026-04-05')");
    
    // Comments
    await client.query("INSERT INTO comments VALUES (1, 1, 2, 'Nice!')");
    await client.query("INSERT INTO comments VALUES (2, 1, 3, 'Welcome!')");
    await client.query("INSERT INTO comments VALUES (3, 3, 4, 'Sounds cool')");
    await client.query("INSERT INTO comments VALUES (4, 3, 2, 'Tell me more')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('find friends of a user', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Alice's friends (id=1)
    const result = await client.query(
      'SELECT p.name FROM people p JOIN friendships f ON (p.id = f.person_b AND f.person_a = 1) OR (p.id = f.person_a AND f.person_b = 1)'
    );
    assert.ok(result.rows.length >= 3); // Bob, Charlie, Frank

    await client.end();
  });

  it('mutual friends', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Find people who are friends with both Alice(1) and Bob(2)
    const result = await client.query(`
      SELECT p.name FROM people p
      JOIN friendships f1 ON (p.id = f1.person_b AND f1.person_a = 1) OR (p.id = f1.person_a AND f1.person_b = 1)
      JOIN friendships f2 ON (p.id = f2.person_b AND f2.person_a = 2) OR (p.id = f2.person_a AND f2.person_b = 2)
    `);
    assert.ok(result.rows.length >= 1); // Charlie is mutual friend

    await client.end();
  });

  it('news feed: posts from friends', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Alice's feed: posts from her friends
    const result = await client.query(`
      SELECT sp.content, p.name AS author, sp.likes
      FROM social_posts sp
      JOIN people p ON sp.author_id = p.id
      JOIN friendships f ON (sp.author_id = f.person_b AND f.person_a = 1) OR (sp.author_id = f.person_a AND f.person_b = 1)
      ORDER BY sp.likes DESC
    `);
    assert.ok(result.rows.length >= 1);

    await client.end();
  });

  it('most popular posts', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT p.name, sp.content, sp.likes FROM social_posts sp JOIN people p ON sp.author_id = p.id ORDER BY sp.likes DESC'
    );
    assert.ok(result.rows.length >= 3);
    assert.strictEqual(result.rows[0].content, 'Working on HenryDB'); // Most liked

    await client.end();
  });

  it('post with comments', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT sp.content AS post, p.name AS commenter, c.content AS comment FROM comments c JOIN social_posts sp ON c.post_id = sp.id JOIN people p ON c.author_id = p.id WHERE sp.id = 3'
    );
    assert.strictEqual(result.rows.length, 2); // Diana and Bob commented

    await client.end();
  });

  it('friend count leaderboard', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(`
      SELECT p.name, COUNT(*) AS friend_count
      FROM people p
      JOIN friendships f ON p.id = f.person_a OR p.id = f.person_b
      GROUP BY p.name
      ORDER BY friend_count DESC
    `);
    assert.ok(result.rows.length >= 4);

    await client.end();
  });

  it('engagement metrics: posts per user', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT p.name, COUNT(sp.id) AS post_count, SUM(sp.likes) AS total_likes FROM people p JOIN social_posts sp ON p.id = sp.author_id GROUP BY p.name ORDER BY total_likes DESC'
    );
    assert.ok(result.rows.length >= 2);
    assert.strictEqual(result.rows[0].name, 'Alice'); // Most total likes

    await client.end();
  });
});
