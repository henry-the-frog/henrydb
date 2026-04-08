// server-music.test.js — Music streaming data model
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15584;

describe('Music Streaming', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE artists (id INTEGER, name TEXT, genre TEXT)');
    await client.query('CREATE TABLE albums (id INTEGER, artist_id INTEGER, title TEXT, year INTEGER)');
    await client.query('CREATE TABLE tracks (id INTEGER, album_id INTEGER, title TEXT, duration_sec INTEGER, plays INTEGER)');
    await client.query('CREATE TABLE playlists (id INTEGER, user_name TEXT, name TEXT)');
    await client.query('CREATE TABLE playlist_tracks (playlist_id INTEGER, track_id INTEGER, position INTEGER)');
    
    await client.query("INSERT INTO artists VALUES (1, 'The Beatles', 'rock')");
    await client.query("INSERT INTO artists VALUES (2, 'Miles Davis', 'jazz')");
    await client.query("INSERT INTO artists VALUES (3, 'Daft Punk', 'electronic')");
    
    await client.query("INSERT INTO albums VALUES (1, 1, 'Abbey Road', 1969)");
    await client.query("INSERT INTO albums VALUES (2, 2, 'Kind of Blue', 1959)");
    await client.query("INSERT INTO albums VALUES (3, 3, 'Random Access Memories', 2013)");
    
    await client.query("INSERT INTO tracks VALUES (1, 1, 'Come Together', 259, 1500000)");
    await client.query("INSERT INTO tracks VALUES (2, 1, 'Here Comes the Sun', 185, 2000000)");
    await client.query("INSERT INTO tracks VALUES (3, 2, 'So What', 562, 800000)");
    await client.query("INSERT INTO tracks VALUES (4, 2, 'Blue in Green', 327, 600000)");
    await client.query("INSERT INTO tracks VALUES (5, 3, 'Get Lucky', 369, 3000000)");
    await client.query("INSERT INTO tracks VALUES (6, 3, 'Instant Crush', 337, 1200000)");
    
    await client.query("INSERT INTO playlists VALUES (1, 'alice', 'Favorites')");
    await client.query('INSERT INTO playlist_tracks VALUES (1, 2, 1)');
    await client.query('INSERT INTO playlist_tracks VALUES (1, 5, 2)');
    await client.query('INSERT INTO playlist_tracks VALUES (1, 3, 3)');
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('top tracks by plays', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT t.title, ar.name AS artist, t.plays FROM tracks t JOIN albums al ON t.album_id = al.id JOIN artists ar ON al.artist_id = ar.id ORDER BY t.plays DESC'
    );
    assert.ok(result.rows.length >= 5);
    assert.strictEqual(result.rows[0].title, 'Get Lucky'); // Most played

    await client.end();
  });

  it('artist discography', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT al.title, al.year, COUNT(t.id) AS tracks FROM albums al JOIN tracks t ON al.id = t.album_id JOIN artists ar ON al.artist_id = ar.id WHERE ar.name = 'Miles Davis' GROUP BY al.title, al.year"
    );
    assert.strictEqual(result.rows.length, 1);

    await client.end();
  });

  it('playlist contents', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT t.title, ar.name AS artist FROM playlist_tracks pt JOIN tracks t ON pt.track_id = t.id JOIN albums al ON t.album_id = al.id JOIN artists ar ON al.artist_id = ar.id WHERE pt.playlist_id = 1 ORDER BY pt.position'
    );
    assert.strictEqual(result.rows.length, 3);

    await client.end();
  });

  it('total plays by genre', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT ar.genre, SUM(t.plays) AS total_plays FROM tracks t JOIN albums al ON t.album_id = al.id JOIN artists ar ON al.artist_id = ar.id GROUP BY ar.genre ORDER BY total_plays DESC'
    );
    assert.strictEqual(result.rows.length, 3);

    await client.end();
  });

  it('total listening time', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT SUM(duration_sec) AS total_seconds FROM tracks');
    assert.ok(parseInt(result.rows[0].total_seconds) > 1000);

    await client.end();
  });
});
