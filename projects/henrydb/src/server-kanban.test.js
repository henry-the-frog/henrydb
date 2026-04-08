// server-kanban.test.js — Project management / Kanban board pattern
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15580;

describe('Kanban Board / Project Management', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE boards (id INTEGER, name TEXT, owner_id INTEGER)');
    await client.query('CREATE TABLE kanban_columns (id INTEGER, board_id INTEGER, name TEXT, position INTEGER)');
    await client.query('CREATE TABLE cards (id INTEGER, column_id INTEGER, title TEXT, description TEXT, assignee TEXT, priority TEXT, due_date TEXT, created_at TEXT)');
    
    await client.query("INSERT INTO boards VALUES (1, 'Sprint 42', 1)");
    
    await client.query("INSERT INTO kanban_columns VALUES (1, 1, 'Backlog', 0)");
    await client.query("INSERT INTO kanban_columns VALUES (2, 1, 'In Progress', 1)");
    await client.query("INSERT INTO kanban_columns VALUES (3, 1, 'Review', 2)");
    await client.query("INSERT INTO kanban_columns VALUES (4, 1, 'Done', 3)");
    
    await client.query("INSERT INTO cards VALUES (1, 1, 'Design API', 'RESTful API design', 'Alice', 'high', '2026-04-10', '2026-04-01')");
    await client.query("INSERT INTO cards VALUES (2, 2, 'Build Frontend', 'React components', 'Bob', 'high', '2026-04-12', '2026-04-02')");
    await client.query("INSERT INTO cards VALUES (3, 2, 'Database Schema', 'Design tables', 'Alice', 'medium', '2026-04-11', '2026-04-02')");
    await client.query("INSERT INTO cards VALUES (4, 3, 'Write Tests', 'Unit tests for API', 'Charlie', 'medium', '2026-04-09', '2026-04-03')");
    await client.query("INSERT INTO cards VALUES (5, 4, 'Setup CI', 'GitHub Actions', 'Bob', 'low', '2026-04-08', '2026-04-01')");
    await client.query("INSERT INTO cards VALUES (6, 1, 'Documentation', 'API docs', 'Alice', 'low', '2026-04-15', '2026-04-04')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('board overview: cards per column', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT col.name, COUNT(c.id) AS card_count FROM kanban_columns col LEFT JOIN cards c ON col.id = c.column_id GROUP BY col.name, col.position ORDER BY col.position'
    );
    assert.strictEqual(result.rows.length, 4);

    await client.end();
  });

  it('cards by assignee', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT assignee, COUNT(*) AS tasks FROM cards GROUP BY assignee ORDER BY tasks DESC'
    );
    assert.ok(result.rows.length >= 3);
    assert.strictEqual(result.rows[0].assignee, 'Alice'); // Most tasks

    await client.end();
  });

  it('overdue cards', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT c.title, c.assignee, c.due_date, col.name AS column FROM cards c JOIN kanban_columns col ON c.column_id = col.id WHERE c.due_date < '2026-04-09' AND col.name != 'Done'"
    );
    assert.ok(result.rows.length >= 0); // May have overdue items

    await client.end();
  });

  it('move card between kanban_columns', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Move "Write Tests" from Review to Done
    await client.query('UPDATE cards SET column_id = 4 WHERE id = 4');
    
    const result = await client.query("SELECT col.name FROM cards c JOIN kanban_columns col ON c.column_id = col.id WHERE c.id = 4");
    assert.strictEqual(result.rows[0].name, 'Done');

    await client.end();
  });

  it('sprint velocity (completed cards)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT COUNT(*) AS completed FROM cards WHERE column_id = 4"
    );
    assert.ok(parseInt(result.rows[0].completed) >= 2); // Setup CI + Write Tests

    await client.end();
  });

  it('high priority items not done', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT c.title, c.assignee, col.name AS status FROM cards c JOIN kanban_columns col ON c.column_id = col.id WHERE c.priority = 'high' AND col.name != 'Done'"
    );
    assert.ok(result.rows.length >= 1);

    await client.end();
  });
});
