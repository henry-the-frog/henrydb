#!/usr/bin/env node
// example-app.js — A real REST API backed by HenryDB via Knex
// Demonstrates that HenryDB works as a drop-in PostgreSQL replacement.

import express from 'express';
import knexLib from 'knex';
import { HenryDBServer } from './server.js';

const DB_PORT = 15480;
const APP_PORT = 3100;

export async function createApp(options = {}) {
  const dbPort = options.dbPort || DB_PORT;
  const appPort = options.appPort || APP_PORT;

  // Start HenryDB server
  const dbServer = new HenryDBServer({ port: dbPort });
  await dbServer.start();

  // Connect Knex
  const db = knexLib({
    client: 'pg',
    connection: { host: '127.0.0.1', port: dbPort, user: 'app', database: 'app' },
    pool: { min: 1, max: 5 },
  });

  // Create schema
  await db.raw(`CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER, 
    title TEXT, 
    description TEXT, 
    status TEXT, 
    priority INTEGER,
    created_at TEXT
  )`);

  await db.raw(`CREATE TABLE IF NOT EXISTS tags (
    id INTEGER,
    task_id INTEGER,
    name TEXT
  )`);

  // Build Express app
  const app = express();
  app.use(express.json());

  // GET /tasks — List all tasks
  app.get('/tasks', async (req, res) => {
    try {
      const result = await db.raw('SELECT * FROM tasks ORDER BY priority DESC, id');
      res.json(result.rows);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // GET /tasks/:id — Get a task
  app.get('/tasks/:id', async (req, res) => {
    try {
      const result = await db.raw('SELECT * FROM tasks WHERE id = ?', [parseInt(req.params.id)]);
      if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
      
      // Get tags
      const tags = await db.raw('SELECT name FROM tags WHERE task_id = ?', [parseInt(req.params.id)]);
      res.json({ ...result.rows[0], tags: tags.rows.map(t => t.name) });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // POST /tasks — Create a task
  app.post('/tasks', async (req, res) => {
    try {
      const { title, description, priority, tags: tagNames } = req.body;
      
      // Get next ID
      const countResult = await db.raw('SELECT COUNT(*) AS c FROM tasks');
      const nextId = parseInt(countResult.rows[0].c) + 1;
      
      const now = new Date().toISOString();
      await db.raw('INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?)', 
        [nextId, title, description || '', 'pending', priority || 0, now]);

      // Add tags
      if (tagNames && tagNames.length > 0) {
        const tagCount = await db.raw('SELECT COUNT(*) AS c FROM tags');
        let tagId = parseInt(tagCount.rows[0].c) + 1;
        for (const tag of tagNames) {
          await db.raw('INSERT INTO tags VALUES (?, ?, ?)', [tagId++, nextId, tag]);
        }
      }

      res.status(201).json({ id: nextId, title, status: 'pending' });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // PATCH /tasks/:id — Update a task
  app.patch('/tasks/:id', async (req, res) => {
    try {
      const { status, priority, title } = req.body;
      const id = parseInt(req.params.id);
      
      if (status) await db.raw('UPDATE tasks SET status = ? WHERE id = ?', [status, id]);
      if (priority !== undefined) await db.raw('UPDATE tasks SET priority = ? WHERE id = ?', [priority, id]);
      if (title) await db.raw('UPDATE tasks SET title = ? WHERE id = ?', [title, id]);
      
      const result = await db.raw('SELECT * FROM tasks WHERE id = ?', [id]);
      res.json(result.rows[0] || { error: 'Not found' });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // DELETE /tasks/:id — Delete a task
  app.delete('/tasks/:id', async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      await db.raw('DELETE FROM tags WHERE task_id = ?', [id]);
      await db.raw('DELETE FROM tasks WHERE id = ?', [id]);
      res.json({ deleted: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // GET /stats — Task statistics
  app.get('/stats', async (req, res) => {
    try {
      const stats = await db.raw(`
        SELECT status, COUNT(*) AS count, AVG(priority) AS avg_priority
        FROM tasks
        GROUP BY status
        ORDER BY status
      `);
      const tagStats = await db.raw(`
        SELECT t.name, COUNT(*) AS count
        FROM tags t
        GROUP BY t.name
        ORDER BY count DESC
      `);
      res.json({ byStatus: stats.rows, byTag: tagStats.rows });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Start the server
  const httpServer = await new Promise((resolve) => {
    const s = app.listen(appPort, () => resolve(s));
  });

  return { app, httpServer, dbServer, db, appPort, dbPort };
}

export async function stopApp({ httpServer, dbServer, db }) {
  await db.destroy();
  await new Promise(r => httpServer.close(r));
  await dbServer.stop();
}

// CLI entry point
if (process.argv[1]?.endsWith('example-app.js')) {
  const app = await createApp();
  console.log(`🚀 Task API running on http://localhost:${APP_PORT}`);
  console.log(`📦 Backed by HenryDB on port ${DB_PORT}`);
  console.log('');
  console.log('Try:');
  console.log('  curl -X POST localhost:3100/tasks -H "Content-Type: application/json" -d \'{"title":"Buy groceries","priority":5,"tags":["shopping","urgent"]}\'');
  console.log('  curl localhost:3100/tasks');
  console.log('  curl localhost:3100/stats');
}
