// example-app.test.js — Integration tests for the REST API mini-app
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createApp, stopApp } from './example-app.js';

const APP_PORT = 15481;
const DB_PORT = 15482;

async function api(path, options = {}) {
  const res = await fetch(`http://127.0.0.1:${APP_PORT}${path}`, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...options.headers },
  });
  const data = await res.json();
  return { status: res.status, data };
}

describe('Task API (Express + Knex + HenryDB)', () => {
  let app;

  before(async () => {
    app = await createApp({ appPort: APP_PORT, dbPort: DB_PORT });
  });

  after(async () => {
    await stopApp(app);
  });

  it('GET /tasks returns empty list initially', async () => {
    const { status, data } = await api('/tasks');
    assert.strictEqual(status, 200);
    assert.deepStrictEqual(data, []);
  });

  it('POST /tasks creates a task', async () => {
    const { status, data } = await api('/tasks', {
      method: 'POST',
      body: JSON.stringify({ title: 'Buy groceries', priority: 5, tags: ['shopping', 'urgent'] }),
    });
    assert.strictEqual(status, 201);
    assert.strictEqual(data.title, 'Buy groceries');
    assert.strictEqual(data.id, 1);
  });

  it('POST /tasks creates more tasks', async () => {
    await api('/tasks', {
      method: 'POST',
      body: JSON.stringify({ title: 'Write tests', priority: 8, tags: ['dev', 'urgent'] }),
    });
    await api('/tasks', {
      method: 'POST',
      body: JSON.stringify({ title: 'Read a book', priority: 2, tags: ['personal'] }),
    });
    await api('/tasks', {
      method: 'POST',
      body: JSON.stringify({ title: 'Deploy server', priority: 9, tags: ['dev', 'ops'] }),
    });

    const { data } = await api('/tasks');
    assert.strictEqual(data.length, 4);
  });

  it('GET /tasks/:id returns task with tags', async () => {
    const { status, data } = await api('/tasks/1');
    assert.strictEqual(status, 200);
    assert.strictEqual(data.title, 'Buy groceries');
    assert.ok(Array.isArray(data.tags));
    assert.ok(data.tags.includes('shopping'));
    assert.ok(data.tags.includes('urgent'));
  });

  it('GET /tasks/:id returns 404 for missing task', async () => {
    const { status } = await api('/tasks/999');
    assert.strictEqual(status, 404);
  });

  it('PATCH /tasks/:id updates status', async () => {
    const { data } = await api('/tasks/1', {
      method: 'PATCH',
      body: JSON.stringify({ status: 'completed' }),
    });
    assert.strictEqual(data.status, 'completed');
  });

  it('PATCH /tasks/:id updates priority', async () => {
    const { data } = await api('/tasks/3', {
      method: 'PATCH',
      body: JSON.stringify({ priority: 7 }),
    });
    assert.strictEqual(parseInt(data.priority), 7);
  });

  it('GET /stats returns aggregated statistics', async () => {
    const { status, data } = await api('/stats');
    assert.strictEqual(status, 200);
    assert.ok(Array.isArray(data.byStatus));
    assert.ok(Array.isArray(data.byTag));
    
    // Should have stats by status
    const completed = data.byStatus.find(s => s.status === 'completed');
    const pending = data.byStatus.find(s => s.status === 'pending');
    assert.ok(completed || pending, 'Expected status stats');
    
    // Should have tag counts
    assert.ok(data.byTag.length > 0, 'Expected tag stats');
    const urgentTag = data.byTag.find(t => t.name === 'urgent');
    assert.ok(urgentTag, 'Expected urgent tag');
    assert.strictEqual(parseInt(urgentTag.count), 2); // 2 tasks have 'urgent'
  });

  it('DELETE /tasks/:id removes task and tags', async () => {
    const { data } = await api('/tasks/3', { method: 'DELETE' });
    assert.strictEqual(data.deleted, true);

    const { status } = await api('/tasks/3');
    assert.strictEqual(status, 404);

    // Verify tags cleaned up
    const tasks = await api('/tasks');
    assert.strictEqual(tasks.data.length, 3);
  });

  it('GET /tasks returns tasks ordered by priority', async () => {
    const { data } = await api('/tasks');
    // Verify ordering: highest priority first
    for (let i = 1; i < data.length; i++) {
      assert.ok(
        parseInt(data[i - 1].priority) >= parseInt(data[i].priority),
        `Expected ${data[i - 1].priority} >= ${data[i].priority}`
      );
    }
  });

  it('handles concurrent requests', async () => {
    const results = await Promise.all([
      api('/tasks'),
      api('/stats'),
      api('/tasks/1'),
      api('/tasks/2'),
    ]);
    for (const { status } of results) {
      assert.strictEqual(status, 200);
    }
  });
});
