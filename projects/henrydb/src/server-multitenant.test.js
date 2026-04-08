// server-multitenant.test.js — Multi-tenant SaaS data patterns
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15516;

describe('Multi-Tenant SaaS Patterns', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    // Tenants
    await client.query('CREATE TABLE tenants (id INTEGER, name TEXT, plan TEXT, created_at TEXT)');
    await client.query("INSERT INTO tenants VALUES (1, 'Acme Corp', 'enterprise', '2026-01-01')");
    await client.query("INSERT INTO tenants VALUES (2, 'Startup Inc', 'starter', '2026-02-15')");
    await client.query("INSERT INTO tenants VALUES (3, 'BigCo Ltd', 'enterprise', '2026-03-01')");
    
    // Users (tenant-scoped)
    await client.query('CREATE TABLE tenant_users (id INTEGER, tenant_id INTEGER, name TEXT, role TEXT)');
    await client.query("INSERT INTO tenant_users VALUES (1, 1, 'Alice', 'admin')");
    await client.query("INSERT INTO tenant_users VALUES (2, 1, 'Bob', 'user')");
    await client.query("INSERT INTO tenant_users VALUES (3, 2, 'Charlie', 'admin')");
    await client.query("INSERT INTO tenant_users VALUES (4, 3, 'Diana', 'admin')");
    await client.query("INSERT INTO tenant_users VALUES (5, 3, 'Eve', 'user')");
    
    // Projects (tenant-scoped)
    await client.query('CREATE TABLE projects (id INTEGER, tenant_id INTEGER, name TEXT, status TEXT)');
    await client.query("INSERT INTO projects VALUES (1, 1, 'Website Redesign', 'active')");
    await client.query("INSERT INTO projects VALUES (2, 1, 'Mobile App', 'active')");
    await client.query("INSERT INTO projects VALUES (3, 2, 'MVP', 'active')");
    await client.query("INSERT INTO projects VALUES (4, 3, 'Enterprise Portal', 'active')");
    await client.query("INSERT INTO projects VALUES (5, 3, 'API Integration', 'completed')");
    
    // Usage metrics
    await client.query('CREATE TABLE usage (id INTEGER, tenant_id INTEGER, metric TEXT, value INTEGER, month TEXT)');
    await client.query("INSERT INTO usage VALUES (1, 1, 'api_calls', 50000, '2026-03')");
    await client.query("INSERT INTO usage VALUES (2, 1, 'storage_mb', 2048, '2026-03')");
    await client.query("INSERT INTO usage VALUES (3, 2, 'api_calls', 5000, '2026-03')");
    await client.query("INSERT INTO usage VALUES (4, 2, 'storage_mb', 256, '2026-03')");
    await client.query("INSERT INTO usage VALUES (5, 3, 'api_calls', 100000, '2026-03')");
    await client.query("INSERT INTO usage VALUES (6, 3, 'storage_mb', 8192, '2026-03')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('tenant isolation: only see own data', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Simulate tenant 1 querying their users
    const result = await client.query('SELECT * FROM tenant_users WHERE tenant_id = 1');
    assert.strictEqual(result.rows.length, 2);
    assert.ok(result.rows.every(r => parseInt(r.tenant_id) === 1));

    await client.end();
  });

  it('cross-tenant admin dashboard', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT t.name AS tenant, COUNT(u.id) AS users, t.plan FROM tenants t JOIN tenant_users u ON t.id = u.tenant_id GROUP BY t.name, t.plan ORDER BY users DESC'
    );
    assert.strictEqual(result.rows.length, 3);

    await client.end();
  });

  it('usage billing report', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT t.name, u.metric, u.value FROM tenants t JOIN usage u ON t.id = u.tenant_id WHERE u.metric = 'api_calls' ORDER BY u.value DESC"
    );
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].name, 'BigCo Ltd'); // Highest usage

    await client.end();
  });

  it('tenant resource summary', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT t.name, COUNT(DISTINCT p.id) AS projects, COUNT(DISTINCT u.id) AS users FROM tenants t LEFT JOIN projects p ON t.id = p.tenant_id LEFT JOIN tenant_users u ON t.id = u.tenant_id GROUP BY t.name ORDER BY projects DESC'
    );
    assert.strictEqual(result.rows.length, 3);

    await client.end();
  });

  it('plan-based filtering', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT t.name, t.plan, SUM(u.value) AS total_usage FROM tenants t JOIN usage u ON t.id = u.tenant_id WHERE t.plan = 'enterprise' GROUP BY t.name, t.plan"
    );
    assert.strictEqual(result.rows.length, 2); // Acme and BigCo

    await client.end();
  });
});
