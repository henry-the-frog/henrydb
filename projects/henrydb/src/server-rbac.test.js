// server-rbac.test.js — Role-Based Access Control pattern
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15533;

describe('RBAC (Role-Based Access Control)', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE rbac_users (id INTEGER, name TEXT, email TEXT)');
    await client.query('CREATE TABLE rbac_roles (id INTEGER, name TEXT, description TEXT)');
    await client.query('CREATE TABLE user_roles (user_id INTEGER, role_id INTEGER)');
    await client.query('CREATE TABLE permissions (id INTEGER, role_id INTEGER, resource TEXT, action TEXT)');
    
    // Users
    await client.query("INSERT INTO rbac_users VALUES (1, 'Alice', 'alice@corp.com')");
    await client.query("INSERT INTO rbac_users VALUES (2, 'Bob', 'bob@corp.com')");
    await client.query("INSERT INTO rbac_users VALUES (3, 'Charlie', 'charlie@corp.com')");
    
    // Roles
    await client.query("INSERT INTO rbac_roles VALUES (1, 'admin', 'Full access')");
    await client.query("INSERT INTO rbac_roles VALUES (2, 'editor', 'Can create and edit content')");
    await client.query("INSERT INTO rbac_roles VALUES (3, 'viewer', 'Read-only access')");
    
    // User-Role assignments
    await client.query('INSERT INTO user_roles VALUES (1, 1)'); // Alice = admin
    await client.query('INSERT INTO user_roles VALUES (2, 2)'); // Bob = editor
    await client.query('INSERT INTO user_roles VALUES (3, 3)'); // Charlie = viewer
    await client.query('INSERT INTO user_roles VALUES (1, 2)'); // Alice also editor
    
    // Permissions
    await client.query("INSERT INTO permissions VALUES (1, 1, 'users', 'read')");
    await client.query("INSERT INTO permissions VALUES (2, 1, 'users', 'write')");
    await client.query("INSERT INTO permissions VALUES (3, 1, 'users', 'delete')");
    await client.query("INSERT INTO permissions VALUES (4, 2, 'content', 'read')");
    await client.query("INSERT INTO permissions VALUES (5, 2, 'content', 'write')");
    await client.query("INSERT INTO permissions VALUES (6, 3, 'content', 'read')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('list user roles', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT u.name, r.name AS role FROM rbac_users u JOIN user_roles ur ON u.id = ur.user_id JOIN rbac_roles r ON ur.role_id = r.id ORDER BY u.name, r.name'
    );
    assert.ok(result.rows.length >= 4);
    // Alice should have 2 roles
    const aliceRoles = result.rows.filter(r => r.name === 'Alice');
    assert.strictEqual(aliceRoles.length, 2);

    await client.end();
  });

  it('check user permissions', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Alice's permissions (via admin + editor roles)
    const result = await client.query(
      'SELECT DISTINCT p.resource, p.action FROM permissions p JOIN user_roles ur ON p.role_id = ur.role_id WHERE ur.user_id = 1 ORDER BY p.resource, p.action'
    );
    assert.ok(result.rows.length >= 5); // users:read/write/delete + content:read/write

    await client.end();
  });

  it('users with specific permission', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Who can write content?
    const result = await client.query(
      "SELECT DISTINCT u.name FROM rbac_users u JOIN user_roles ur ON u.id = ur.user_id JOIN permissions p ON ur.role_id = p.role_id WHERE p.resource = 'content' AND p.action = 'write'"
    );
    assert.ok(result.rows.length >= 1);

    await client.end();
  });

  it('role permission summary', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT r.name AS role, COUNT(p.id) AS permission_count FROM rbac_roles r JOIN permissions p ON r.id = p.role_id GROUP BY r.name ORDER BY permission_count DESC'
    );
    assert.ok(result.rows.length >= 3);
    assert.strictEqual(result.rows[0].role, 'admin'); // Most permissions

    await client.end();
  });
});
