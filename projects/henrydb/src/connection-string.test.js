// connection-string.test.js — Tests for connection string parser
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parseConnectionString, buildConnectionString } from './connection-string.js';

describe('Connection String Parser', () => {
  describe('URL format', () => {
    it('parses full URL', () => {
      const r = parseConnectionString('postgres://admin:secret@dbhost:5433/mydb');
      assert.equal(r.host, 'dbhost');
      assert.equal(r.port, 5433);
      assert.equal(r.database, 'mydb');
      assert.equal(r.user, 'admin');
      assert.equal(r.password, 'secret');
    });

    it('parses URL without password', () => {
      const r = parseConnectionString('postgres://admin@localhost/testdb');
      assert.equal(r.user, 'admin');
      assert.equal(r.password, '');
      assert.equal(r.database, 'testdb');
    });

    it('parses URL without auth', () => {
      const r = parseConnectionString('postgres://localhost:5432/mydb');
      assert.equal(r.host, 'localhost');
      assert.equal(r.port, 5432);
      assert.equal(r.database, 'mydb');
    });

    it('parses postgresql:// alias', () => {
      const r = parseConnectionString('postgresql://user@host/db');
      assert.equal(r.host, 'host');
      assert.equal(r.user, 'user');
    });

    it('parses SSL parameter', () => {
      const r = parseConnectionString('postgres://localhost/db?ssl=true');
      assert.equal(r.ssl, true);
    });

    it('parses sslmode parameter', () => {
      const r = parseConnectionString('postgres://localhost/db?sslmode=require');
      assert.equal(r.ssl, true);
    });

    it('parses multiple query params', () => {
      const r = parseConnectionString('postgres://localhost/db?ssl=true&application_name=myapp&connect_timeout=10');
      assert.equal(r.ssl, true);
      assert.equal(r.options.applicationName, 'myapp');
      assert.equal(r.options.connectTimeout, 10);
    });

    it('handles URL-encoded characters', () => {
      const r = parseConnectionString('postgres://user%40org:p%40ss@host/db');
      assert.equal(r.user, 'user@org');
      assert.equal(r.password, 'p@ss');
    });

    it('defaults for minimal URL', () => {
      const r = parseConnectionString('postgres://localhost');
      assert.equal(r.host, 'localhost');
      assert.equal(r.port, 5432);
      assert.equal(r.database, 'postgres');
    });
  });

  describe('DSN format', () => {
    it('parses key-value pairs', () => {
      const r = parseConnectionString("host=dbhost port=5433 dbname=mydb user=admin password=secret");
      assert.equal(r.host, 'dbhost');
      assert.equal(r.port, 5433);
      assert.equal(r.database, 'mydb');
      assert.equal(r.user, 'admin');
      assert.equal(r.password, 'secret');
    });

    it('parses quoted values', () => {
      const r = parseConnectionString("host=localhost password='my password' dbname=test");
      assert.equal(r.password, 'my password');
    });

    it('handles sslmode in DSN', () => {
      const r = parseConnectionString('host=localhost sslmode=require');
      assert.equal(r.ssl, true);
    });
  });

  describe('edge cases', () => {
    it('returns defaults for empty string', () => {
      const r = parseConnectionString('');
      assert.equal(r.host, 'localhost');
      assert.equal(r.port, 5432);
    });

    it('returns defaults for null', () => {
      const r = parseConnectionString(null);
      assert.equal(r.host, 'localhost');
    });

    it('treats plain string as host', () => {
      const r = parseConnectionString('myhost.example.com');
      assert.equal(r.host, 'myhost.example.com');
    });
  });

  describe('buildConnectionString', () => {
    it('builds URL from config', () => {
      const url = buildConnectionString({ host: 'db.example.com', port: 5433, database: 'mydb', user: 'admin', password: 'secret' });
      assert.ok(url.startsWith('postgres://'));
      assert.ok(url.includes('admin'));
      assert.ok(url.includes('5433'));
      assert.ok(url.includes('mydb'));
    });

    it('omits default port', () => {
      const url = buildConnectionString({ host: 'localhost', port: 5432 });
      assert.ok(!url.includes(':5432'));
    });

    it('includes SSL param', () => {
      const url = buildConnectionString({ ssl: true });
      assert.ok(url.includes('ssl=true'));
    });

    it('round-trip: parse → build → parse', () => {
      const original = 'postgres://admin:secret@dbhost:5433/mydb?ssl=true';
      const parsed = parseConnectionString(original);
      const rebuilt = buildConnectionString(parsed);
      const reparsed = parseConnectionString(rebuilt);
      
      assert.equal(reparsed.host, parsed.host);
      assert.equal(reparsed.port, parsed.port);
      assert.equal(reparsed.database, parsed.database);
      assert.equal(reparsed.user, parsed.user);
      assert.equal(reparsed.ssl, parsed.ssl);
    });
  });
});
