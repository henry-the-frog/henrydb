// er-diagram.test.js — Tests for ER diagram generator
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { generateERDiagram, extractSchema } from './er-diagram.js';
import { Database } from './db.js';

describe('ER Diagram Generator', () => {
  it('generates SVG for simple schema', () => {
    const tables = [
      { name: 'users', columns: [
        { name: 'id', type: 'INTEGER', pk: true },
        { name: 'name', type: 'TEXT' },
        { name: 'email', type: 'TEXT' },
      ]},
      { name: 'posts', columns: [
        { name: 'id', type: 'INTEGER', pk: true },
        { name: 'user_id', type: 'INTEGER', fk: 'users' },
        { name: 'title', type: 'TEXT' },
      ]},
    ];
    
    const svg = generateERDiagram(tables);
    assert.ok(svg.includes('<svg'));
    assert.ok(svg.includes('users'));
    assert.ok(svg.includes('posts'));
    assert.ok(svg.includes('🔑')); // PK icon
    assert.ok(svg.includes('🔗')); // FK icon
  });

  it('handles empty tables array', () => {
    const svg = generateERDiagram([]);
    assert.ok(svg.includes('No tables'));
  });

  it('handles single table', () => {
    const tables = [
      { name: 'config', columns: [
        { name: 'key', type: 'TEXT', pk: true },
        { name: 'value', type: 'TEXT' },
      ]},
    ];
    const svg = generateERDiagram(tables);
    assert.ok(svg.includes('config'));
  });

  it('handles many tables in grid layout', () => {
    const tables = Array.from({ length: 6 }, (_, i) => ({
      name: `table_${i}`,
      columns: [{ name: 'id', type: 'INTEGER', pk: true }],
    }));
    const svg = generateERDiagram(tables);
    assert.ok(svg.includes('table_0'));
    assert.ok(svg.includes('table_5'));
  });

  it('draws relationship lines for FK references', () => {
    const tables = [
      { name: 'authors', columns: [{ name: 'id', type: 'INTEGER', pk: true }] },
      { name: 'books', columns: [
        { name: 'id', type: 'INTEGER', pk: true },
        { name: 'author_id', type: 'INTEGER', fk: 'authors' },
      ]},
    ];
    const svg = generateERDiagram(tables);
    assert.ok(svg.includes('path')); // Relationship line
    assert.ok(svg.includes('arrowhead')); // Arrow marker
  });

  it('extractSchema gets tables from Database', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute('CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)');
    db.execute("INSERT INTO posts VALUES (1, 1, 'Hello')");
    
    const schema = extractSchema(db);
    assert.equal(schema.length, 2);
    assert.equal(schema[0].name, 'users');
    assert.ok(schema[0].columns.length >= 2);
  });

  it('generates complete diagram from Database', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'a@b.com')");
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)');
    db.execute("INSERT INTO orders VALUES (1, 1, 99.99)");
    
    const schema = extractSchema(db);
    const svg = generateERDiagram(schema);
    assert.ok(svg.includes('users'));
    assert.ok(svg.includes('orders'));
    assert.ok(svg.length > 500); // Should be a substantial SVG
  });

  it('handles special characters in names', () => {
    const tables = [
      { name: 'user&data', columns: [
        { name: 'id<pk>', type: 'INT"EGER' },
      ]},
    ];
    const svg = generateERDiagram(tables);
    assert.ok(svg.includes('&amp;'));
    assert.ok(svg.includes('&lt;'));
  });
});
