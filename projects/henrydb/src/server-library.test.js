// server-library.test.js — Library catalog data model
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15591;

describe('Library Catalog', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE books (id INTEGER, title TEXT, author TEXT, genre TEXT, isbn TEXT, published_year INTEGER, copies INTEGER)');
    await client.query('CREATE TABLE members (id INTEGER, name TEXT, membership_type TEXT, joined_at TEXT)');
    await client.query('CREATE TABLE checkouts (id INTEGER, book_id INTEGER, member_id INTEGER, checked_out TEXT, due_date TEXT, returned_at TEXT)');
    
    await client.query("INSERT INTO books VALUES (1, 'The Great Gatsby', 'F. Scott Fitzgerald', 'fiction', '978-0-7432-7356-5', 1925, 3)");
    await client.query("INSERT INTO books VALUES (2, 'To Kill a Mockingbird', 'Harper Lee', 'fiction', '978-0-06-112008-4', 1960, 5)");
    await client.query("INSERT INTO books VALUES (3, 'Introduction to Algorithms', 'CLRS', 'nonfiction', '978-0-262-03384-8', 2009, 2)");
    await client.query("INSERT INTO books VALUES (4, 'Design Patterns', 'Gang of Four', 'nonfiction', '978-0-201-63361-0', 1994, 4)");
    await client.query("INSERT INTO books VALUES (5, '1984', 'George Orwell', 'fiction', '978-0-451-52493-5', 1949, 6)");
    
    await client.query("INSERT INTO members VALUES (1, 'Alice Reader', 'premium', '2025-01-01')");
    await client.query("INSERT INTO members VALUES (2, 'Bob Browser', 'basic', '2025-06-15')");
    await client.query("INSERT INTO members VALUES (3, 'Charlie Checker', 'premium', '2026-01-01')");
    
    await client.query("INSERT INTO checkouts VALUES (1, 1, 1, '2026-04-01', '2026-04-15', '2026-04-10')");
    await client.query("INSERT INTO checkouts VALUES (2, 3, 1, '2026-04-05', '2026-04-19', NULL)");
    await client.query("INSERT INTO checkouts VALUES (3, 2, 2, '2026-04-02', '2026-04-16', '2026-04-14')");
    await client.query("INSERT INTO checkouts VALUES (4, 5, 3, '2026-04-03', '2026-04-17', NULL)");
    await client.query("INSERT INTO checkouts VALUES (5, 1, 3, '2026-04-06', '2026-04-20', NULL)");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('search books by genre', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT title, author FROM books WHERE genre = 'fiction' ORDER BY title");
    assert.strictEqual(result.rows.length, 3);

    await client.end();
  });

  it('currently checked out books', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT b.title, m.name AS borrower, c.due_date FROM checkouts c JOIN books b ON c.book_id = b.id JOIN members m ON c.member_id = m.id WHERE c.returned_at IS NULL'
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('member checkout history', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT b.title, c.checked_out, c.returned_at FROM checkouts c JOIN books b ON c.book_id = b.id WHERE c.member_id = 1 ORDER BY c.checked_out'
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('most popular books', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT b.title, COUNT(c.id) AS checkouts FROM books b JOIN checkouts c ON b.id = c.book_id GROUP BY b.title ORDER BY checkouts DESC'
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });

  it('overdue books', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT b.title, m.name, c.due_date FROM checkouts c JOIN books b ON c.book_id = b.id JOIN members m ON c.member_id = m.id WHERE c.returned_at IS NULL AND c.due_date < '2026-04-20'"
    );
    assert.ok(result.rows.length >= 1);

    await client.end();
  });
});
