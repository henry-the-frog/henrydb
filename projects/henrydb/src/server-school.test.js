// server-school.test.js — School/University data model
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15582;

describe('School/University', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE students (id INTEGER, name TEXT, major TEXT, gpa REAL, year INTEGER)');
    await client.query('CREATE TABLE courses (id INTEGER, name TEXT, department TEXT, credits INTEGER)');
    await client.query('CREATE TABLE enrollments (student_id INTEGER, course_id INTEGER, grade TEXT, semester TEXT)');
    
    await client.query("INSERT INTO students VALUES (1, 'Alice Chen', 'CS', 3.8, 3)");
    await client.query("INSERT INTO students VALUES (2, 'Bob Kumar', 'Math', 3.5, 2)");
    await client.query("INSERT INTO students VALUES (3, 'Charlie Park', 'CS', 3.2, 4)");
    await client.query("INSERT INTO students VALUES (4, 'Diana Liu', 'Physics', 3.9, 3)");
    
    await client.query("INSERT INTO courses VALUES (1, 'Algorithms', 'CS', 4)");
    await client.query("INSERT INTO courses VALUES (2, 'Database Systems', 'CS', 3)");
    await client.query("INSERT INTO courses VALUES (3, 'Linear Algebra', 'Math', 3)");
    await client.query("INSERT INTO courses VALUES (4, 'Quantum Mechanics', 'Physics', 4)");
    
    await client.query("INSERT INTO enrollments VALUES (1, 1, 'A', 'F2026')");
    await client.query("INSERT INTO enrollments VALUES (1, 2, 'A', 'F2026')");
    await client.query("INSERT INTO enrollments VALUES (2, 3, 'B+', 'F2026')");
    await client.query("INSERT INTO enrollments VALUES (2, 1, 'B', 'F2026')");
    await client.query("INSERT INTO enrollments VALUES (3, 2, 'A-', 'F2026')");
    await client.query("INSERT INTO enrollments VALUES (4, 4, 'A', 'F2026')");
    await client.query("INSERT INTO enrollments VALUES (4, 3, 'A', 'F2026')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('student course schedule', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT c.name AS course, c.credits, e.grade FROM enrollments e JOIN courses c ON e.course_id = c.id WHERE e.student_id = 1 AND e.semester = 'F2026'"
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('course roster', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT s.name, e.grade FROM enrollments e JOIN students s ON e.student_id = s.id WHERE e.course_id = 1'
    );
    assert.strictEqual(result.rows.length, 2); // Alice and Bob in Algorithms

    await client.end();
  });

  it('dean list (GPA > 3.7)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT name, gpa, major FROM students WHERE gpa > 3.7 ORDER BY gpa DESC");
    assert.strictEqual(result.rows.length, 2); // Diana and Alice

    await client.end();
  });

  it('enrollment by department', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT c.department, COUNT(*) AS enrollments FROM enrollments e JOIN courses c ON e.course_id = c.id GROUP BY c.department ORDER BY enrollments DESC'
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('students per major', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT major, COUNT(*) AS count, AVG(gpa) AS avg_gpa FROM students GROUP BY major ORDER BY count DESC'
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });
});
