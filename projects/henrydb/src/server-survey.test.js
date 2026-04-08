// server-survey.test.js — Survey/questionnaire data model
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15590;

describe('Survey System', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE surveys (id INTEGER, title TEXT, status TEXT, created_at TEXT)');
    await client.query('CREATE TABLE questions (id INTEGER, survey_id INTEGER, text TEXT, type TEXT, position INTEGER)');
    await client.query('CREATE TABLE responses (id INTEGER, survey_id INTEGER, respondent TEXT, submitted_at TEXT)');
    await client.query('CREATE TABLE answers (id INTEGER, response_id INTEGER, question_id INTEGER, answer TEXT)');
    
    await client.query("INSERT INTO surveys VALUES (1, 'Employee Satisfaction', 'active', '2026-04-01')");
    
    await client.query("INSERT INTO questions VALUES (1, 1, 'Rate your job satisfaction', 'scale', 1)");
    await client.query("INSERT INTO questions VALUES (2, 1, 'Rate work-life balance', 'scale', 2)");
    await client.query("INSERT INTO questions VALUES (3, 1, 'Department', 'choice', 3)");
    
    for (let i = 1; i <= 20; i++) {
      await client.query(`INSERT INTO responses VALUES (${i}, 1, 'employee_${i}', '2026-04-0${Math.min(i % 7 + 1, 8)}')`);
      const sat = 1 + (i % 5);
      const wlb = 1 + ((i + 2) % 5);
      const dept = ['Engineering', 'Marketing', 'Sales', 'HR'][i % 4];
      await client.query(`INSERT INTO answers VALUES (${i * 3 - 2}, ${i}, 1, '${sat}')`);
      await client.query(`INSERT INTO answers VALUES (${i * 3 - 1}, ${i}, 2, '${wlb}')`);
      await client.query(`INSERT INTO answers VALUES (${i * 3}, ${i}, 3, '${dept}')`);
    }
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('response count', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT COUNT(*) AS cnt FROM responses WHERE survey_id = 1');
    assert.strictEqual(parseInt(result.rows[0].cnt), 20);

    await client.end();
  });

  it('average satisfaction score', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT q.text, AVG(CAST(a.answer AS INTEGER)) AS avg_score FROM answers a JOIN questions q ON a.question_id = q.id WHERE q.type = 'scale' GROUP BY q.text"
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('responses by department', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT a.answer AS department, COUNT(*) AS count FROM answers a WHERE a.question_id = 3 GROUP BY a.answer ORDER BY count DESC"
    );
    assert.strictEqual(result.rows.length, 4);

    await client.end();
  });

  it('completion rate over time', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT submitted_at, COUNT(*) AS responses FROM responses GROUP BY submitted_at ORDER BY submitted_at'
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });
});
