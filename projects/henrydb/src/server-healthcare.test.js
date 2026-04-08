// server-healthcare.test.js — Healthcare data patterns
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15537;

describe('Healthcare Data', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE patients (id INTEGER, name TEXT, dob TEXT, blood_type TEXT)');
    await client.query('CREATE TABLE doctors (id INTEGER, name TEXT, specialty TEXT)');
    await client.query('CREATE TABLE appointments (id INTEGER, patient_id INTEGER, doctor_id INTEGER, date TEXT, type TEXT, notes TEXT)');
    await client.query('CREATE TABLE vitals (id INTEGER, patient_id INTEGER, metric TEXT, value REAL, unit TEXT, recorded_at TEXT)');
    
    await client.query("INSERT INTO patients VALUES (1, 'John Smith', '1985-06-15', 'A+')");
    await client.query("INSERT INTO patients VALUES (2, 'Jane Doe', '1992-03-22', 'O-')");
    await client.query("INSERT INTO patients VALUES (3, 'Bob Wilson', '1978-11-30', 'B+')");
    
    await client.query("INSERT INTO doctors VALUES (1, 'Dr. Adams', 'Cardiology')");
    await client.query("INSERT INTO doctors VALUES (2, 'Dr. Baker', 'General Practice')");
    
    await client.query("INSERT INTO appointments VALUES (1, 1, 1, '2026-04-01', 'checkup', 'Annual heart checkup')");
    await client.query("INSERT INTO appointments VALUES (2, 2, 2, '2026-04-02', 'sick', 'Flu symptoms')");
    await client.query("INSERT INTO appointments VALUES (3, 1, 2, '2026-04-05', 'followup', 'Blood work results')");
    await client.query("INSERT INTO appointments VALUES (4, 3, 1, '2026-04-06', 'checkup', 'Routine')");
    
    // Vitals
    await client.query("INSERT INTO vitals VALUES (1, 1, 'blood_pressure_sys', 120, 'mmHg', '2026-04-01')");
    await client.query("INSERT INTO vitals VALUES (2, 1, 'blood_pressure_dia', 80, 'mmHg', '2026-04-01')");
    await client.query("INSERT INTO vitals VALUES (3, 1, 'heart_rate', 72, 'bpm', '2026-04-01')");
    await client.query("INSERT INTO vitals VALUES (4, 2, 'temperature', 101.2, 'F', '2026-04-02')");
    await client.query("INSERT INTO vitals VALUES (5, 2, 'heart_rate', 88, 'bpm', '2026-04-02')");
    await client.query("INSERT INTO vitals VALUES (6, 3, 'blood_pressure_sys', 145, 'mmHg', '2026-04-06')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('patient appointment history', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT a.date, d.name AS doctor, a.type, a.notes FROM appointments a JOIN doctors d ON a.doctor_id = d.id WHERE a.patient_id = 1 ORDER BY a.date"
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('doctor schedule', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT d.name, p.name AS patient, a.date, a.type FROM appointments a JOIN doctors d ON a.doctor_id = d.id JOIN patients p ON a.patient_id = p.id WHERE d.name = 'Dr. Adams' ORDER BY a.date"
    );
    assert.ok(result.rows.length >= 1);

    await client.end();
  });

  it('patient vitals', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT p.name, v.metric, v.value, v.unit FROM vitals v JOIN patients p ON v.patient_id = p.id WHERE p.id = 1 ORDER BY v.metric'
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });

  it('abnormal vitals alert', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // High blood pressure or fever
    const result = await client.query(
      "SELECT p.name, v.metric, v.value FROM vitals v JOIN patients p ON v.patient_id = p.id WHERE (v.metric = 'blood_pressure_sys' AND v.value > 140) OR (v.metric = 'temperature' AND v.value > 100)"
    );
    assert.ok(result.rows.length >= 2); // Bob's BP and Jane's temperature

    await client.end();
  });

  it('appointment summary by doctor', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT d.name, d.specialty, COUNT(a.id) AS appointment_count FROM doctors d JOIN appointments a ON d.id = a.doctor_id GROUP BY d.name, d.specialty ORDER BY appointment_count DESC'
    );
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });
});
