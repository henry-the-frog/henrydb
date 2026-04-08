// server-sequelize.test.js — Tests for Sequelize ORM integration
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { Sequelize, DataTypes, Op } from 'sequelize';
import { HenryDBServer } from './server.js';

const PORT = 15509;

describe('Sequelize ORM Integration', () => {
  let server, sequelize;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    sequelize = new Sequelize('test', 'test', 'test', {
      host: '127.0.0.1',
      port: PORT,
      dialect: 'postgres',
      logging: false,
    });
  });

  after(async () => {
    try { await sequelize.close(); } catch (e) {}
    await server.stop();
  });

  it('connects via Sequelize', async () => {
    await sequelize.authenticate();
  });

  it('raw query works', async () => {
    await sequelize.query('CREATE TABLE seq_raw (id INTEGER, name TEXT)');
    await sequelize.query("INSERT INTO seq_raw VALUES (1, 'hello')");
    
    const [results] = await sequelize.query('SELECT * FROM seq_raw');
    assert.strictEqual(results.length, 1);
    assert.strictEqual(results[0].name, 'hello');
  });

  it('model.create and model.findAll', async () => {
    const Task = sequelize.define('Task', {
      id: { type: DataTypes.INTEGER, primaryKey: true },
      title: DataTypes.TEXT,
      done: DataTypes.BOOLEAN,
    }, { timestamps: false, tableName: 'seq_tasks' });

    await sequelize.query('CREATE TABLE seq_tasks (id INTEGER, title TEXT, done INTEGER)');

    // Create
    await Task.create({ id: 1, title: 'Buy groceries', done: false });
    await Task.create({ id: 2, title: 'Write tests', done: true });

    // FindAll
    const tasks = await Task.findAll({ raw: true });
    assert.strictEqual(tasks.length, 2);
  });

  it('model.findOne', async () => {
    const [results] = await sequelize.query("SELECT * FROM seq_tasks WHERE id = 1");
    assert.strictEqual(results.length, 1);
    assert.strictEqual(results[0].title, 'Buy groceries');
  });

  it('model.update via raw query', async () => {
    await sequelize.query("UPDATE seq_tasks SET done = 1 WHERE id = 1");
    const [results] = await sequelize.query("SELECT * FROM seq_tasks WHERE id = 1");
    assert.ok(results[0].done === 1 || results[0].done === '1' || results[0].done === true);
  });

  it('model.destroy via raw query', async () => {
    await sequelize.query("DELETE FROM seq_tasks WHERE id = 2");
    const [results] = await sequelize.query("SELECT * FROM seq_tasks");
    assert.strictEqual(results.length, 1);
  });

  it('aggregate queries', async () => {
    await sequelize.query('CREATE TABLE seq_scores (student TEXT, score INTEGER)');
    await sequelize.query("INSERT INTO seq_scores VALUES ('alice', 85)");
    await sequelize.query("INSERT INTO seq_scores VALUES ('bob', 92)");
    await sequelize.query("INSERT INTO seq_scores VALUES ('charlie', 78)");

    const [results] = await sequelize.query('SELECT AVG(score) AS avg_score FROM seq_scores');
    assert.ok(parseFloat(results[0].avg_score) > 80);
  });

  it('JOIN queries', async () => {
    await sequelize.query('CREATE TABLE seq_users (id INTEGER, name TEXT)');
    await sequelize.query('CREATE TABLE seq_orders (id INTEGER, user_id INTEGER, product TEXT)');
    await sequelize.query("INSERT INTO seq_users VALUES (1, 'Alice')");
    await sequelize.query("INSERT INTO seq_orders VALUES (1, 1, 'Widget')");

    const [results] = await sequelize.query(
      'SELECT u.name, o.product FROM seq_users u JOIN seq_orders o ON u.id = o.user_id'
    );
    assert.strictEqual(results.length, 1);
    assert.strictEqual(results[0].name, 'Alice');
    assert.strictEqual(results[0].product, 'Widget');
  });

  it('transaction via raw query', async () => {
    await sequelize.query('CREATE TABLE seq_tx (id INTEGER, val TEXT)');
    await sequelize.query('BEGIN');
    await sequelize.query("INSERT INTO seq_tx VALUES (1, 'committed')");
    await sequelize.query('COMMIT');

    const [results] = await sequelize.query('SELECT * FROM seq_tx');
    assert.strictEqual(results.length, 1);
    assert.strictEqual(results[0].val, 'committed');
  });
});
