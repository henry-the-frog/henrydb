// repl.test.js — Tests for HenryDB REPL (non-interactive)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBRepl } from './repl.js';
import { Writable } from 'node:stream';

// Capture output
function createCapture() {
  let output = '';
  const stream = new Writable({
    write(chunk, encoding, callback) {
      output += chunk.toString();
      callback();
    }
  });
  return { stream, getOutput: () => output, clear: () => { output = ''; } };
}

describe('HenryDB REPL', () => {
  it('executes SQL and shows table output', () => {
    const cap = createCapture();
    const repl = new HenryDBRepl({ output: cap.stream });
    
    repl.execute('CREATE TABLE test (id INT PRIMARY KEY, name TEXT)');
    repl.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')");
    cap.clear();
    
    repl.execute('SELECT * FROM test ORDER BY id');
    const out = cap.getOutput();
    assert.ok(out.includes('Alice'));
    assert.ok(out.includes('Bob'));
    assert.ok(out.includes('2 row(s)'));
  });

  it('.tables lists tables', () => {
    const cap = createCapture();
    const repl = new HenryDBRepl({ output: cap.stream });
    
    repl.execute('CREATE TABLE users (id INT, name TEXT)');
    repl.execute('CREATE TABLE orders (id INT, amount INT)');
    cap.clear();
    
    repl.execute('.tables');
    const out = cap.getOutput();
    assert.ok(out.includes('users'));
    assert.ok(out.includes('orders'));
  });

  it('.schema shows table structure', () => {
    const cap = createCapture();
    const repl = new HenryDBRepl({ output: cap.stream });
    
    repl.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, salary INT)');
    cap.clear();
    
    repl.execute('.schema employees');
    const out = cap.getOutput();
    assert.ok(out.includes('id'));
    assert.ok(out.includes('name'));
    assert.ok(out.includes('salary'));
    assert.ok(out.includes('PRIMARY KEY'));
  });

  it('.help shows available commands', () => {
    const cap = createCapture();
    const repl = new HenryDBRepl({ output: cap.stream });
    
    repl.execute('.help');
    const out = cap.getOutput();
    assert.ok(out.includes('.tables'));
    assert.ok(out.includes('.schema'));
    assert.ok(out.includes('.quit'));
  });

  it('shows error for invalid SQL', () => {
    const cap = createCapture();
    const repl = new HenryDBRepl({ output: cap.stream });
    
    repl.execute('SELECT * FROM nonexistent');
    const out = cap.getOutput();
    assert.ok(out.includes('Error'));
  });

  it('.timing off hides timing', () => {
    const cap = createCapture();
    const repl = new HenryDBRepl({ output: cap.stream });
    
    repl.execute('.timing off');
    repl.execute('CREATE TABLE t (id INT)');
    cap.clear();
    
    repl.execute('SELECT * FROM t');
    const out = cap.getOutput();
    assert.ok(!out.includes('ms)'));
  });

  it('.quit returns quit signal', () => {
    const cap = createCapture();
    const repl = new HenryDBRepl({ output: cap.stream });
    
    const result = repl.execute('.quit');
    assert.equal(result, 'quit');
  });

  it('.history tracks commands', () => {
    const cap = createCapture();
    const repl = new HenryDBRepl({ output: cap.stream });
    
    repl.execute('CREATE TABLE t (id INT)');
    repl.execute('SELECT * FROM t');
    cap.clear();
    
    repl.execute('.history');
    const out = cap.getOutput();
    assert.ok(out.includes('CREATE TABLE'));
    assert.ok(out.includes('SELECT'));
  });

  it('unknown dot command shows error', () => {
    const cap = createCapture();
    const repl = new HenryDBRepl({ output: cap.stream });
    
    repl.execute('.unknown');
    const out = cap.getOutput();
    assert.ok(out.includes('Unknown command'));
  });
});
