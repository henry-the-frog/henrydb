// rope.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Rope } from './rope.js';

describe('Rope', () => {
  it('construct from string', () => {
    const r = new Rope('hello world');
    assert.equal(r.length, 11);
    assert.equal(r.toString(), 'hello world');
  });

  it('charAt', () => {
    const r = new Rope('abcdef');
    assert.equal(r.charAt(0), 'a');
    assert.equal(r.charAt(3), 'd');
    assert.equal(r.charAt(5), 'f');
  });

  it('substring', () => {
    const r = new Rope('hello world');
    assert.equal(r.substring(0, 5), 'hello');
    assert.equal(r.substring(6, 11), 'world');
  });

  it('insert', () => {
    const r = new Rope('hello world');
    r.insert(5, ' beautiful');
    assert.equal(r.toString(), 'hello beautiful world');
  });

  it('delete', () => {
    const r = new Rope('hello beautiful world');
    r.delete(5, 15);
    assert.equal(r.toString(), 'hello world');
  });

  it('append', () => {
    const r = new Rope('hello');
    r.append(' world');
    assert.equal(r.toString(), 'hello world');
  });

  it('large text: 100K characters', () => {
    const bigText = 'x'.repeat(100000);
    const r = new Rope(bigText);
    assert.equal(r.length, 100000);
    assert.equal(r.charAt(50000), 'x');
    
    r.insert(50000, 'INSERT');
    assert.equal(r.length, 100006);
    assert.equal(r.substring(49998, 50008), 'xxINSERTxx');
  });

  it('performance: many inserts', () => {
    const r = new Rope('');
    const t0 = performance.now();
    for (let i = 0; i < 1000; i++) {
      r.insert(Math.floor(r.length / 2), `chunk${i}`);
    }
    const elapsed = performance.now() - t0;
    console.log(`  1K middle-inserts: ${elapsed.toFixed(1)}ms, final length: ${r.length}`);
  });
});
