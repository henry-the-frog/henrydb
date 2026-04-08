// rope.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Rope } from './rope.js';

describe('Rope', () => {
  it('create from string', () => {
    const r = new Rope('hello');
    assert.equal(r.length, 5);
    assert.equal(r.toString(), 'hello');
  });

  it('charAt', () => {
    const r = new Rope('abcde');
    assert.equal(r.charAt(0), 'a');
    assert.equal(r.charAt(4), 'e');
    assert.equal(r.charAt(5), undefined);
  });

  it('concat', () => {
    const a = new Rope('hello');
    const b = new Rope(' world');
    const c = Rope.concat(a, b);
    assert.equal(c.toString(), 'hello world');
    assert.equal(c.length, 11);
  });

  it('split', () => {
    const r = new Rope('hello world');
    const [left, right] = r.split(5);
    assert.equal(left.toString(), 'hello');
    assert.equal(right.toString(), ' world');
  });

  it('insert', () => {
    const r = new Rope('helloworld');
    const result = r.insert(5, ' ');
    assert.equal(result.toString(), 'hello world');
  });

  it('delete', () => {
    const r = new Rope('hello beautiful world');
    const result = r.delete(5, 15);
    assert.equal(result.toString(), 'hello world');
  });

  it('substring', () => {
    const r = Rope.concat(new Rope('hello'), new Rope(' world'));
    assert.equal(r.substring(3, 8), 'lo wo');
  });

  it('many concatenations', () => {
    let r = new Rope('');
    for (let i = 0; i < 100; i++) r = Rope.concat(r, new Rope(`${i} `));
    assert.ok(r.length > 0);
    assert.ok(r.toString().startsWith('0 1 2'));
  });

  it('depth after concatenations', () => {
    let r = new Rope('a');
    for (let i = 0; i < 20; i++) r = Rope.concat(r, new Rope('b'));
    assert.ok(r.depth() <= 20);
  });

  it('benchmark: 10K inserts', () => {
    let r = new Rope('');
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) r = Rope.concat(r, new Rope('x'));
    console.log(`    Rope 10K concat: ${Date.now() - t0}ms, len=${r.length}`);
    assert.equal(r.length, 10000);
  });
});
