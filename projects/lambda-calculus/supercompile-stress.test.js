import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Num, Var, Add, Mul, If0, drive, homeomorphicEmbedding, supercompile } from './supercompile.js';

describe('Supercompiler', () => {
  describe('drive (symbolic evaluation)', () => {
    it('evaluates constant addition', () => {
      const result = drive(new Add(new Num(3), new Num(4)));
      assert.equal(result.tag, 'Num');
      assert.equal(result.n, 7);
    });

    it('evaluates constant multiplication', () => {
      const result = drive(new Mul(new Num(3), new Num(4)));
      assert.equal(result.tag, 'Num');
      assert.equal(result.n, 12);
    });

    it('eliminates add-zero', () => {
      const result = drive(new Add(new Num(0), new Var('x')));
      assert.equal(result.tag, 'Var');
      assert.equal(result.name, 'x');
    });

    it('eliminates mul-by-zero', () => {
      const result = drive(new Mul(new Num(0), new Var('x')));
      assert.equal(result.tag, 'Num');
      assert.equal(result.n, 0);
    });

    it('eliminates mul-by-one', () => {
      const result = drive(new Mul(new Num(1), new Var('x')));
      assert.equal(result.tag, 'Var');
      assert.equal(result.name, 'x');
    });

    it('resolves if0 with known condition', () => {
      const result = drive(new If0(new Num(0), new Num(1), new Num(2)));
      assert.equal(result.n, 1);
    });

    it('resolves if0 with non-zero condition', () => {
      const result = drive(new If0(new Num(5), new Num(1), new Num(2)));
      assert.equal(result.n, 2);
    });

    it('preserves symbolic if0', () => {
      const result = drive(new If0(new Var('x'), new Num(1), new Num(2)));
      assert.equal(result.tag, 'If0');
    });

    it('drives sub-expressions recursively', () => {
      // (0 + x) + (3 + 4) → x + 7
      const result = drive(new Add(new Add(new Num(0), new Var('x')), new Add(new Num(3), new Num(4))));
      assert.equal(result.tag, 'Add');
      assert.equal(result.left.name, 'x');
      assert.equal(result.right.n, 7);
    });
  });

  describe('homeomorphicEmbedding', () => {
    it('number embeds in larger number', () => {
      assert.ok(homeomorphicEmbedding(new Num(3), new Num(5)));
    });

    it('number does not embed in smaller', () => {
      assert.ok(!homeomorphicEmbedding(new Num(5), new Num(3)));
    });

    it('variable embeds in same variable', () => {
      assert.ok(homeomorphicEmbedding(new Var('x'), new Var('x')));
    });

    it('variable does not embed in different variable', () => {
      assert.ok(!homeomorphicEmbedding(new Var('x'), new Var('y')));
    });

    it('expression embeds in sub-expression (diving)', () => {
      // x embeds in (x + 1) 
      assert.ok(homeomorphicEmbedding(new Var('x'), new Add(new Var('x'), new Num(1))));
    });

    it('expression embeds in deep sub-expression', () => {
      // x embeds in ((x + 1) * 2)
      assert.ok(homeomorphicEmbedding(new Var('x'), new Mul(new Add(new Var('x'), new Num(1)), new Num(2))));
    });
  });

  describe('supercompile', () => {
    it('simplifies constant expression', () => {
      const { result } = supercompile(new Add(new Num(1), new Add(new Num(2), new Num(3))));
      assert.equal(result.tag, 'Num');
      assert.equal(result.n, 6);
    });

    it('simplifies with algebraic identities', () => {
      // (x + 0) * 1 → x
      const { result } = supercompile(new Mul(new Add(new Var('x'), new Num(0)), new Num(1)));
      assert.equal(result.tag, 'Var');
      assert.equal(result.name, 'x');
    });

    it('resolves nested if0 with constants', () => {
      // if0(0, 42, if0(1, 10, 20)) → 42
      const { result } = supercompile(new If0(new Num(0), new Num(42), new If0(new Num(1), new Num(10), new Num(20))));
      assert.equal(result.n, 42);
    });

    it('terminates on growing expressions (whistle)', () => {
      // x + x + x + ... would grow without whistle
      let expr = new Var('x');
      for (let i = 0; i < 10; i++) expr = new Add(expr, new Var('x'));
      const { result } = supercompile(expr, 50);
      // Should terminate (whistle fires) without stack overflow
      assert.ok(result);
    });

    it('handles deeply nested arithmetic', () => {
      // ((1 + 2) * (3 + 4)) + ((5 * 0) + (6 * 1)) → 21 + 6 = 27
      const { result } = supercompile(
        new Add(
          new Mul(new Add(new Num(1), new Num(2)), new Add(new Num(3), new Num(4))),
          new Add(new Mul(new Num(5), new Num(0)), new Mul(new Num(6), new Num(1)))
        )
      );
      assert.equal(result.tag, 'Num');
      assert.equal(result.n, 27);
    });
  });
});
