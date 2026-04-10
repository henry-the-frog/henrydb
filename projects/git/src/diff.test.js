// diff.test.js — Tests for Myers diff algorithm
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { myersDiff, diffLines, formatUnifiedDiff } from './diff.js';

describe('Myers Diff', () => {
  describe('myersDiff', () => {
    it('handles identical sequences', () => {
      const ops = myersDiff(['a', 'b', 'c'], ['a', 'b', 'c']);
      assert.ok(ops.every(op => op.type === 'equal'));
      assert.strictEqual(ops.length, 3);
    });

    it('handles empty → non-empty', () => {
      const ops = myersDiff([], ['a', 'b']);
      assert.strictEqual(ops.length, 2);
      assert.ok(ops.every(op => op.type === 'insert'));
    });

    it('handles non-empty → empty', () => {
      const ops = myersDiff(['a', 'b'], []);
      assert.strictEqual(ops.length, 2);
      assert.ok(ops.every(op => op.type === 'delete'));
    });

    it('handles single insertion', () => {
      const ops = myersDiff(['a', 'c'], ['a', 'b', 'c']);
      const inserts = ops.filter(op => op.type === 'insert');
      assert.strictEqual(inserts.length, 1);
      assert.strictEqual(inserts[0].value, 'b');
    });

    it('handles single deletion', () => {
      const ops = myersDiff(['a', 'b', 'c'], ['a', 'c']);
      const deletes = ops.filter(op => op.type === 'delete');
      assert.strictEqual(deletes.length, 1);
      assert.strictEqual(deletes[0].value, 'b');
    });

    it('handles replacement', () => {
      const ops = myersDiff(['a', 'b', 'c'], ['a', 'x', 'c']);
      const deletes = ops.filter(op => op.type === 'delete');
      const inserts = ops.filter(op => op.type === 'insert');
      assert.strictEqual(deletes.length, 1);
      assert.strictEqual(inserts.length, 1);
      assert.strictEqual(deletes[0].value, 'b');
      assert.strictEqual(inserts[0].value, 'x');
    });

    it('handles completely different sequences', () => {
      const ops = myersDiff(['a', 'b'], ['c', 'd']);
      const deletes = ops.filter(op => op.type === 'delete');
      const inserts = ops.filter(op => op.type === 'insert');
      assert.strictEqual(deletes.length, 2);
      assert.strictEqual(inserts.length, 2);
    });

    it('finds shortest edit script', () => {
      // Classic example: ABCABBA → CBABAC
      const ops = myersDiff(
        ['A', 'B', 'C', 'A', 'B', 'B', 'A'],
        ['C', 'B', 'A', 'B', 'A', 'C']
      );
      const edits = ops.filter(op => op.type !== 'equal');
      // Shortest edit distance is 5 (known)
      assert.ok(edits.length <= 5);
    });
  });

  describe('diffLines', () => {
    it('diffs text by lines', () => {
      const ops = diffLines('hello\nworld', 'hello\nplanet');
      const changed = ops.filter(op => op.type !== 'equal');
      assert.ok(changed.length > 0);
    });

    it('handles empty old text', () => {
      const ops = diffLines('', 'new line');
      assert.ok(ops.some(op => op.type === 'insert'));
    });

    it('handles same text', () => {
      const ops = diffLines('same\ntext', 'same\ntext');
      assert.ok(ops.every(op => op.type === 'equal'));
    });
  });

  describe('formatUnifiedDiff', () => {
    it('produces unified diff format', () => {
      const ops = diffLines('line1\nline2\nline3', 'line1\nmodified\nline3');
      const output = formatUnifiedDiff('file.txt', 'file.txt', ops);
      
      assert.ok(output.includes('--- a/file.txt'));
      assert.ok(output.includes('+++ b/file.txt'));
      assert.ok(output.includes('@@'));
      assert.ok(output.includes('-line2'));
      assert.ok(output.includes('+modified'));
    });

    it('shows context lines', () => {
      const old = 'a\nb\nc\nd\ne\nf\ng\nh';
      const _new = 'a\nb\nc\nX\ne\nf\ng\nh';
      const ops = diffLines(old, _new);
      const output = formatUnifiedDiff('f.txt', 'f.txt', ops);
      
      // Should have context around the change
      assert.ok(output.includes(' c'));
      assert.ok(output.includes('-d'));
      assert.ok(output.includes('+X'));
      assert.ok(output.includes(' e'));
    });

    it('returns empty for identical files', () => {
      const ops = diffLines('same', 'same');
      const output = formatUnifiedDiff('f.txt', 'f.txt', ops);
      assert.strictEqual(output, '');
    });

    it('handles new file', () => {
      const ops = diffLines('', 'new content\nline 2');
      const output = formatUnifiedDiff('/dev/null', 'new.txt', ops);
      assert.ok(output.includes('+new content'));
      assert.ok(output.includes('+line 2'));
    });

    it('handles deleted file', () => {
      const ops = diffLines('old content\nline 2', '');
      const output = formatUnifiedDiff('old.txt', '/dev/null', ops);
      assert.ok(output.includes('-old content'));
    });
  });
});
