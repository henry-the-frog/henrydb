// stress.test.js — Stress tests for tiny-git
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, readFileSync, mkdirSync, existsSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { init, commit, log } from './commands.js';
import { addToIndex, getStatus, readIndex } from './index.js';
import { checkoutNewBranch, checkout } from './checkout.js';
import { merge, findMergeBase } from './merge.js';
import { resolveHead, getCurrentBranch } from './refs.js';
import { hashObject, readObject, parseCommit, parseTree } from './objects.js';

describe('Stress Tests', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-stress-'));
    workDir = tmp;
    gitDir = init(workDir);
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  describe('Large repos', () => {
    it('handles 100 files', () => {
      for (let i = 0; i < 100; i++) {
        writeFileSync(join(workDir, `file-${i}.txt`), `Content of file ${i}`);
      }
      for (let i = 0; i < 100; i++) {
        addToIndex(gitDir, workDir, `file-${i}.txt`);
      }
      const hash = commit(gitDir, workDir, '100 files');
      const entries = readIndex(gitDir);
      assert.strictEqual(entries.length, 100);
      
      // Verify commit
      const commitData = parseCommit(readObject(gitDir, hash).content);
      const tree = parseTree(readObject(gitDir, commitData.tree).content);
      assert.strictEqual(tree.length, 100);
    });

    it('handles deep nesting (10 levels)', () => {
      let path = workDir;
      for (let i = 0; i < 10; i++) {
        path = join(path, `level${i}`);
        mkdirSync(path, { recursive: true });
      }
      writeFileSync(join(path, 'deep.txt'), 'deep content');
      addToIndex(gitDir, workDir, 'level0');
      const hash = commit(gitDir, workDir, 'Deep nesting');
      
      const entries = readIndex(gitDir);
      assert.ok(entries[0].path.includes('deep.txt'));
      assert.ok(entries[0].path.split('/').length >= 10);
    });

    it('handles 50 commits', () => {
      for (let i = 0; i < 50; i++) {
        writeFileSync(join(workDir, 'file.txt'), `Version ${i}`);
        addToIndex(gitDir, workDir, 'file.txt');
        commit(gitDir, workDir, `Commit ${i}`);
      }
      
      const entries = log(gitDir);
      assert.strictEqual(entries.length, 50);
      assert.strictEqual(entries[0].message, 'Commit 49');
      assert.strictEqual(entries[49].message, 'Commit 0');
    });
  });

  describe('Edge cases', () => {
    it('handles binary content', () => {
      const binary = Buffer.from([0, 1, 2, 3, 0xFF, 0xFE, 0x00, 0xBE, 0xEF]);
      writeFileSync(join(workDir, 'binary.bin'), binary);
      addToIndex(gitDir, workDir, 'binary.bin');
      const hash = commit(gitDir, workDir, 'Binary file');
      
      // Checkout to another branch and back
      checkoutNewBranch(gitDir, workDir, 'other');
      checkout(gitDir, workDir, 'main');
      
      const content = readFileSync(join(workDir, 'binary.bin'));
      assert.deepStrictEqual(content, binary);
    });

    it('handles empty file', () => {
      writeFileSync(join(workDir, 'empty.txt'), '');
      addToIndex(gitDir, workDir, 'empty.txt');
      const hash = commit(gitDir, workDir, 'Empty file');
      
      const entries = readIndex(gitDir);
      assert.strictEqual(entries[0].size, 0);
      // Empty blob hash should be the well-known constant
      assert.strictEqual(entries[0].hash, 'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391');
    });

    it('handles large file (1MB)', () => {
      const content = 'x'.repeat(1024 * 1024);
      writeFileSync(join(workDir, 'large.txt'), content);
      addToIndex(gitDir, workDir, 'large.txt');
      commit(gitDir, workDir, 'Large file');
      
      // Verify it reads back correctly
      checkoutNewBranch(gitDir, workDir, 'other');
      writeFileSync(join(workDir, 'large.txt'), 'small now');
      addToIndex(gitDir, workDir, 'large.txt');
      commit(gitDir, workDir, 'Overwrite');
      
      checkout(gitDir, workDir, 'main');
      const readBack = readFileSync(join(workDir, 'large.txt'), 'utf8');
      assert.strictEqual(readBack.length, 1024 * 1024);
    });

    it('handles filenames with spaces', () => {
      writeFileSync(join(workDir, 'my file.txt'), 'content');
      addToIndex(gitDir, workDir, 'my file.txt');
      commit(gitDir, workDir, 'File with spaces');
      
      const entries = readIndex(gitDir);
      assert.ok(entries.some(e => e.path === 'my file.txt'));
    });

    it('handles Unicode content', () => {
      writeFileSync(join(workDir, 'unicode.txt'), '日本語テスト 🎉 émojis');
      addToIndex(gitDir, workDir, 'unicode.txt');
      commit(gitDir, workDir, 'Unicode content');
      
      checkoutNewBranch(gitDir, workDir, 'other');
      checkout(gitDir, workDir, 'main');
      
      assert.strictEqual(readFileSync(join(workDir, 'unicode.txt'), 'utf8'), '日本語テスト 🎉 émojis');
    });

    it('handles commit message with newlines', () => {
      writeFileSync(join(workDir, 'file.txt'), 'content');
      addToIndex(gitDir, workDir, 'file.txt');
      commit(gitDir, workDir, 'Title\n\nBody paragraph 1.\n\nBody paragraph 2.');
      
      const entries = log(gitDir);
      assert.ok(entries[0].message.includes('Title'));
      assert.ok(entries[0].message.includes('Body paragraph 2.'));
    });
  });

  describe('Branch workflows', () => {
    it('handles branch diverge and merge', () => {
      // Create base
      writeFileSync(join(workDir, 'shared.txt'), 'base');
      writeFileSync(join(workDir, 'a.txt'), 'a-base');
      writeFileSync(join(workDir, 'b.txt'), 'b-base');
      addToIndex(gitDir, workDir, 'shared.txt');
      addToIndex(gitDir, workDir, 'a.txt');
      addToIndex(gitDir, workDir, 'b.txt');
      commit(gitDir, workDir, 'Base');
      
      // Feature branch: modify a.txt
      checkoutNewBranch(gitDir, workDir, 'feature-a');
      writeFileSync(join(workDir, 'a.txt'), 'a-feature');
      addToIndex(gitDir, workDir, 'a.txt');
      commit(gitDir, workDir, 'Feature A');
      
      // Back to main, modify b.txt
      checkout(gitDir, workDir, 'main');
      writeFileSync(join(workDir, 'b.txt'), 'b-main');
      addToIndex(gitDir, workDir, 'b.txt');
      commit(gitDir, workDir, 'Main B');
      
      // Merge
      const result = merge(gitDir, workDir, 'feature-a');
      assert.strictEqual(result.type, 'merge');
      
      // Verify all files
      assert.strictEqual(readFileSync(join(workDir, 'a.txt'), 'utf8'), 'a-feature');
      assert.strictEqual(readFileSync(join(workDir, 'b.txt'), 'utf8'), 'b-main');
      assert.strictEqual(readFileSync(join(workDir, 'shared.txt'), 'utf8'), 'base');
    });

    it('handles multiple merges', () => {
      writeFileSync(join(workDir, 'f.txt'), 'v0');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'V0');
      
      // Branch 1
      checkoutNewBranch(gitDir, workDir, 'b1');
      writeFileSync(join(workDir, 'b1.txt'), 'b1');
      addToIndex(gitDir, workDir, 'b1.txt');
      commit(gitDir, workDir, 'B1');
      
      // Back to main, merge b1
      checkout(gitDir, workDir, 'main');
      merge(gitDir, workDir, 'b1');
      
      // Branch 2
      checkoutNewBranch(gitDir, workDir, 'b2');
      writeFileSync(join(workDir, 'b2.txt'), 'b2');
      addToIndex(gitDir, workDir, 'b2.txt');
      commit(gitDir, workDir, 'B2');
      
      // Back to main, merge b2
      checkout(gitDir, workDir, 'main');
      const result = merge(gitDir, workDir, 'b2');
      
      // Both files should exist
      assert.ok(existsSync(join(workDir, 'b1.txt')));
      assert.ok(existsSync(join(workDir, 'b2.txt')));
    });

    it('detached HEAD commit and reattach', () => {
      writeFileSync(join(workDir, 'f.txt'), 'v1');
      addToIndex(gitDir, workDir, 'f.txt');
      const c1 = commit(gitDir, workDir, 'V1');
      
      writeFileSync(join(workDir, 'f.txt'), 'v2');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'V2');
      
      // Detach to c1
      checkout(gitDir, workDir, c1);
      assert.strictEqual(getCurrentBranch(gitDir), null);
      assert.strictEqual(readFileSync(join(workDir, 'f.txt'), 'utf8'), 'v1');
      
      // Back to main
      checkout(gitDir, workDir, 'main');
      assert.strictEqual(getCurrentBranch(gitDir), 'main');
      assert.strictEqual(readFileSync(join(workDir, 'f.txt'), 'utf8'), 'v2');
    });
  });

  describe('Diff edge cases', () => {
    it('handles identical file modification (no actual change)', () => {
      writeFileSync(join(workDir, 'f.txt'), 'same');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'First');
      
      // "Modify" with same content
      writeFileSync(join(workDir, 'f.txt'), 'same');
      const status = getStatus(gitDir, workDir, null);
      // Should NOT be modified since content hash is the same
      assert.strictEqual(status.modified.length, 0);
    });
  });

  describe('Performance', () => {
    it('completes 100 commits in under 2 seconds', () => {
      const start = Date.now();
      for (let i = 0; i < 100; i++) {
        writeFileSync(join(workDir, 'counter.txt'), String(i));
        addToIndex(gitDir, workDir, 'counter.txt');
        commit(gitDir, workDir, `#${i}`);
      }
      const elapsed = Date.now() - start;
      assert.ok(elapsed < 2000, `100 commits took ${elapsed}ms (limit: 2000ms)`);
    });
  });
});
