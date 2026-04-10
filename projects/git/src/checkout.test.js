// checkout.test.js — Tests for branch and checkout
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, readFileSync, mkdirSync, existsSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { checkout, checkoutNewBranch, flattenTree } from './checkout.js';
import { init, commit } from './commands.js';
import { addToIndex } from './index.js';
import { resolveHead, getCurrentBranch, listBranches, readHead } from './refs.js';

import { readObject, parseCommit } from './objects.js';

describe('Git Checkout', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-'));
    workDir = tmp;
    gitDir = init(workDir);
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  describe('checkoutNewBranch', () => {
    it('creates a new branch', () => {
      writeFileSync(join(workDir, 'file.txt'), 'content');
      addToIndex(gitDir, workDir, 'file.txt');
      const c1 = commit(gitDir, workDir, 'Initial');
      
      checkoutNewBranch(gitDir, workDir, 'feature');
      
      assert.strictEqual(getCurrentBranch(gitDir), 'feature');
      assert.strictEqual(resolveHead(gitDir), c1);
      assert.ok(listBranches(gitDir).includes('feature'));
    });

    it('throws for duplicate branch name', () => {
      writeFileSync(join(workDir, 'file.txt'), 'content');
      addToIndex(gitDir, workDir, 'file.txt');
      commit(gitDir, workDir, 'Initial');
      
      checkoutNewBranch(gitDir, workDir, 'feature');
      assert.throws(() => checkoutNewBranch(gitDir, workDir, 'feature'), /already exists/i);
    });

    it('throws without any commits', () => {
      assert.throws(() => checkoutNewBranch(gitDir, workDir, 'new'), /no commits/i);
    });
  });

  describe('checkout', () => {
    it('switches to existing branch', () => {
      writeFileSync(join(workDir, 'file.txt'), 'main content');
      addToIndex(gitDir, workDir, 'file.txt');
      const mainCommit = commit(gitDir, workDir, 'Main commit');
      
      checkoutNewBranch(gitDir, workDir, 'feature');
      writeFileSync(join(workDir, 'file.txt'), 'feature content');
      addToIndex(gitDir, workDir, 'file.txt');
      const featureCommit = commit(gitDir, workDir, 'Feature commit');
      
      // Switch back to main
      checkout(gitDir, workDir, 'main');
      
      assert.strictEqual(getCurrentBranch(gitDir), 'main');
      assert.strictEqual(resolveHead(gitDir), mainCommit);
      assert.strictEqual(readFileSync(join(workDir, 'file.txt'), 'utf8'), 'main content');
    });

    it('adds files from target branch', () => {
      writeFileSync(join(workDir, 'base.txt'), 'base');
      addToIndex(gitDir, workDir, 'base.txt');
      commit(gitDir, workDir, 'Base');
      
      checkoutNewBranch(gitDir, workDir, 'feature');
      writeFileSync(join(workDir, 'new.txt'), 'new file');
      addToIndex(gitDir, workDir, 'new.txt');
      commit(gitDir, workDir, 'Add file');
      
      checkout(gitDir, workDir, 'main');
      assert.ok(!existsSync(join(workDir, 'new.txt')));
      
      checkout(gitDir, workDir, 'feature');
      assert.ok(existsSync(join(workDir, 'new.txt')));
      assert.strictEqual(readFileSync(join(workDir, 'new.txt'), 'utf8'), 'new file');
    });

    it('removes files not in target branch', () => {
      writeFileSync(join(workDir, 'keep.txt'), 'keep');
      addToIndex(gitDir, workDir, 'keep.txt');
      commit(gitDir, workDir, 'Base');
      
      checkoutNewBranch(gitDir, workDir, 'feature');
      writeFileSync(join(workDir, 'extra.txt'), 'extra');
      addToIndex(gitDir, workDir, 'extra.txt');
      commit(gitDir, workDir, 'Add extra');
      
      checkout(gitDir, workDir, 'main');
      assert.ok(!existsSync(join(workDir, 'extra.txt')));
    });

    it('handles nested directories', () => {
      mkdirSync(join(workDir, 'src'));
      writeFileSync(join(workDir, 'src', 'main.js'), 'code');
      addToIndex(gitDir, workDir, 'src');
      commit(gitDir, workDir, 'Base');
      
      checkoutNewBranch(gitDir, workDir, 'feature');
      writeFileSync(join(workDir, 'src', 'main.js'), 'updated code');
      addToIndex(gitDir, workDir, 'src/main.js');
      commit(gitDir, workDir, 'Update');
      
      checkout(gitDir, workDir, 'main');
      assert.strictEqual(readFileSync(join(workDir, 'src', 'main.js'), 'utf8'), 'code');
    });

    it('allows detached HEAD checkout', () => {
      writeFileSync(join(workDir, 'file.txt'), 'v1');
      addToIndex(gitDir, workDir, 'file.txt');
      const c1 = commit(gitDir, workDir, 'V1');
      
      writeFileSync(join(workDir, 'file.txt'), 'v2');
      addToIndex(gitDir, workDir, 'file.txt');
      commit(gitDir, workDir, 'V2');
      
      // Checkout specific commit (detached)
      checkout(gitDir, workDir, c1);
      
      const head = readHead(gitDir);
      assert.strictEqual(head.type, 'hash');
      assert.strictEqual(head.hash, c1);
      assert.strictEqual(readFileSync(join(workDir, 'file.txt'), 'utf8'), 'v1');
    });

    it('throws for unknown target', () => {
      writeFileSync(join(workDir, 'file.txt'), 'x');
      addToIndex(gitDir, workDir, 'file.txt');
      commit(gitDir, workDir, 'Init');
      
      assert.throws(() => checkout(gitDir, workDir, 'nonexistent'), /unknown/i);
    });
  });

  describe('flattenTree', () => {
    it('flattens nested tree into file list', () => {
      mkdirSync(join(workDir, 'a'));
      mkdirSync(join(workDir, 'a', 'b'));
      writeFileSync(join(workDir, 'root.txt'), 'root');
      writeFileSync(join(workDir, 'a', 'mid.txt'), 'mid');
      writeFileSync(join(workDir, 'a', 'b', 'deep.txt'), 'deep');
      addToIndex(gitDir, workDir, 'root.txt');
      addToIndex(gitDir, workDir, 'a');
      const hash = commit(gitDir, workDir, 'Nested');
      
      const commitData = parseCommit(readObject(gitDir, hash).content);
      const files = flattenTree(gitDir, commitData.tree);
      
      assert.strictEqual(files.length, 3);
      assert.ok(files.some(f => f.path === 'root.txt'));
      assert.ok(files.some(f => f.path === 'a/mid.txt'));
      assert.ok(files.some(f => f.path === 'a/b/deep.txt'));
    });
  });
});
