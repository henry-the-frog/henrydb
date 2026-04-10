// merge.test.js — Tests for three-way merge
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, readFileSync, mkdirSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { merge, findMergeBase } from './merge.js';
import { init, commit, log } from './commands.js';
import { addToIndex } from './index.js';
import { checkoutNewBranch, checkout } from './checkout.js';
import { resolveHead, getCurrentBranch } from './refs.js';
import { parseCommit, readObject } from './objects.js';

describe('Git Merge', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-'));
    workDir = tmp;
    gitDir = init(workDir);
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  describe('findMergeBase', () => {
    it('finds common ancestor of diverged branches', () => {
      writeFileSync(join(workDir, 'f.txt'), 'base');
      addToIndex(gitDir, workDir, 'f.txt');
      const base = commit(gitDir, workDir, 'Base');
      
      // Branch A
      checkoutNewBranch(gitDir, workDir, 'a');
      writeFileSync(join(workDir, 'f.txt'), 'a');
      addToIndex(gitDir, workDir, 'f.txt');
      const aCommit = commit(gitDir, workDir, 'A');
      
      // Branch B
      checkout(gitDir, workDir, 'main');
      checkoutNewBranch(gitDir, workDir, 'b');
      writeFileSync(join(workDir, 'f.txt'), 'b');
      addToIndex(gitDir, workDir, 'f.txt');
      const bCommit = commit(gitDir, workDir, 'B');
      
      const mergeBase = findMergeBase(gitDir, aCommit, bCommit);
      assert.strictEqual(mergeBase, base);
    });

    it('returns head when one is ancestor of other', () => {
      writeFileSync(join(workDir, 'f.txt'), 'v1');
      addToIndex(gitDir, workDir, 'f.txt');
      const c1 = commit(gitDir, workDir, 'First');
      
      writeFileSync(join(workDir, 'f.txt'), 'v2');
      addToIndex(gitDir, workDir, 'f.txt');
      const c2 = commit(gitDir, workDir, 'Second');
      
      const base = findMergeBase(gitDir, c1, c2);
      assert.strictEqual(base, c1);
    });
  });

  describe('fast-forward merge', () => {
    it('fast-forwards when possible', () => {
      writeFileSync(join(workDir, 'f.txt'), 'base');
      addToIndex(gitDir, workDir, 'f.txt');
      const baseHash = commit(gitDir, workDir, 'Base');
      
      checkoutNewBranch(gitDir, workDir, 'feature');
      writeFileSync(join(workDir, 'f.txt'), 'feature');
      addToIndex(gitDir, workDir, 'f.txt');
      const featureHash = commit(gitDir, workDir, 'Feature');
      
      // Go back to main and merge
      checkout(gitDir, workDir, 'main');
      const result = merge(gitDir, workDir, 'feature');
      
      assert.strictEqual(result.type, 'fast-forward');
      assert.strictEqual(result.hash, featureHash);
      assert.strictEqual(resolveHead(gitDir), featureHash);
      assert.strictEqual(readFileSync(join(workDir, 'f.txt'), 'utf8'), 'feature');
    });
  });

  describe('three-way merge', () => {
    it('merges non-conflicting changes', () => {
      writeFileSync(join(workDir, 'a.txt'), 'base');
      writeFileSync(join(workDir, 'b.txt'), 'base');
      addToIndex(gitDir, workDir, 'a.txt');
      addToIndex(gitDir, workDir, 'b.txt');
      commit(gitDir, workDir, 'Base');
      
      // Feature changes a.txt
      checkoutNewBranch(gitDir, workDir, 'feature');
      writeFileSync(join(workDir, 'a.txt'), 'feature-changed');
      addToIndex(gitDir, workDir, 'a.txt');
      commit(gitDir, workDir, 'Change A');
      
      // Main changes b.txt
      checkout(gitDir, workDir, 'main');
      writeFileSync(join(workDir, 'b.txt'), 'main-changed');
      addToIndex(gitDir, workDir, 'b.txt');
      commit(gitDir, workDir, 'Change B');
      
      // Merge feature into main
      const result = merge(gitDir, workDir, 'feature');
      
      assert.strictEqual(result.type, 'merge');
      assert.strictEqual(readFileSync(join(workDir, 'a.txt'), 'utf8'), 'feature-changed');
      assert.strictEqual(readFileSync(join(workDir, 'b.txt'), 'utf8'), 'main-changed');
      
      // Verify merge commit has two parents
      const commitData = parseCommit(readObject(gitDir, result.hash).content);
      assert.strictEqual(commitData.parents.length, 2);
    });

    it('detects conflicts', () => {
      writeFileSync(join(workDir, 'f.txt'), 'base content');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'Base');
      
      // Feature changes f.txt
      checkoutNewBranch(gitDir, workDir, 'feature');
      writeFileSync(join(workDir, 'f.txt'), 'feature content');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'Feature');
      
      // Main also changes f.txt
      checkout(gitDir, workDir, 'main');
      writeFileSync(join(workDir, 'f.txt'), 'main content');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'Main');
      
      // Merge — should conflict
      const result = merge(gitDir, workDir, 'feature');
      
      assert.strictEqual(result.type, 'conflict');
      assert.ok(result.conflicts.includes('f.txt'));
      
      // Check conflict markers in working tree
      const content = readFileSync(join(workDir, 'f.txt'), 'utf8');
      assert.ok(content.includes('<<<<<<<'));
      assert.ok(content.includes('======='));
      assert.ok(content.includes('>>>>>>>'));
    });

    it('handles new file in branch', () => {
      writeFileSync(join(workDir, 'base.txt'), 'base');
      addToIndex(gitDir, workDir, 'base.txt');
      commit(gitDir, workDir, 'Base');
      
      checkoutNewBranch(gitDir, workDir, 'feature');
      writeFileSync(join(workDir, 'new.txt'), 'new file');
      addToIndex(gitDir, workDir, 'new.txt');
      commit(gitDir, workDir, 'Add file');
      
      checkout(gitDir, workDir, 'main');
      writeFileSync(join(workDir, 'main.txt'), 'main file');
      addToIndex(gitDir, workDir, 'main.txt');
      commit(gitDir, workDir, 'Add main file');
      
      const result = merge(gitDir, workDir, 'feature');
      
      assert.strictEqual(result.type, 'merge');
      assert.strictEqual(readFileSync(join(workDir, 'new.txt'), 'utf8'), 'new file');
      assert.strictEqual(readFileSync(join(workDir, 'main.txt'), 'utf8'), 'main file');
    });
  });

  describe('already-up-to-date', () => {
    it('returns already-up-to-date when branch is ancestor', () => {
      writeFileSync(join(workDir, 'f.txt'), 'base');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'Base');
      
      checkoutNewBranch(gitDir, workDir, 'feature');
      // No new commits on feature
      
      checkout(gitDir, workDir, 'main');
      writeFileSync(join(workDir, 'f.txt'), 'advanced');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'Advance');
      
      const result = merge(gitDir, workDir, 'feature');
      assert.strictEqual(result.type, 'already-up-to-date');
    });
  });
});
