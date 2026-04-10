// reset.test.js — Tests for git reset
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, readFileSync, existsSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { reset } from './reset.js';
import { init, commit, log } from './commands.js';
import { addToIndex, readIndex, getStatus } from './index.js';
import { resolveHead } from './refs.js';

describe('Git Reset', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-reset-'));
    workDir = tmp;
    gitDir = init(workDir);
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  describe('soft reset', () => {
    it('moves HEAD without changing index or working tree', () => {
      writeFileSync(join(workDir, 'f.txt'), 'v1');
      addToIndex(gitDir, workDir, 'f.txt');
      const c1 = commit(gitDir, workDir, 'V1');
      
      writeFileSync(join(workDir, 'f.txt'), 'v2');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'V2');
      
      reset(gitDir, workDir, 'HEAD~1', 'soft');
      
      assert.strictEqual(resolveHead(gitDir), c1);
      // Working tree still has v2
      assert.strictEqual(readFileSync(join(workDir, 'f.txt'), 'utf8'), 'v2');
      // Index still has v2's hash
      const index = readIndex(gitDir);
      assert.ok(index.some(e => e.path === 'f.txt'));
    });
  });

  describe('mixed reset (default)', () => {
    it('moves HEAD and resets index', () => {
      writeFileSync(join(workDir, 'f.txt'), 'v1');
      addToIndex(gitDir, workDir, 'f.txt');
      const c1 = commit(gitDir, workDir, 'V1');
      
      writeFileSync(join(workDir, 'f.txt'), 'v2');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'V2');
      
      reset(gitDir, workDir, 'HEAD~1');
      
      assert.strictEqual(resolveHead(gitDir), c1);
      // Working tree still has v2
      assert.strictEqual(readFileSync(join(workDir, 'f.txt'), 'utf8'), 'v2');
    });
  });

  describe('hard reset', () => {
    it('moves HEAD, resets index and working tree', () => {
      writeFileSync(join(workDir, 'f.txt'), 'v1');
      addToIndex(gitDir, workDir, 'f.txt');
      const c1 = commit(gitDir, workDir, 'V1');
      
      writeFileSync(join(workDir, 'f.txt'), 'v2');
      addToIndex(gitDir, workDir, 'f.txt');
      commit(gitDir, workDir, 'V2');
      
      reset(gitDir, workDir, 'HEAD~1', 'hard');
      
      assert.strictEqual(resolveHead(gitDir), c1);
      // Working tree reset to v1
      assert.strictEqual(readFileSync(join(workDir, 'f.txt'), 'utf8'), 'v1');
    });

    it('removes files not in target commit', () => {
      writeFileSync(join(workDir, 'a.txt'), 'a');
      addToIndex(gitDir, workDir, 'a.txt');
      commit(gitDir, workDir, 'Base');
      
      writeFileSync(join(workDir, 'b.txt'), 'b');
      addToIndex(gitDir, workDir, 'b.txt');
      commit(gitDir, workDir, 'Add b');
      
      reset(gitDir, workDir, 'HEAD~1', 'hard');
      
      assert.ok(existsSync(join(workDir, 'a.txt')));
      assert.ok(!existsSync(join(workDir, 'b.txt')));
    });
  });

  describe('HEAD~ notation', () => {
    it('supports HEAD~N', () => {
      for (let i = 0; i < 5; i++) {
        writeFileSync(join(workDir, 'f.txt'), `v${i}`);
        addToIndex(gitDir, workDir, 'f.txt');
        commit(gitDir, workDir, `V${i}`);
      }
      
      reset(gitDir, workDir, 'HEAD~3', 'hard');
      
      const entries = log(gitDir);
      assert.strictEqual(entries.length, 2);
      assert.strictEqual(entries[0].message, 'V1');
    });
  });
});
