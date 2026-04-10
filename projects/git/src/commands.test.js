// commands.test.js — Integration tests for git commands
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { init, commit, log, formatLog } from './commands.js';
import { addToIndex, readIndex, getStatus } from './index.js';
import { readObject, parseTree, parseCommit } from './objects.js';
import { resolveHead, getCurrentBranch, readHead } from './refs.js';

describe('Git Commands', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-'));
    workDir = tmp;
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  describe('init', () => {
    it('creates .git directory structure', () => {
      gitDir = init(workDir);
      assert.ok(gitDir.endsWith('.git'));
      
      const head = readHead(gitDir);
      assert.strictEqual(head.type, 'ref');
      assert.strictEqual(head.ref, 'refs/heads/main');
    });

    it('sets current branch to main', () => {
      gitDir = init(workDir);
      assert.strictEqual(getCurrentBranch(gitDir), 'main');
    });
  });

  describe('commit', () => {
    beforeEach(() => {
      gitDir = init(workDir);
    });

    it('creates first commit', () => {
      writeFileSync(join(workDir, 'hello.txt'), 'hello world');
      addToIndex(gitDir, workDir, 'hello.txt');
      
      const hash = commit(gitDir, workDir, 'Initial commit', 'Test', 'test@test.com');
      assert.strictEqual(hash.length, 40);
      
      // Verify HEAD points to commit
      assert.strictEqual(resolveHead(gitDir), hash);
    });

    it('creates commit with correct tree', () => {
      writeFileSync(join(workDir, 'a.txt'), 'content a');
      writeFileSync(join(workDir, 'b.txt'), 'content b');
      addToIndex(gitDir, workDir, 'a.txt');
      addToIndex(gitDir, workDir, 'b.txt');
      
      const hash = commit(gitDir, workDir, 'Two files');
      const obj = readObject(gitDir, hash);
      const commitData = parseCommit(obj.content);
      
      const tree = parseTree(readObject(gitDir, commitData.tree).content);
      assert.strictEqual(tree.length, 2);
      assert.ok(tree.some(e => e.name === 'a.txt'));
      assert.ok(tree.some(e => e.name === 'b.txt'));
    });

    it('links to parent commit', () => {
      writeFileSync(join(workDir, 'file.txt'), 'v1');
      addToIndex(gitDir, workDir, 'file.txt');
      const c1 = commit(gitDir, workDir, 'First');
      
      writeFileSync(join(workDir, 'file.txt'), 'v2');
      addToIndex(gitDir, workDir, 'file.txt');
      const c2 = commit(gitDir, workDir, 'Second');
      
      const commitData = parseCommit(readObject(gitDir, c2).content);
      assert.strictEqual(commitData.parents.length, 1);
      assert.strictEqual(commitData.parents[0], c1);
    });

    it('throws on empty index', () => {
      assert.throws(() => commit(gitDir, workDir, 'Empty'), /nothing to commit/i);
    });

    it('handles nested directories', () => {
      mkdirSync(join(workDir, 'src'));
      writeFileSync(join(workDir, 'README.md'), '# Project');
      writeFileSync(join(workDir, 'src', 'main.js'), 'hello');
      addToIndex(gitDir, workDir, 'README.md');
      addToIndex(gitDir, workDir, 'src/main.js');
      
      const hash = commit(gitDir, workDir, 'Nested');
      const commitData = parseCommit(readObject(gitDir, hash).content);
      const tree = parseTree(readObject(gitDir, commitData.tree).content);
      
      assert.strictEqual(tree.length, 2);
      const srcEntry = tree.find(e => e.name === 'src');
      assert.ok(srcEntry);
      assert.strictEqual(srcEntry.mode, '040000');
    });
  });

  describe('log', () => {
    beforeEach(() => {
      gitDir = init(workDir);
    });

    it('returns empty log for no commits', () => {
      const entries = log(gitDir);
      assert.strictEqual(entries.length, 0);
    });

    it('returns single commit', () => {
      writeFileSync(join(workDir, 'file.txt'), 'test');
      addToIndex(gitDir, workDir, 'file.txt');
      commit(gitDir, workDir, 'Only commit');
      
      const entries = log(gitDir);
      assert.strictEqual(entries.length, 1);
      assert.strictEqual(entries[0].message, 'Only commit');
    });

    it('returns commits in reverse order', () => {
      writeFileSync(join(workDir, 'file.txt'), 'v1');
      addToIndex(gitDir, workDir, 'file.txt');
      commit(gitDir, workDir, 'First');
      
      writeFileSync(join(workDir, 'file.txt'), 'v2');
      addToIndex(gitDir, workDir, 'file.txt');
      commit(gitDir, workDir, 'Second');
      
      writeFileSync(join(workDir, 'file.txt'), 'v3');
      addToIndex(gitDir, workDir, 'file.txt');
      commit(gitDir, workDir, 'Third');
      
      const entries = log(gitDir);
      assert.strictEqual(entries.length, 3);
      assert.strictEqual(entries[0].message, 'Third');
      assert.strictEqual(entries[1].message, 'Second');
      assert.strictEqual(entries[2].message, 'First');
    });

    it('respects maxCount', () => {
      for (let i = 0; i < 5; i++) {
        writeFileSync(join(workDir, 'file.txt'), `v${i}`);
        addToIndex(gitDir, workDir, 'file.txt');
        commit(gitDir, workDir, `Commit ${i}`);
      }
      
      const entries = log(gitDir, 3);
      assert.strictEqual(entries.length, 3);
    });
  });

  describe('formatLog', () => {
    it('formats log entries', () => {
      gitDir = init(workDir);
      writeFileSync(join(workDir, 'file.txt'), 'test');
      addToIndex(gitDir, workDir, 'file.txt');
      commit(gitDir, workDir, 'Test commit');
      
      const entries = log(gitDir);
      const output = formatLog(entries);
      assert.ok(output.includes('commit '));
      assert.ok(output.includes('Author: '));
      assert.ok(output.includes('    Test commit'));
    });
  });

  describe('End-to-end: init → add → commit → log', () => {
    it('full workflow', () => {
      // Init
      gitDir = init(workDir);
      
      // Create files
      writeFileSync(join(workDir, 'README.md'), '# My Project');
      mkdirSync(join(workDir, 'src'));
      writeFileSync(join(workDir, 'src', 'index.js'), 'console.log("hello")');
      
      // Add all
      addToIndex(gitDir, workDir, 'README.md');
      addToIndex(gitDir, workDir, 'src');
      
      // Status before commit
      const status = getStatus(gitDir, workDir, null);
      assert.ok(status.staged.includes('README.md'));
      assert.ok(status.staged.includes('src/index.js'));
      
      // First commit
      const c1 = commit(gitDir, workDir, 'Initial commit');
      
      // Modify a file
      writeFileSync(join(workDir, 'src', 'index.js'), 'console.log("updated")');
      addToIndex(gitDir, workDir, 'src/index.js');
      
      // Second commit
      const c2 = commit(gitDir, workDir, 'Update index.js');
      
      // Verify log
      const entries = log(gitDir);
      assert.strictEqual(entries.length, 2);
      assert.strictEqual(entries[0].message, 'Update index.js');
      assert.strictEqual(entries[0].parents[0], c1);
      assert.strictEqual(entries[1].message, 'Initial commit');
      assert.strictEqual(entries[1].parents.length, 0);
      
      // Verify branch
      assert.strictEqual(getCurrentBranch(gitDir), 'main');
      assert.strictEqual(resolveHead(gitDir), c2);
    });
  });
});
