// index.test.js — Tests for git index (staging area)
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, mkdirSync, writeFileSync, readFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import {
  readIndex, writeIndex, addToIndex, removeFromIndex,
  listFiles, getStatus, buildTreeFromEntries
} from './index.js';
import { readObject, parseTree } from './objects.js';

describe('Git Index', () => {
  let tmp, gitDir, workDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-'));
    workDir = tmp;
    gitDir = join(tmp, '.git');
    mkdirSync(join(gitDir, 'objects'), { recursive: true });
    mkdirSync(join(gitDir, 'refs'), { recursive: true });
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  describe('readIndex / writeIndex', () => {
    it('returns empty array when no index exists', () => {
      assert.deepStrictEqual(readIndex(gitDir), []);
    });

    it('roundtrips entries', () => {
      const entries = [
        { path: 'a.txt', mode: '100644', hash: 'abc123', size: 5, mtime: 1000 },
        { path: 'b.txt', mode: '100644', hash: 'def456', size: 10, mtime: 2000 }
      ];
      writeIndex(gitDir, entries);
      const read = readIndex(gitDir);
      assert.deepStrictEqual(read, entries);
    });

    it('sorts entries by path', () => {
      const entries = [
        { path: 'z.txt', mode: '100644', hash: 'z', size: 1, mtime: 1 },
        { path: 'a.txt', mode: '100644', hash: 'a', size: 1, mtime: 1 },
      ];
      writeIndex(gitDir, entries);
      const read = readIndex(gitDir);
      assert.strictEqual(read[0].path, 'a.txt');
      assert.strictEqual(read[1].path, 'z.txt');
    });
  });

  describe('addToIndex', () => {
    it('adds a file to the index', () => {
      writeFileSync(join(workDir, 'hello.txt'), 'hello world');
      addToIndex(gitDir, workDir, 'hello.txt');
      
      const entries = readIndex(gitDir);
      assert.strictEqual(entries.length, 1);
      assert.strictEqual(entries[0].path, 'hello.txt');
      assert.strictEqual(entries[0].mode, '100644');
      assert.strictEqual(entries[0].size, 11);
      assert.ok(entries[0].hash);
    });

    it('updates existing entry on re-add', () => {
      writeFileSync(join(workDir, 'file.txt'), 'version 1');
      addToIndex(gitDir, workDir, 'file.txt');
      
      const h1 = readIndex(gitDir)[0].hash;
      
      writeFileSync(join(workDir, 'file.txt'), 'version 2');
      addToIndex(gitDir, workDir, 'file.txt');
      
      const entries = readIndex(gitDir);
      assert.strictEqual(entries.length, 1);
      assert.notStrictEqual(entries[0].hash, h1);
    });

    it('adds multiple files', () => {
      writeFileSync(join(workDir, 'a.txt'), 'a');
      writeFileSync(join(workDir, 'b.txt'), 'b');
      addToIndex(gitDir, workDir, 'a.txt');
      addToIndex(gitDir, workDir, 'b.txt');
      
      const entries = readIndex(gitDir);
      assert.strictEqual(entries.length, 2);
    });

    it('adds a directory recursively', () => {
      mkdirSync(join(workDir, 'src'));
      writeFileSync(join(workDir, 'src', 'main.js'), 'console.log("hi")');
      writeFileSync(join(workDir, 'src', 'util.js'), 'export {}');
      addToIndex(gitDir, workDir, 'src');
      
      const entries = readIndex(gitDir);
      assert.strictEqual(entries.length, 2);
      assert.ok(entries.some(e => e.path === 'src/main.js'));
      assert.ok(entries.some(e => e.path === 'src/util.js'));
    });

    it('throws for nonexistent file', () => {
      assert.throws(() => addToIndex(gitDir, workDir, 'nope.txt'), /not found/i);
    });
  });

  describe('removeFromIndex', () => {
    it('removes a file from the index', () => {
      writeFileSync(join(workDir, 'a.txt'), 'a');
      writeFileSync(join(workDir, 'b.txt'), 'b');
      addToIndex(gitDir, workDir, 'a.txt');
      addToIndex(gitDir, workDir, 'b.txt');
      
      removeFromIndex(gitDir, 'a.txt');
      
      const entries = readIndex(gitDir);
      assert.strictEqual(entries.length, 1);
      assert.strictEqual(entries[0].path, 'b.txt');
    });
  });

  describe('listFiles', () => {
    it('lists files in working directory', () => {
      writeFileSync(join(workDir, 'a.txt'), 'a');
      writeFileSync(join(workDir, 'b.txt'), 'b');
      
      const files = listFiles(workDir);
      assert.ok(files.includes('a.txt'));
      assert.ok(files.includes('b.txt'));
    });

    it('skips .git directory', () => {
      writeFileSync(join(workDir, 'a.txt'), 'a');
      const files = listFiles(workDir);
      assert.ok(!files.some(f => f.includes('.git')));
    });

    it('lists nested files', () => {
      mkdirSync(join(workDir, 'src'));
      writeFileSync(join(workDir, 'src', 'main.js'), 'x');
      
      const files = listFiles(workDir);
      assert.ok(files.includes('src/main.js'));
    });
  });

  describe('getStatus', () => {
    it('detects untracked files', () => {
      writeFileSync(join(workDir, 'new.txt'), 'new');
      
      const status = getStatus(gitDir, workDir, null);
      assert.ok(status.untracked.includes('new.txt'));
    });

    it('detects staged files', () => {
      writeFileSync(join(workDir, 'staged.txt'), 'staged');
      addToIndex(gitDir, workDir, 'staged.txt');
      
      const status = getStatus(gitDir, workDir, null);
      assert.ok(status.staged.includes('staged.txt'));
      assert.strictEqual(status.untracked.length, 0);
    });

    it('detects modified files', () => {
      writeFileSync(join(workDir, 'mod.txt'), 'original');
      addToIndex(gitDir, workDir, 'mod.txt');
      
      // Modify after staging
      writeFileSync(join(workDir, 'mod.txt'), 'modified');
      
      const status = getStatus(gitDir, workDir, null);
      assert.ok(status.modified.includes('mod.txt'));
    });

    it('detects deleted files', () => {
      writeFileSync(join(workDir, 'del.txt'), 'to delete');
      addToIndex(gitDir, workDir, 'del.txt');
      rmSync(join(workDir, 'del.txt'));
      
      const status = getStatus(gitDir, workDir, null);
      assert.ok(status.deleted.includes('del.txt'));
    });
  });

  describe('buildTreeFromEntries', () => {
    it('builds a flat tree', () => {
      writeFileSync(join(workDir, 'a.txt'), 'hello');
      writeFileSync(join(workDir, 'b.txt'), 'world');
      addToIndex(gitDir, workDir, 'a.txt');
      addToIndex(gitDir, workDir, 'b.txt');
      
      const entries = readIndex(gitDir);
      const treeHash = buildTreeFromEntries(gitDir, entries);
      
      const tree = readObject(gitDir, treeHash);
      assert.strictEqual(tree.type, 'tree');
      const parsed = parseTree(tree.content);
      assert.strictEqual(parsed.length, 2);
    });

    it('builds nested tree from flat entries', () => {
      mkdirSync(join(workDir, 'src'));
      writeFileSync(join(workDir, 'README.md'), '# Hello');
      writeFileSync(join(workDir, 'src', 'main.js'), 'code');
      addToIndex(gitDir, workDir, 'README.md');
      addToIndex(gitDir, workDir, 'src/main.js');
      
      const entries = readIndex(gitDir);
      const treeHash = buildTreeFromEntries(gitDir, entries);
      
      const tree = parseTree(readObject(gitDir, treeHash).content);
      assert.strictEqual(tree.length, 2);
      
      const srcEntry = tree.find(e => e.name === 'src');
      assert.ok(srcEntry);
      assert.strictEqual(srcEntry.mode, '040000');
      
      // Verify subtree
      const subTree = parseTree(readObject(gitDir, srcEntry.hash).content);
      assert.strictEqual(subTree.length, 1);
      assert.strictEqual(subTree[0].name, 'main.js');
    });
  });
});
