// pack.test.js — Tests for git pack format
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { createPack, unpack, enumerateObjects } from './pack.js';
import { init, commit } from './commands.js';
import { addToIndex } from './index.js';
import { createBlob, createTree, readObject, objectExists } from './objects.js';
import { resolveHead } from './refs.js';

describe('Git Pack Format', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-pack-'));
    workDir = tmp;
    gitDir = init(workDir);
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  describe('createPack / unpack', () => {
    it('round-trips a single blob', () => {
      const hash = createBlob(gitDir, 'test content');
      
      const pack = createPack(gitDir, [hash]);
      assert.ok(pack.length > 0);
      assert.ok(pack.toString('ascii', 0, 4) === 'PACK');
      
      // Create a second git dir and unpack into it
      const tmp2 = mkdtempSync(join(tmpdir(), 'tiny-git-pack2-'));
      const gitDir2 = init(tmp2);
      
      const results = unpack(join(tmp2, '.git'), pack);
      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0].hash, hash);
      assert.strictEqual(results[0].type, 'blob');
      
      // Verify the object was written correctly
      const obj = readObject(join(tmp2, '.git'), hash);
      assert.strictEqual(obj.content.toString(), 'test content');
      
      rmSync(tmp2, { recursive: true, force: true });
    });

    it('round-trips multiple objects', () => {
      const h1 = createBlob(gitDir, 'file one');
      const h2 = createBlob(gitDir, 'file two');
      const treeHash = createTree(gitDir, [
        { mode: '100644', name: 'one.txt', hash: h1 },
        { mode: '100644', name: 'two.txt', hash: h2 }
      ]);
      
      const pack = createPack(gitDir, [h1, h2, treeHash]);
      
      const tmp2 = mkdtempSync(join(tmpdir(), 'tiny-git-pack3-'));
      const gitDir2 = init(tmp2);
      
      const results = unpack(join(tmp2, '.git'), pack);
      assert.strictEqual(results.length, 3);
      
      // All objects should exist in the new store
      assert.ok(objectExists(join(tmp2, '.git'), h1));
      assert.ok(objectExists(join(tmp2, '.git'), h2));
      assert.ok(objectExists(join(tmp2, '.git'), treeHash));
      
      rmSync(tmp2, { recursive: true, force: true });
    });

    it('handles commit objects', () => {
      writeFileSync(join(workDir, 'file.txt'), 'content');
      addToIndex(gitDir, workDir, 'file.txt');
      const commitHash = commit(gitDir, workDir, 'Test commit');
      
      const pack = createPack(gitDir, [commitHash]);
      
      const tmp2 = mkdtempSync(join(tmpdir(), 'tiny-git-pack4-'));
      const gitDir2 = init(tmp2);
      
      const results = unpack(join(tmp2, '.git'), pack);
      assert.strictEqual(results[0].type, 'commit');
      
      rmSync(tmp2, { recursive: true, force: true });
    });
  });

  describe('enumerateObjects', () => {
    it('finds all objects reachable from a commit', () => {
      writeFileSync(join(workDir, 'a.txt'), 'aaa');
      writeFileSync(join(workDir, 'b.txt'), 'bbb');
      addToIndex(gitDir, workDir, 'a.txt');
      addToIndex(gitDir, workDir, 'b.txt');
      const hash = commit(gitDir, workDir, 'Initial');
      
      const objects = enumerateObjects(gitDir, [hash]);
      
      // Should have: 1 commit + 1 tree + 2 blobs = 4 objects
      assert.strictEqual(objects.length, 4);
      assert.ok(objects.includes(hash)); // commit
    });

    it('handles nested trees', () => {
      mkdirSync(join(workDir, 'src'));
      writeFileSync(join(workDir, 'README.md'), 'readme');
      writeFileSync(join(workDir, 'src', 'main.js'), 'main');
      addToIndex(gitDir, workDir, 'README.md');
      addToIndex(gitDir, workDir, 'src');
      commit(gitDir, workDir, 'Nested');
      
      const objects = enumerateObjects(gitDir, [resolveHead(gitDir)]);
      // commit + root tree + src tree + 2 blobs = 5
      assert.strictEqual(objects.length, 5);
    });

    it('deduplicates across commits', () => {
      writeFileSync(join(workDir, 'file.txt'), 'v1');
      addToIndex(gitDir, workDir, 'file.txt');
      const c1 = commit(gitDir, workDir, 'V1');
      
      writeFileSync(join(workDir, 'file.txt'), 'v2');
      addToIndex(gitDir, workDir, 'file.txt');
      const c2 = commit(gitDir, workDir, 'V2');
      
      const objects = enumerateObjects(gitDir, [c2]);
      // Each object counted once even if reachable from multiple paths
      const unique = new Set(objects);
      assert.strictEqual(objects.length, unique.size);
    });
  });

  describe('Pack header', () => {
    it('has correct magic number', () => {
      const hash = createBlob(gitDir, 'x');
      const pack = createPack(gitDir, [hash]);
      assert.strictEqual(pack.toString('ascii', 0, 4), 'PACK');
    });

    it('has correct version', () => {
      const hash = createBlob(gitDir, 'x');
      const pack = createPack(gitDir, [hash]);
      assert.strictEqual(pack.readUInt32BE(4), 2);
    });

    it('has correct object count', () => {
      const h1 = createBlob(gitDir, 'a');
      const h2 = createBlob(gitDir, 'b');
      const h3 = createBlob(gitDir, 'c');
      const pack = createPack(gitDir, [h1, h2, h3]);
      assert.strictEqual(pack.readUInt32BE(8), 3);
    });
  });
});
