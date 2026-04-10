// objects.test.js — Tests for git object store
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync, mkdirSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import {
  hashObject, writeObject, readObject, objectExists,
  createBlob, createTree, parseTree,
  createCommit, parseCommit, createTag
} from './objects.js';

describe('Git Object Store', () => {
  let gitDir;

  beforeEach(() => {
    const tmp = mkdtempSync(join(tmpdir(), 'tiny-git-'));
    gitDir = join(tmp, '.git');
    mkdirSync(join(gitDir, 'objects'), { recursive: true });
  });

  afterEach(() => {
    rmSync(gitDir.replace('/.git', ''), { recursive: true, force: true });
  });

  describe('hashObject', () => {
    it('produces correct SHA-1 for "hello world"', () => {
      // git hash-object -t blob --stdin <<< "hello world" (without newline)
      // echo -n "hello world" | git hash-object --stdin
      const { hash } = hashObject('blob', 'hello world');
      assert.strictEqual(hash.length, 40);
      // Verify against known git hash: "blob 11\0hello world"
      assert.strictEqual(hash, '95d09f2b10159347eece71399a7e2e907ea3df4f');
    });

    it('produces different hashes for different content', () => {
      const { hash: h1 } = hashObject('blob', 'hello');
      const { hash: h2 } = hashObject('blob', 'world');
      assert.notStrictEqual(h1, h2);
    });

    it('produces same hash for same content', () => {
      const { hash: h1 } = hashObject('blob', 'test');
      const { hash: h2 } = hashObject('blob', 'test');
      assert.strictEqual(h1, h2);
    });

    it('handles empty content', () => {
      const { hash } = hashObject('blob', '');
      // "blob 0\0" → known hash
      assert.strictEqual(hash, 'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391');
    });

    it('handles binary content', () => {
      const { hash } = hashObject('blob', Buffer.from([0, 1, 2, 3, 255]));
      assert.strictEqual(hash.length, 40);
    });
  });

  describe('writeObject / readObject', () => {
    it('roundtrips a blob', () => {
      const content = 'Hello, Git!';
      const hash = writeObject(gitDir, 'blob', content);
      const obj = readObject(gitDir, hash);
      assert.strictEqual(obj.type, 'blob');
      assert.strictEqual(obj.content.toString('utf8'), content);
      assert.strictEqual(obj.size, Buffer.byteLength(content));
    });

    it('writes to correct directory structure', () => {
      const hash = writeObject(gitDir, 'blob', 'test');
      const dir = join(gitDir, 'objects', hash.slice(0, 2));
      const file = join(dir, hash.slice(2));
      assert.ok(readFileSync(file));
    });

    it('is idempotent (writing same content twice)', () => {
      const h1 = writeObject(gitDir, 'blob', 'same');
      const h2 = writeObject(gitDir, 'blob', 'same');
      assert.strictEqual(h1, h2);
    });

    it('handles large content', () => {
      const content = 'x'.repeat(100000);
      const hash = writeObject(gitDir, 'blob', content);
      const obj = readObject(gitDir, hash);
      assert.strictEqual(obj.content.length, 100000);
    });

    it('compresses content (file smaller than original)', () => {
      const content = 'a'.repeat(10000);
      const hash = writeObject(gitDir, 'blob', content);
      const file = join(gitDir, 'objects', hash.slice(0, 2), hash.slice(2));
      const compressed = readFileSync(file);
      assert.ok(compressed.length < content.length, 'Compressed should be smaller');
    });

    it('throws for nonexistent object', () => {
      assert.throws(() => readObject(gitDir, 'deadbeef' + '0'.repeat(32)), /not found/i);
    });
  });

  describe('objectExists', () => {
    it('returns false for missing object', () => {
      assert.strictEqual(objectExists(gitDir, '0'.repeat(40)), false);
    });

    it('returns true after writing', () => {
      const hash = writeObject(gitDir, 'blob', 'exists');
      assert.strictEqual(objectExists(gitDir, hash), true);
    });
  });

  describe('Blob', () => {
    it('creates and reads a blob', () => {
      const hash = createBlob(gitDir, 'file content');
      const obj = readObject(gitDir, hash);
      assert.strictEqual(obj.type, 'blob');
      assert.strictEqual(obj.content.toString(), 'file content');
    });
  });

  describe('Tree', () => {
    it('creates and parses a tree with files', () => {
      const blobHash = createBlob(gitDir, 'hello');
      const treeHash = createTree(gitDir, [
        { mode: '100644', name: 'hello.txt', hash: blobHash }
      ]);
      
      const obj = readObject(gitDir, treeHash);
      assert.strictEqual(obj.type, 'tree');
      
      const entries = parseTree(obj.content);
      assert.strictEqual(entries.length, 1);
      assert.strictEqual(entries[0].mode, '100644');
      assert.strictEqual(entries[0].name, 'hello.txt');
      assert.strictEqual(entries[0].hash, blobHash);
    });

    it('sorts entries alphabetically', () => {
      const h1 = createBlob(gitDir, 'a');
      const h2 = createBlob(gitDir, 'b');
      const h3 = createBlob(gitDir, 'c');
      
      // Add in wrong order
      const treeHash = createTree(gitDir, [
        { mode: '100644', name: 'c.txt', hash: h3 },
        { mode: '100644', name: 'a.txt', hash: h1 },
        { mode: '100644', name: 'b.txt', hash: h2 },
      ]);
      
      const entries = parseTree(readObject(gitDir, treeHash).content);
      assert.deepStrictEqual(entries.map(e => e.name), ['a.txt', 'b.txt', 'c.txt']);
    });

    it('handles nested trees', () => {
      const blobHash = createBlob(gitDir, 'nested file');
      const subTree = createTree(gitDir, [
        { mode: '100644', name: 'file.txt', hash: blobHash }
      ]);
      const rootTree = createTree(gitDir, [
        { mode: '040000', name: 'subdir', hash: subTree },
        { mode: '100644', name: 'root.txt', hash: blobHash }
      ]);
      
      const entries = parseTree(readObject(gitDir, rootTree).content);
      assert.strictEqual(entries.length, 2);
      const dirEntry = entries.find(e => e.name === 'subdir');
      assert.ok(dirEntry);
      assert.strictEqual(dirEntry.mode, '040000');
    });

    it('handles empty tree', () => {
      const treeHash = createTree(gitDir, []);
      const obj = readObject(gitDir, treeHash);
      assert.strictEqual(obj.type, 'tree');
      const entries = parseTree(obj.content);
      assert.strictEqual(entries.length, 0);
    });
  });

  describe('Commit', () => {
    it('creates and parses a commit', () => {
      const blobHash = createBlob(gitDir, 'initial');
      const treeHash = createTree(gitDir, [
        { mode: '100644', name: 'file.txt', hash: blobHash }
      ]);
      
      const now = Math.floor(Date.now() / 1000);
      const author = `Henry <henry@example.com> ${now} -0600`;
      const commitHash = createCommit(gitDir, treeHash, [], author, author, 'Initial commit');
      
      const obj = readObject(gitDir, commitHash);
      assert.strictEqual(obj.type, 'commit');
      
      const commit = parseCommit(obj.content);
      assert.strictEqual(commit.tree, treeHash);
      assert.strictEqual(commit.parents.length, 0);
      assert.strictEqual(commit.message, 'Initial commit');
      assert.ok(commit.author.includes('Henry'));
    });

    it('handles parent commits', () => {
      const treeHash = createTree(gitDir, []);
      const now = Math.floor(Date.now() / 1000);
      const auth = `Test <test@test.com> ${now} +0000`;
      
      const c1 = createCommit(gitDir, treeHash, [], auth, auth, 'First');
      const c2 = createCommit(gitDir, treeHash, [c1], auth, auth, 'Second');
      
      const commit = parseCommit(readObject(gitDir, c2).content);
      assert.strictEqual(commit.parents.length, 1);
      assert.strictEqual(commit.parents[0], c1);
    });

    it('handles merge commits (multiple parents)', () => {
      const treeHash = createTree(gitDir, []);
      const auth = `Merge <merge@test.com> 1000000 +0000`;
      
      const c1 = createCommit(gitDir, treeHash, [], auth, auth, 'Branch A');
      const c2 = createCommit(gitDir, treeHash, [], auth, auth, 'Branch B');
      const merge = createCommit(gitDir, treeHash, [c1, c2], auth, auth, 'Merge');
      
      const commit = parseCommit(readObject(gitDir, merge).content);
      assert.strictEqual(commit.parents.length, 2);
      assert.strictEqual(commit.parents[0], c1);
      assert.strictEqual(commit.parents[1], c2);
    });
  });

  describe('Tag', () => {
    it('creates an annotated tag', () => {
      const treeHash = createTree(gitDir, []);
      const auth = `Tagger <tag@test.com> 1000000 +0000`;
      const commitHash = createCommit(gitDir, treeHash, [], auth, auth, 'Tagged');
      
      const tagHash = createTag(gitDir, commitHash, 'commit', 'v1.0', auth, 'Release 1.0');
      const obj = readObject(gitDir, tagHash);
      assert.strictEqual(obj.type, 'tag');
      assert.ok(obj.content.toString().includes('v1.0'));
      assert.ok(obj.content.toString().includes('Release 1.0'));
    });
  });

  describe('Compatibility with real git', () => {
    it('produces same hash as git for empty blob', () => {
      // This is the well-known empty blob hash
      const { hash } = hashObject('blob', '');
      assert.strictEqual(hash, 'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391');
    });

    it('produces same hash as git for empty tree', () => {
      // Empty tree hash is well-known
      const { hash } = hashObject('tree', Buffer.alloc(0));
      assert.strictEqual(hash, '4b825dc642cb6eb9a060e54bf8d69288fbee4904');
    });
  });
});
