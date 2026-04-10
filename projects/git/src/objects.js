// objects.js — Git object store (content-addressable storage)
// Objects are stored as: {type} {size}\0{content}, then SHA-1 hashed and zlib compressed

import { createHash } from 'crypto';
import { deflateSync, inflateSync } from 'zlib';
import { mkdirSync, writeFileSync, readFileSync, existsSync } from 'fs';
import { join } from 'path';

/**
 * Hash a git object and return its SHA-1 hex digest.
 * Format: "{type} {size}\0{content}"
 */
export function hashObject(type, content) {
  const buf = typeof content === 'string' ? Buffer.from(content, 'utf8') : content;
  const header = `${type} ${buf.length}\0`;
  const store = Buffer.concat([Buffer.from(header), buf]);
  const hash = createHash('sha1').update(store).digest('hex');
  return { hash, store };
}

/**
 * Write a git object to the object store.
 * Returns the SHA-1 hash.
 */
export function writeObject(gitDir, type, content) {
  const { hash, store } = hashObject(type, content);
  const dir = join(gitDir, 'objects', hash.slice(0, 2));
  const file = join(dir, hash.slice(2));
  
  if (!existsSync(file)) {
    mkdirSync(dir, { recursive: true });
    writeFileSync(file, deflateSync(store));
  }
  
  return hash;
}

/**
 * Read a git object from the object store.
 * Returns { type, size, content } where content is a Buffer.
 */
export function readObject(gitDir, hash) {
  const dir = join(gitDir, 'objects', hash.slice(0, 2));
  const file = join(dir, hash.slice(2));
  
  if (!existsSync(file)) {
    throw new Error(`Object not found: ${hash}`);
  }
  
  const compressed = readFileSync(file);
  const store = inflateSync(compressed);
  
  // Parse header: "{type} {size}\0{content}"
  const nullIdx = store.indexOf(0);
  const header = store.subarray(0, nullIdx).toString('utf8');
  const [type, sizeStr] = header.split(' ');
  const size = parseInt(sizeStr, 10);
  const content = store.subarray(nullIdx + 1);
  
  if (content.length !== size) {
    throw new Error(`Object size mismatch: expected ${size}, got ${content.length}`);
  }
  
  return { type, size, content };
}

/**
 * Check if an object exists in the store.
 */
export function objectExists(gitDir, hash) {
  return existsSync(join(gitDir, 'objects', hash.slice(0, 2), hash.slice(2)));
}

// ===== Blob =====

/**
 * Create a blob object from file content.
 */
export function createBlob(gitDir, content) {
  return writeObject(gitDir, 'blob', content);
}

// ===== Tree =====

/**
 * A tree entry: { mode, name, hash }
 * mode: '100644' (regular file), '100755' (executable), '040000' (directory/tree)
 */
export function createTree(gitDir, entries) {
  // Sort entries by name (git sorts trees lexicographically)
  const sorted = [...entries].sort((a, b) => {
    // Git sorts directories with trailing '/' for comparison
    const aName = a.mode === '040000' ? a.name + '/' : a.name;
    const bName = b.mode === '040000' ? b.name + '/' : b.name;
    return aName < bName ? -1 : aName > bName ? 1 : 0;
  });
  
  // Tree format: for each entry: "{mode} {name}\0{20-byte-hash}"
  const buffers = sorted.map(entry => {
    const modeName = Buffer.from(`${entry.mode} ${entry.name}\0`);
    const hashBytes = Buffer.from(entry.hash, 'hex');
    return Buffer.concat([modeName, hashBytes]);
  });
  
  const content = Buffer.concat(buffers);
  return writeObject(gitDir, 'tree', content);
}

/**
 * Parse a tree object into entries.
 */
export function parseTree(content) {
  const entries = [];
  let offset = 0;
  
  while (offset < content.length) {
    // Find the null byte
    const nullIdx = content.indexOf(0, offset);
    if (nullIdx === -1) break;
    
    const modeAndName = content.subarray(offset, nullIdx).toString('utf8');
    const spaceIdx = modeAndName.indexOf(' ');
    const mode = modeAndName.slice(0, spaceIdx);
    const name = modeAndName.slice(spaceIdx + 1);
    
    // Next 20 bytes are the SHA-1 hash
    const hashBytes = content.subarray(nullIdx + 1, nullIdx + 21);
    const hash = hashBytes.toString('hex');
    
    entries.push({ mode, name, hash });
    offset = nullIdx + 21;
  }
  
  return entries;
}

// ===== Commit =====

/**
 * Create a commit object.
 * @param {string} gitDir
 * @param {string} treeHash - SHA-1 of the root tree
 * @param {string[]} parents - Array of parent commit hashes
 * @param {string} author - "Name <email> timestamp timezone"
 * @param {string} committer - same format as author
 * @param {string} message - commit message
 */
export function createCommit(gitDir, treeHash, parents, author, committer, message) {
  const lines = [`tree ${treeHash}`];
  for (const parent of parents) {
    lines.push(`parent ${parent}`);
  }
  lines.push(`author ${author}`);
  lines.push(`committer ${committer}`);
  lines.push('');
  lines.push(message);
  
  const content = lines.join('\n');
  return writeObject(gitDir, 'commit', content);
}

/**
 * Parse a commit object.
 */
export function parseCommit(content) {
  const text = typeof content === 'string' ? content : content.toString('utf8');
  const lines = text.split('\n');
  
  const commit = { tree: null, parents: [], author: null, committer: null, message: '' };
  let i = 0;
  
  // Parse headers
  while (i < lines.length && lines[i] !== '') {
    const line = lines[i];
    if (line.startsWith('tree ')) {
      commit.tree = line.slice(5);
    } else if (line.startsWith('parent ')) {
      commit.parents.push(line.slice(7));
    } else if (line.startsWith('author ')) {
      commit.author = line.slice(7);
    } else if (line.startsWith('committer ')) {
      commit.committer = line.slice(10);
    }
    i++;
  }
  
  // Skip blank line
  if (i < lines.length && lines[i] === '') i++;
  
  // Rest is message (trim trailing newline)
  commit.message = lines.slice(i).join('\n').replace(/\n$/, '');
  
  return commit;
}

// ===== Tag =====

/**
 * Create an annotated tag object.
 */
export function createTag(gitDir, objectHash, objectType, tagName, tagger, message) {
  const lines = [
    `object ${objectHash}`,
    `type ${objectType}`,
    `tag ${tagName}`,
    `tagger ${tagger}`,
    '',
    message
  ];
  return writeObject(gitDir, 'tag', lines.join('\n'));
}
