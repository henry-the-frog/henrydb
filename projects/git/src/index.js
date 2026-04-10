// index.js — Git index (staging area)
// Binary format: DIRC header + sorted entries + SHA-1 checksum
// Simplified version: we use JSON for now, but maintain the same semantics

import { readFileSync, writeFileSync, existsSync, statSync, readdirSync } from 'fs';
import { join, relative, sep } from 'path';
import { createHash } from 'crypto';
import { hashObject, createBlob, createTree } from './objects.js';

/**
 * An index entry:
 * { path, mode, hash, size, mtime }
 */

/**
 * Read the index from disk.
 * Returns sorted array of entries.
 */
export function readIndex(gitDir) {
  const indexPath = join(gitDir, 'index');
  if (!existsSync(indexPath)) return [];
  
  try {
    const data = readFileSync(indexPath, 'utf8');
    return JSON.parse(data);
  } catch {
    return [];
  }
}

/**
 * Write the index to disk.
 */
export function writeIndex(gitDir, entries) {
  const indexPath = join(gitDir, 'index');
  // Sort by path (git index is always sorted)
  const sorted = [...entries].sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0);
  writeFileSync(indexPath, JSON.stringify(sorted, null, 2));
}

/**
 * Add a file to the index.
 * Reads the file, creates a blob, and adds/updates the index entry.
 */
export function addToIndex(gitDir, workDir, filePath) {
  const fullPath = join(workDir, filePath);
  if (!existsSync(fullPath)) {
    throw new Error(`File not found: ${filePath}`);
  }
  
  const stat = statSync(fullPath);
  if (stat.isDirectory()) {
    // Recursively add directory contents
    const files = listFiles(workDir, filePath);
    for (const f of files) {
      addToIndex(gitDir, workDir, f);
    }
    return;
  }
  
  const content = readFileSync(fullPath);
  const hash = createBlob(gitDir, content);
  const mode = stat.mode & 0o111 ? '100755' : '100644';
  
  const entries = readIndex(gitDir);
  
  // Update or add entry
  const normalized = filePath.split(sep).join('/');
  const idx = entries.findIndex(e => e.path === normalized);
  const entry = { path: normalized, mode, hash, size: stat.size, mtime: stat.mtimeMs };
  
  if (idx >= 0) {
    entries[idx] = entry;
  } else {
    entries.push(entry);
  }
  
  writeIndex(gitDir, entries);
  return hash;
}

/**
 * Remove a file from the index.
 */
export function removeFromIndex(gitDir, filePath) {
  const entries = readIndex(gitDir);
  const normalized = filePath.split(sep).join('/');
  const filtered = entries.filter(e => e.path !== normalized);
  writeIndex(gitDir, filtered);
}

/**
 * List all regular files under a directory (relative to workDir).
 */
export function listFiles(workDir, subPath = '') {
  const result = [];
  const dir = subPath ? join(workDir, subPath) : workDir;
  
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === '.git') continue;
    if (entry.name === 'node_modules') continue;
    
    const relPath = subPath ? `${subPath}/${entry.name}` : entry.name;
    
    if (entry.isDirectory()) {
      result.push(...listFiles(workDir, relPath));
    } else if (entry.isFile()) {
      result.push(relPath);
    }
  }
  
  return result;
}

/**
 * Get the status of the working tree.
 * Returns { staged, modified, untracked }
 */
export function getStatus(gitDir, workDir, headTree) {
  const index = readIndex(gitDir);
  const workFiles = listFiles(workDir);
  
  const staged = [];    // In index but different from HEAD
  const modified = [];  // In index but different from working tree
  const untracked = []; // In working tree but not in index
  const deleted = [];   // In index but not in working tree
  
  const indexMap = new Map(index.map(e => [e.path, e]));
  const headMap = headTree ? new Map(headTree.map(e => [e.path, e])) : new Map();
  
  // Check working tree files
  for (const file of workFiles) {
    const entry = indexMap.get(file);
    if (!entry) {
      untracked.push(file);
    } else {
      // Check if modified since staging
      const fullPath = join(workDir, file);
      const content = readFileSync(fullPath);
      const { hash } = hashObject('blob', content);
      if (hash !== entry.hash) {
        modified.push(file);
      }
    }
  }
  
  // Check for staged changes (index vs HEAD)
  for (const entry of index) {
    const headEntry = headMap.get(entry.path);
    if (!headEntry || headEntry.hash !== entry.hash) {
      staged.push(entry.path);
    }
    
    // Check for deleted files
    const fullPath = join(workDir, entry.path);
    if (!existsSync(fullPath)) {
      deleted.push(entry.path);
    }
  }
  
  // Check for files deleted from index but present in HEAD
  for (const [path] of headMap) {
    if (!indexMap.has(path)) {
      staged.push(path); // Deleted from index = staged deletion
    }
  }
  
  return { staged, modified, untracked, deleted };
}

/**
 * Build a tree object from the current index.
 * Returns the root tree hash.
 */
export function buildTree(gitDir) {
  const index = readIndex(gitDir);
  return buildTreeFromEntries(gitDir, index);
}

/**
 * Build a tree from a flat list of index entries.
 * Handles nested directories by creating subtrees.
 */
export function buildTreeFromEntries(gitDir, entries) {
  // Group by top-level directory
  const groups = new Map(); // dirname → entries with that prefix
  const files = [];
  
  for (const entry of entries) {
    const slashIdx = entry.path.indexOf('/');
    if (slashIdx === -1) {
      files.push(entry);
    } else {
      const dir = entry.path.slice(0, slashIdx);
      const rest = entry.path.slice(slashIdx + 1);
      if (!groups.has(dir)) groups.set(dir, []);
      groups.get(dir).push({ ...entry, path: rest });
    }
  }
  
  // Build tree entries
  const treeEntries = [];
  
  // Add file entries
  for (const entry of files) {
    treeEntries.push({ mode: entry.mode, name: entry.path, hash: entry.hash });
  }
  
  // Recursively build subtrees
  for (const [dir, subEntries] of groups) {
    const subHash = buildTreeFromEntries(gitDir, subEntries);
    treeEntries.push({ mode: '040000', name: dir, hash: subHash });
  }
  
  // Import createTree
  return createTree(gitDir, treeEntries);
}
