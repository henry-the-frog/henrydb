// checkout.js — Branch and checkout operations

import { readFileSync, writeFileSync, existsSync, mkdirSync, unlinkSync, rmSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { readObject, parseTree, parseCommit } from './objects.js';
import { readIndex, writeIndex } from './index.js';
import { readHead, writeHead, resolveHead, resolveRef, writeRef, createBranch, getCurrentBranch, listBranches } from './refs.js';

/**
 * Checkout a branch or commit.
 * Updates HEAD, index, and working tree.
 */
export function checkout(gitDir, workDir, target) {
  // Check if target is a branch name
  const branches = listBranches(gitDir);
  let targetHash;
  let isBranch = false;
  
  if (branches.includes(target)) {
    targetHash = resolveRef(gitDir, `refs/heads/${target}`);
    isBranch = true;
  } else if (target.length === 40 && /^[0-9a-f]+$/.test(target)) {
    targetHash = target;
  } else {
    throw new Error(`Unknown branch or commit: ${target}`);
  }
  
  if (!targetHash) {
    throw new Error(`Branch '${target}' has no commits`);
  }
  
  // Read the target commit's tree
  const commitObj = readObject(gitDir, targetHash);
  const commitData = parseCommit(commitObj.content);
  
  // Flatten the tree into file entries
  const treeEntries = flattenTree(gitDir, commitData.tree);
  
  // Update working tree
  updateWorkingTree(gitDir, workDir, treeEntries);
  
  // Update index
  writeIndex(gitDir, treeEntries.map(e => ({
    path: e.path,
    mode: e.mode,
    hash: e.hash,
    size: e.size || 0,
    mtime: Date.now()
  })));
  
  // Update HEAD
  if (isBranch) {
    writeHead(gitDir, { type: 'ref', ref: `refs/heads/${target}` });
  } else {
    writeHead(gitDir, { type: 'hash', hash: targetHash });
  }
  
  return { hash: targetHash, branch: isBranch ? target : null };
}

/**
 * Create a new branch and optionally check it out.
 */
export function checkoutNewBranch(gitDir, workDir, branchName) {
  const currentHash = resolveHead(gitDir);
  if (!currentHash) {
    throw new Error('Cannot create branch: no commits yet');
  }
  
  // Check if branch already exists
  if (listBranches(gitDir).includes(branchName)) {
    throw new Error(`Branch '${branchName}' already exists`);
  }
  
  // Create the branch
  createBranch(gitDir, branchName, currentHash);
  
  // Point HEAD to new branch
  writeHead(gitDir, { type: 'ref', ref: `refs/heads/${branchName}` });
  
  return currentHash;
}

/**
 * Flatten a tree object into a list of { path, mode, hash } entries.
 */
export function flattenTree(gitDir, treeHash, prefix = '') {
  const treeObj = readObject(gitDir, treeHash);
  const entries = parseTree(treeObj.content);
  const result = [];
  
  for (const entry of entries) {
    const fullPath = prefix ? `${prefix}/${entry.name}` : entry.name;
    
    if (entry.mode === '040000' || entry.mode === '40000') {
      // Recurse into subtree
      result.push(...flattenTree(gitDir, entry.hash, fullPath));
    } else {
      result.push({
        path: fullPath,
        mode: entry.mode,
        hash: entry.hash
      });
    }
  }
  
  return result;
}

/**
 * Update the working tree to match tree entries.
 */
function updateWorkingTree(gitDir, workDir, treeEntries) {
  // Get current index to know what to remove
  const currentIndex = readIndex(gitDir);
  const currentPaths = new Set(currentIndex.map(e => e.path));
  const newPaths = new Set(treeEntries.map(e => e.path));
  
  // Remove files that are in current but not in new
  for (const path of currentPaths) {
    if (!newPaths.has(path)) {
      const fullPath = join(workDir, path);
      if (existsSync(fullPath)) {
        unlinkSync(fullPath);
        // Try to remove empty parent directories
        cleanEmptyDirs(workDir, dirname(path));
      }
    }
  }
  
  // Write/update files
  for (const entry of treeEntries) {
    const fullPath = join(workDir, entry.path);
    mkdirSync(dirname(fullPath), { recursive: true });
    
    const obj = readObject(gitDir, entry.hash);
    writeFileSync(fullPath, obj.content);
  }
}

function cleanEmptyDirs(workDir, relDir) {
  if (!relDir || relDir === '.') return;
  const fullDir = join(workDir, relDir);
  try {
    if (readdirSync(fullDir).length === 0) {
      rmSync(fullDir);
      cleanEmptyDirs(workDir, dirname(relDir));
    }
  } catch {}
}
