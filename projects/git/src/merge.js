// merge.js — Three-way merge

import { readObject, parseTree, parseCommit, createBlob, createTree, createCommit } from './objects.js';
import { readIndex, writeIndex, buildTreeFromEntries } from './index.js';
import { resolveHead, resolveRef, updateHead, readHead, getCurrentBranch } from './refs.js';
import { flattenTree } from './checkout.js';
import { myersDiff } from './diff.js';
import { writeFileSync, mkdirSync, existsSync } from 'fs';
import { join, dirname } from 'path';

/**
 * Merge a branch into the current branch.
 * Returns { type: 'fast-forward'|'merge'|'conflict', hash?, conflicts? }
 */
export function merge(gitDir, workDir, branchName, authorName = 'User', authorEmail = 'user@example.com') {
  const headHash = resolveHead(gitDir);
  const branchHash = resolveRef(gitDir, `refs/heads/${branchName}`);
  
  if (!branchHash) {
    throw new Error(`Branch '${branchName}' not found`);
  }
  
  if (headHash === branchHash) {
    return { type: 'already-up-to-date' };
  }
  
  // Find merge base (common ancestor)
  const base = findMergeBase(gitDir, headHash, branchHash);
  
  // Fast-forward: if base === HEAD, just move HEAD to branch
  if (base === headHash) {
    return fastForward(gitDir, workDir, branchHash);
  }
  
  // Fast-forward the other way: if base === branch, already up to date
  if (base === branchHash) {
    return { type: 'already-up-to-date' };
  }
  
  // Three-way merge
  return threeWayMerge(gitDir, workDir, headHash, branchHash, base, branchName, authorName, authorEmail);
}

/**
 * Fast-forward: move HEAD and update working tree.
 */
function fastForward(gitDir, workDir, targetHash) {
  const commitData = parseCommit(readObject(gitDir, targetHash).content);
  const files = flattenTree(gitDir, commitData.tree);
  
  // Update working tree
  for (const file of files) {
    const fullPath = join(workDir, file.path);
    mkdirSync(dirname(fullPath), { recursive: true });
    writeFileSync(fullPath, readObject(gitDir, file.hash).content);
  }
  
  // Update index
  writeIndex(gitDir, files.map(f => ({
    path: f.path, mode: f.mode, hash: f.hash, size: 0, mtime: Date.now()
  })));
  
  // Update HEAD
  updateHead(gitDir, targetHash);
  
  return { type: 'fast-forward', hash: targetHash };
}

/**
 * Three-way merge.
 */
function threeWayMerge(gitDir, workDir, oursHash, theirsHash, baseHash, branchName, authorName, authorEmail) {
  // Get file lists for base, ours, theirs
  const baseCommit = parseCommit(readObject(gitDir, baseHash).content);
  const oursCommit = parseCommit(readObject(gitDir, oursHash).content);
  const theirsCommit = parseCommit(readObject(gitDir, theirsHash).content);
  
  const baseFiles = new Map(flattenTree(gitDir, baseCommit.tree).map(f => [f.path, f]));
  const oursFiles = new Map(flattenTree(gitDir, oursCommit.tree).map(f => [f.path, f]));
  const theirsFiles = new Map(flattenTree(gitDir, theirsCommit.tree).map(f => [f.path, f]));
  
  // All paths
  const allPaths = new Set([...baseFiles.keys(), ...oursFiles.keys(), ...theirsFiles.keys()]);
  
  const mergedEntries = [];
  const conflicts = [];
  
  for (const path of allPaths) {
    const base = baseFiles.get(path);
    const ours = oursFiles.get(path);
    const theirs = theirsFiles.get(path);
    
    const baseHash = base?.hash;
    const oursHash2 = ours?.hash;
    const theirsHash2 = theirs?.hash;
    
    if (oursHash2 === theirsHash2) {
      // Both same (or both deleted) — no conflict
      if (oursHash2) {
        mergedEntries.push({ path, mode: ours.mode, hash: oursHash2 });
      }
      // else: both deleted
    } else if (baseHash === oursHash2) {
      // We didn't change, they did — take theirs
      if (theirsHash2) {
        mergedEntries.push({ path, mode: theirs.mode, hash: theirsHash2 });
      }
      // else: they deleted
    } else if (baseHash === theirsHash2) {
      // They didn't change, we did — take ours
      if (oursHash2) {
        mergedEntries.push({ path, mode: ours.mode, hash: oursHash2 });
      }
      // else: we deleted
    } else {
      // Both changed — need to attempt text merge
      const baseContent = baseHash ? readObject(gitDir, baseHash).content.toString() : '';
      const oursContent = oursHash2 ? readObject(gitDir, oursHash2).content.toString() : '';
      const theirsContent = theirsHash2 ? readObject(gitDir, theirsHash2).content.toString() : '';
      
      const result = mergeText(baseContent, oursContent, theirsContent);
      
      if (result.conflict) {
        conflicts.push({ path, content: result.content });
        // Write conflict markers to working tree
        const fullPath = join(workDir, path);
        mkdirSync(dirname(fullPath), { recursive: true });
        writeFileSync(fullPath, result.content);
        // Add to index with conflict content
        const conflictHash = createBlob(gitDir, result.content);
        mergedEntries.push({ path, mode: ours?.mode || theirs?.mode || '100644', hash: conflictHash });
      } else {
        const mergedHash = createBlob(gitDir, result.content);
        mergedEntries.push({ path, mode: ours?.mode || theirs?.mode || '100644', hash: mergedHash });
      }
    }
  }
  
  if (conflicts.length > 0) {
    // Write merged (non-conflicting) + conflicting files to working tree
    for (const entry of mergedEntries) {
      if (!conflicts.some(c => c.path === entry.path)) {
        const fullPath = join(workDir, entry.path);
        mkdirSync(dirname(fullPath), { recursive: true });
        writeFileSync(fullPath, readObject(gitDir, entry.hash).content);
      }
    }
    
    writeIndex(gitDir, mergedEntries.map(e => ({
      path: e.path, mode: e.mode, hash: e.hash, size: 0, mtime: Date.now()
    })));
    
    return { type: 'conflict', conflicts: conflicts.map(c => c.path) };
  }
  
  // No conflicts — create merge commit
  writeIndex(gitDir, mergedEntries.map(e => ({
    path: e.path, mode: e.mode, hash: e.hash, size: 0, mtime: Date.now()
  })));
  
  const treeHash = buildTreeFromEntries(gitDir, mergedEntries);
  
  const now = Math.floor(Date.now() / 1000);
  const author = `${authorName} <${authorEmail}> ${now} +0000`;
  const mergeCommitHash = createCommit(
    gitDir, treeHash,
    [oursHash, theirsHash],
    author, author,
    `Merge branch '${branchName}'`
  );
  
  // Update HEAD and working tree
  for (const entry of mergedEntries) {
    const fullPath = join(workDir, entry.path);
    mkdirSync(dirname(fullPath), { recursive: true });
    writeFileSync(fullPath, readObject(gitDir, entry.hash).content);
  }
  
  updateHead(gitDir, mergeCommitHash);
  
  return { type: 'merge', hash: mergeCommitHash };
}

/**
 * Simple text merge with conflict markers.
 */
function mergeText(base, ours, theirs) {
  const baseLines = base.split('\n');
  const oursLines = ours.split('\n');
  const theirsLines = theirs.split('\n');
  
  // Simple approach: if one side didn't change a section, take the other
  // If both changed the same section, mark conflict
  const oursOps = myersDiff(baseLines, oursLines);
  const theirsOps = myersDiff(baseLines, theirsLines);
  
  // Build merged result
  const result = [];
  let hasConflict = false;
  let bi = 0; // base index
  
  // Map base line → changes from ours and theirs
  const oursChanges = new Map(); // base line idx → { type, lines }
  const theirsChanges = new Map();
  
  for (const op of oursOps) {
    if (op.type === 'delete') oursChanges.set(op.aIdx, { type: 'delete' });
    if (op.type === 'insert') {
      const key = op.bIdx !== undefined ? `insert-${op.bIdx}` : `insert-${result.length}`;
      // Track inserts relative to their position
    }
  }
  
  // Simplified merge: just check if both modified the same content
  // If the changes don't overlap, merge is clean
  // If they do overlap, it's a conflict
  
  // For simplicity, if both sides changed the file differently, mark as conflict
  if (ours !== base && theirs !== base && ours !== theirs) {
    hasConflict = true;
    const content = [
      '<<<<<<< HEAD',
      ours,
      '=======',
      theirs,
      '>>>>>>> merge'
    ].join('\n');
    return { conflict: true, content };
  }
  
  // One side changed, other didn't
  if (ours !== base) return { conflict: false, content: ours };
  if (theirs !== base) return { conflict: false, content: theirs };
  return { conflict: false, content: base };
}

/**
 * Find the merge base (common ancestor) of two commits.
 * Simple BFS approach.
 */
export function findMergeBase(gitDir, hash1, hash2) {
  if (!hash1 || !hash2) return null;
  
  // Get all ancestors of hash1
  const ancestors1 = new Set();
  const queue1 = [hash1];
  while (queue1.length > 0) {
    const h = queue1.shift();
    if (ancestors1.has(h)) continue;
    ancestors1.add(h);
    
    try {
      const commit = parseCommit(readObject(gitDir, h).content);
      for (const parent of commit.parents) {
        queue1.push(parent);
      }
    } catch { break; }
  }
  
  // BFS from hash2, find first ancestor that's in ancestors1
  const queue2 = [hash2];
  const visited = new Set();
  while (queue2.length > 0) {
    const h = queue2.shift();
    if (visited.has(h)) continue;
    visited.add(h);
    
    if (ancestors1.has(h)) return h;
    
    try {
      const commit = parseCommit(readObject(gitDir, h).content);
      for (const parent of commit.parents) {
        queue2.push(parent);
      }
    } catch { break; }
  }
  
  return null;
}
