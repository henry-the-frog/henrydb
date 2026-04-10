// commands.js — High-level git commands (init, commit, log)

import { mkdirSync, writeFileSync, existsSync } from 'fs';
import { join } from 'path';
import { readIndex, buildTreeFromEntries } from './index.js';
import { createCommit, readObject, parseCommit } from './objects.js';
import { readHead, writeHead, resolveHead, updateHead, getCurrentBranch } from './refs.js';

/**
 * Initialize a new git repository.
 */
export function init(workDir) {
  const gitDir = join(workDir, '.git');
  mkdirSync(join(gitDir, 'objects'), { recursive: true });
  mkdirSync(join(gitDir, 'refs', 'heads'), { recursive: true });
  mkdirSync(join(gitDir, 'refs', 'tags'), { recursive: true });
  writeHead(gitDir, { type: 'ref', ref: 'refs/heads/main' });
  writeFileSync(join(gitDir, 'config'), '[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n');
  writeFileSync(join(gitDir, 'description'), 'Unnamed repository; edit this file to name the repository.\n');
  return gitDir;
}

/**
 * Create a commit from the current index.
 */
export function commit(gitDir, workDir, message, authorName = 'User', authorEmail = 'user@example.com') {
  const index = readIndex(gitDir);
  if (index.length === 0) {
    throw new Error('Nothing to commit (empty index)');
  }
  
  // Build tree from index
  const treeHash = buildTreeFromEntries(gitDir, index);
  
  // Get parent commit
  const parentHash = resolveHead(gitDir);
  const parents = parentHash ? [parentHash] : [];
  
  // Create author/committer string
  const now = Math.floor(Date.now() / 1000);
  const tz = formatTimezone(new Date().getTimezoneOffset());
  const author = `${authorName} <${authorEmail}> ${now} ${tz}`;
  
  // Create commit
  const commitHash = createCommit(gitDir, treeHash, parents, author, author, message);
  
  // Update HEAD
  updateHead(gitDir, commitHash);
  
  return commitHash;
}

/**
 * Walk the commit graph and return log entries.
 */
export function log(gitDir, maxCount = Infinity) {
  const entries = [];
  let hash = resolveHead(gitDir);
  
  while (hash && entries.length < maxCount) {
    const obj = readObject(gitDir, hash);
    const commitData = parseCommit(obj.content);
    entries.push({ hash, ...commitData });
    hash = commitData.parents[0] || null; // Follow first parent
  }
  
  return entries;
}

/**
 * Format log entries as text.
 */
export function formatLog(entries) {
  return entries.map(entry => {
    const lines = [];
    lines.push(`commit ${entry.hash}`);
    if (entry.parents.length > 1) {
      lines.push(`Merge: ${entry.parents.map(p => p.slice(0, 7)).join(' ')}`);
    }
    lines.push(`Author: ${entry.author}`);
    lines.push('');
    lines.push(`    ${entry.message}`);
    lines.push('');
    return lines.join('\n');
  }).join('\n');
}

function formatTimezone(offsetMinutes) {
  const sign = offsetMinutes <= 0 ? '+' : '-';
  const abs = Math.abs(offsetMinutes);
  const hours = String(Math.floor(abs / 60)).padStart(2, '0');
  const mins = String(abs % 60).padStart(2, '0');
  return `${sign}${hours}${mins}`;
}
