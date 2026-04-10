// refs.js — Git references (HEAD, branches, tags)

import { readFileSync, writeFileSync, existsSync, mkdirSync, readdirSync, unlinkSync } from 'fs';
import { join, dirname } from 'path';

/**
 * Read HEAD — returns either { type: 'ref', ref: 'refs/heads/main' }
 * or { type: 'hash', hash: 'abc123...' }
 */
export function readHead(gitDir) {
  const headPath = join(gitDir, 'HEAD');
  if (!existsSync(headPath)) return null;
  
  const content = readFileSync(headPath, 'utf8').trim();
  if (content.startsWith('ref: ')) {
    return { type: 'ref', ref: content.slice(5) };
  }
  return { type: 'hash', hash: content };
}

/**
 * Write HEAD.
 */
export function writeHead(gitDir, value) {
  const headPath = join(gitDir, 'HEAD');
  if (value.type === 'ref') {
    writeFileSync(headPath, `ref: ${value.ref}\n`);
  } else {
    writeFileSync(headPath, `${value.hash}\n`);
  }
}

/**
 * Resolve HEAD to a commit hash.
 */
export function resolveHead(gitDir) {
  const head = readHead(gitDir);
  if (!head) return null;
  if (head.type === 'hash') return head.hash;
  return resolveRef(gitDir, head.ref);
}

/**
 * Read a ref (e.g., 'refs/heads/main').
 */
export function resolveRef(gitDir, ref) {
  const refPath = join(gitDir, ref);
  if (!existsSync(refPath)) return null;
  return readFileSync(refPath, 'utf8').trim();
}

/**
 * Write a ref.
 */
export function writeRef(gitDir, ref, hash) {
  const refPath = join(gitDir, ref);
  mkdirSync(dirname(refPath), { recursive: true });
  writeFileSync(refPath, `${hash}\n`);
}

/**
 * Update the current branch (what HEAD points to) with a new hash.
 */
export function updateHead(gitDir, hash) {
  const head = readHead(gitDir);
  if (!head) {
    writeHead(gitDir, { type: 'hash', hash });
    return;
  }
  if (head.type === 'ref') {
    writeRef(gitDir, head.ref, hash);
  } else {
    writeHead(gitDir, { type: 'hash', hash });
  }
}

/**
 * Get current branch name, or null if detached.
 */
export function getCurrentBranch(gitDir) {
  const head = readHead(gitDir);
  if (!head || head.type !== 'ref') return null;
  if (head.ref.startsWith('refs/heads/')) {
    return head.ref.slice('refs/heads/'.length);
  }
  return head.ref;
}

/**
 * List all branches.
 */
export function listBranches(gitDir) {
  const headsDir = join(gitDir, 'refs', 'heads');
  if (!existsSync(headsDir)) return [];
  return readdirSync(headsDir).filter(f => !f.startsWith('.'));
}

/**
 * Create a new branch pointing to a commit.
 */
export function createBranch(gitDir, name, commitHash) {
  writeRef(gitDir, `refs/heads/${name}`, commitHash);
}

/**
 * Delete a branch.
 */
export function deleteBranch(gitDir, name) {
  unlinkSync(join(gitDir, 'refs', 'heads', name));
}
