// diff.js — Myers diff algorithm + unified diff output

/**
 * Myers diff algorithm: find the shortest edit script between two sequences.
 * Returns an array of operations: { type: 'equal'|'insert'|'delete', value }
 * 
 * Based on "An O(ND) Difference Algorithm and Its Variations" by Eugene W. Myers (1986)
 */
export function myersDiff(a, b) {
  const n = a.length;
  const m = b.length;
  const max = n + m;
  
  // V[k] = furthest reaching x on diagonal k
  // Offset by max to handle negative indices
  const v = new Array(2 * max + 1);
  v[max + 1] = 0; // V[1] = 0 (start sentinel)
  
  // Track the edit path for backtracking
  const trace = [];
  
  for (let d = 0; d <= max; d++) {
    trace.push([...v]);
    
    for (let k = -d; k <= d; k += 2) {
      // Decide: move down (insert) or right (delete)
      let x;
      if (k === -d || (k !== d && v[max + k - 1] < v[max + k + 1])) {
        x = v[max + k + 1]; // Move down: insert from b
      } else {
        x = v[max + k - 1] + 1; // Move right: delete from a
      }
      
      let y = x - k;
      
      // Follow diagonal (equal elements)
      while (x < n && y < m && a[x] === b[y]) {
        x++;
        y++;
      }
      
      v[max + k] = x;
      
      if (x >= n && y >= m) {
        // Found the edit script — backtrack to build it
        return backtrack(trace, a, b, n, m, max);
      }
    }
  }
  
  return backtrack(trace, a, b, n, m, max);
}

function backtrack(trace, a, b, n, m, max) {
  const ops = [];
  let x = n;
  let y = m;
  
  for (let d = trace.length - 1; d >= 0; d--) {
    const v = trace[d];
    const k = x - y;
    
    let prevK;
    if (k === -d || (k !== d && v[max + k - 1] < v[max + k + 1])) {
      prevK = k + 1; // Came from above (insert)
    } else {
      prevK = k - 1; // Came from left (delete)
    }
    
    const prevX = v[max + prevK];
    const prevY = prevX - prevK;
    
    // Diagonal moves (equal)
    while (x > prevX && y > prevY) {
      x--;
      y--;
      ops.unshift({ type: 'equal', aIdx: x, bIdx: y, value: a[x] });
    }
    
    if (d > 0) {
      if (x === prevX) {
        // Insert
        y--;
        ops.unshift({ type: 'insert', bIdx: y, value: b[y] });
      } else {
        // Delete
        x--;
        ops.unshift({ type: 'delete', aIdx: x, value: a[x] });
      }
    }
  }
  
  return ops;
}

/**
 * Compute diff between two strings (line-by-line).
 */
export function diffLines(oldText, newText) {
  const oldLines = oldText ? oldText.split('\n') : [];
  const newLines = newText ? newText.split('\n') : [];
  return myersDiff(oldLines, newLines);
}

/**
 * Format diff as unified diff output.
 * @param {string} oldPath - Path of old file
 * @param {string} newPath - Path of new file
 * @param {Array} ops - Diff operations from myersDiff
 * @param {number} context - Number of context lines (default 3)
 */
export function formatUnifiedDiff(oldPath, newPath, ops, context = 3) {
  if (ops.length === 0) return '';
  
  // Group operations into hunks
  const hunks = [];
  let currentHunk = null;
  
  for (let i = 0; i < ops.length; i++) {
    const op = ops[i];
    if (op.type !== 'equal') {
      // Start or extend a hunk
      if (!currentHunk) {
        // Start new hunk with context before
        const start = Math.max(0, i - context);
        currentHunk = { start, ops: [] };
        for (let j = start; j < i; j++) {
          currentHunk.ops.push(ops[j]);
        }
      }
      currentHunk.ops.push(op);
      currentHunk.end = i;
    } else if (currentHunk) {
      // Equal line after a change
      const distToNext = findNextChange(ops, i);
      if (distToNext !== -1 && distToNext <= 2 * context) {
        // Close to another change — include in same hunk
        currentHunk.ops.push(op);
        currentHunk.end = i;
      } else {
        // Add trailing context and close hunk
        const trailingEnd = Math.min(ops.length, i + context);
        for (let j = i; j < trailingEnd; j++) {
          currentHunk.ops.push(ops[j]);
        }
        currentHunk.end = trailingEnd - 1;
        hunks.push(currentHunk);
        currentHunk = null;
      }
    }
  }
  
  if (currentHunk) {
    const trailingEnd = Math.min(ops.length, (currentHunk.end || ops.length - 1) + context + 1);
    for (let j = (currentHunk.end || 0) + 1; j < trailingEnd; j++) {
      if (!currentHunk.ops.includes(ops[j])) {
        currentHunk.ops.push(ops[j]);
      }
    }
    hunks.push(currentHunk);
  }
  
  if (hunks.length === 0) return '';
  
  // Format
  const lines = [];
  lines.push(`--- a/${oldPath}`);
  lines.push(`+++ b/${newPath}`);
  
  for (const hunk of hunks) {
    // Calculate line numbers
    let oldStart = 1, oldCount = 0, newStart = 1, newCount = 0;
    let firstOld = true, firstNew = true;
    
    for (const op of hunk.ops) {
      if (op.type === 'equal' || op.type === 'delete') {
        if (firstOld) { oldStart = (op.aIdx || 0) + 1; firstOld = false; }
        oldCount++;
      }
      if (op.type === 'equal' || op.type === 'insert') {
        if (firstNew) { newStart = (op.bIdx || 0) + 1; firstNew = false; }
        newCount++;
      }
    }
    
    lines.push(`@@ -${oldStart},${oldCount} +${newStart},${newCount} @@`);
    
    for (const op of hunk.ops) {
      if (op.type === 'equal') lines.push(` ${op.value}`);
      else if (op.type === 'delete') lines.push(`-${op.value}`);
      else if (op.type === 'insert') lines.push(`+${op.value}`);
    }
  }
  
  return lines.join('\n');
}

function findNextChange(ops, fromIdx) {
  for (let i = fromIdx + 1; i < ops.length; i++) {
    if (ops[i].type !== 'equal') return i - fromIdx;
  }
  return -1;
}
