// join-algorithms.js — Classic join algorithm implementations
export function nestedLoopJoin(left, right, predicate) {
  const result = [];
  for (const l of left) for (const r of right) if (predicate(l, r)) result.push({ ...l, ...r });
  return result;
}

export function hashJoin(left, right, leftKey, rightKey) {
  const hash = new Map();
  for (const l of left) {
    const k = l[leftKey];
    if (!hash.has(k)) hash.set(k, []);
    hash.get(k).push(l);
  }
  const result = [];
  for (const r of right) {
    const matches = hash.get(r[rightKey]);
    if (matches) for (const l of matches) result.push({ ...l, ...r });
  }
  return result;
}

export function sortMergeJoin(left, right, leftKey, rightKey) {
  const sortedL = [...left].sort((a, b) => (a[leftKey] < b[leftKey] ? -1 : 1));
  const sortedR = [...right].sort((a, b) => (a[rightKey] < b[rightKey] ? -1 : 1));
  
  const result = [];
  let i = 0, j = 0;
  while (i < sortedL.length && j < sortedR.length) {
    if (sortedL[i][leftKey] === sortedR[j][rightKey]) {
      let k = j;
      while (k < sortedR.length && sortedR[k][rightKey] === sortedL[i][leftKey]) {
        result.push({ ...sortedL[i], ...sortedR[k] });
        k++;
      }
      i++;
    } else if (sortedL[i][leftKey] < sortedR[j][rightKey]) i++;
    else j++;
  }
  return result;
}
