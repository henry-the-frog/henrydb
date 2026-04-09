// sort-external.js — External merge sort for data larger than memory
// Simulates sorting chunks that don't fit in memory by splitting into runs.
export class ExternalSort {
  static sort(data, chunkSize = 100) {
    // Phase 1: Sort chunks
    const runs = [];
    for (let i = 0; i < data.length; i += chunkSize) {
      runs.push(data.slice(i, i + chunkSize).sort((a, b) => a - b));
    }
    
    // Phase 2: K-way merge
    return ExternalSort._kWayMerge(runs);
  }

  static _kWayMerge(runs) {
    const result = [];
    const ptrs = runs.map(() => 0);
    
    while (true) {
      let minVal = Infinity, minRun = -1;
      for (let i = 0; i < runs.length; i++) {
        if (ptrs[i] < runs[i].length && runs[i][ptrs[i]] < minVal) {
          minVal = runs[i][ptrs[i]];
          minRun = i;
        }
      }
      if (minRun === -1) break;
      result.push(minVal);
      ptrs[minRun]++;
    }
    return result;
  }
}
