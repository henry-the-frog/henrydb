// radix-sort.js — O(n·k) non-comparison integer sorting
// Uses counting sort as subroutine. Sorts by digit from LSD to MSD.
// Used in database engines for integer column sorting (faster than quicksort).
// The 50th data structure in HenryDB's library! 🎉

/**
 * Counting sort — O(n + k) stable sort for small integer ranges.
 */
export function countingSort(arr, max) {
  const count = new Array(max + 1).fill(0);
  for (const x of arr) count[x]++;
  const result = [];
  for (let i = 0; i <= max; i++) {
    for (let j = 0; j < count[i]; j++) result.push(i);
  }
  return result;
}

/**
 * Radix sort — O(n·d) where d = number of digits.
 * Sorts non-negative integers. Stable.
 */
export function radixSort(arr) {
  if (arr.length <= 1) return [...arr];
  
  const max = Math.max(...arr);
  let result = [...arr];
  
  for (let exp = 1; max / exp >= 1; exp *= 10) {
    result = countingSortByDigit(result, exp);
  }
  
  return result;
}

function countingSortByDigit(arr, exp) {
  const output = new Array(arr.length);
  const count = new Array(10).fill(0);
  
  for (const x of arr) count[Math.floor(x / exp) % 10]++;
  for (let i = 1; i < 10; i++) count[i] += count[i - 1];
  
  for (let i = arr.length - 1; i >= 0; i--) {
    const digit = Math.floor(arr[i] / exp) % 10;
    output[count[digit] - 1] = arr[i];
    count[digit]--;
  }
  
  return output;
}

/**
 * Bucket sort — O(n) average for uniformly distributed floats in [0, 1).
 */
export function bucketSort(arr, bucketCount) {
  const n = bucketCount || arr.length;
  const buckets = Array.from({ length: n }, () => []);
  
  for (const x of arr) {
    const idx = Math.min(Math.floor(x * n), n - 1);
    buckets[idx].push(x);
  }
  
  for (const bucket of buckets) bucket.sort((a, b) => a - b);
  return buckets.flat();
}
