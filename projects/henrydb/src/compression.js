// rle.js — Run Length Encoding for column compression
export class RLE {
  static encode(arr) {
    if (arr.length === 0) return [];
    const runs = [];
    let current = arr[0], count = 1;
    for (let i = 1; i < arr.length; i++) {
      if (arr[i] === current) count++;
      else { runs.push({ value: current, count }); current = arr[i]; count = 1; }
    }
    runs.push({ value: current, count });
    return runs;
  }

  static decode(runs) {
    const result = [];
    for (const { value, count } of runs) {
      for (let i = 0; i < count; i++) result.push(value);
    }
    return result;
  }

  static ratio(arr) {
    const encoded = RLE.encode(arr);
    return arr.length / encoded.length;
  }
}

// delta.js — Delta Encoding for sorted integer sequences
export class DeltaEncoding {
  static encode(arr) {
    if (arr.length === 0) return [];
    const deltas = [arr[0]];
    for (let i = 1; i < arr.length; i++) deltas.push(arr[i] - arr[i - 1]);
    return deltas;
  }

  static decode(deltas) {
    if (deltas.length === 0) return [];
    const arr = [deltas[0]];
    for (let i = 1; i < deltas.length; i++) arr.push(arr[i - 1] + deltas[i]);
    return arr;
  }
}
