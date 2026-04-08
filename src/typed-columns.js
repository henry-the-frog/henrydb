// typed-columns.js — TypedArray-backed columns for fast numeric operations
// Regular JS arrays box numbers and have overhead from hidden classes.
// TypedArrays (Float64Array, Int32Array) store raw bytes, enabling:
// - No boxing/unboxing overhead
// - Better cache locality (contiguous memory)
// - V8 can use SIMD-like optimizations
// - Predictable memory usage

/**
 * TypedColumn — a column backed by a TypedArray.
 * Supports INT (Int32Array), FLOAT (Float64Array), and nullable variants.
 */
export class TypedColumn {
  constructor(type, initialCapacity = 1024) {
    this.type = type;
    this._length = 0;
    this._capacity = initialCapacity;
    this._nullBitmap = null; // Lazy: only created if nulls are inserted

    switch (type) {
      case 'INT':
      case 'int':
        this._data = new Int32Array(initialCapacity);
        break;
      case 'FLOAT':
      case 'float':
      case 'REAL':
        this._data = new Float64Array(initialCapacity);
        break;
      case 'BIGINT':
      case 'bigint':
        this._data = new BigInt64Array(initialCapacity);
        break;
      default:
        throw new Error(`Unsupported typed column type: ${type}`);
    }
  }

  /**
   * Append a value.
   */
  push(value) {
    if (this._length >= this._capacity) {
      this._grow();
    }

    if (value === null || value === undefined) {
      this._data[this._length] = 0;
      this._ensureNullBitmap();
      this._nullBitmap[this._length] = 1;
    } else {
      this._data[this._length] = this.type === 'BIGINT' || this.type === 'bigint' 
        ? BigInt(value) 
        : value;
    }
    this._length++;
  }

  /**
   * Get value at index (null-aware).
   */
  get(index) {
    if (index < 0 || index >= this._length) return undefined;
    if (this._nullBitmap && this._nullBitmap[index]) return null;
    return this._data[index];
  }

  /**
   * Get raw value at index (ignoring nulls, for fast batch ops).
   */
  getRaw(index) {
    return this._data[index];
  }

  /**
   * Is the value at index null?
   */
  isNull(index) {
    return this._nullBitmap ? this._nullBitmap[index] === 1 : false;
  }

  /**
   * Get the underlying TypedArray (view of active elements).
   */
  toArray() {
    return this._data.subarray(0, this._length);
  }

  /**
   * SUM: vectorized sum of all non-null values.
   */
  sum() {
    const arr = this._data;
    const len = this._length;
    let total = this.type === 'BIGINT' || this.type === 'bigint' ? 0n : 0;
    
    if (!this._nullBitmap) {
      // No nulls: tight loop V8 can optimize
      for (let i = 0; i < len; i++) total += arr[i];
    } else {
      const bitmap = this._nullBitmap;
      for (let i = 0; i < len; i++) {
        if (!bitmap[i]) total += arr[i];
      }
    }
    return total;
  }

  /**
   * SUM over selected indices.
   */
  sumSelection(selection) {
    const arr = this._data;
    let total = 0;
    for (let i = 0; i < selection.length; i++) {
      total += arr[selection[i]];
    }
    return total;
  }

  /**
   * COUNT non-null values.
   */
  count() {
    if (!this._nullBitmap) return this._length;
    let cnt = 0;
    for (let i = 0; i < this._length; i++) {
      if (!this._nullBitmap[i]) cnt++;
    }
    return cnt;
  }

  /**
   * AVG of non-null values.
   */
  avg() {
    const cnt = this.count();
    return cnt > 0 ? Number(this.sum()) / cnt : null;
  }

  /**
   * MIN of non-null values.
   */
  min() {
    const arr = this._data;
    const len = this._length;
    let result = Infinity;
    let found = false;

    if (!this._nullBitmap) {
      for (let i = 0; i < len; i++) {
        if (arr[i] < result) { result = arr[i]; found = true; }
      }
    } else {
      const bitmap = this._nullBitmap;
      for (let i = 0; i < len; i++) {
        if (!bitmap[i] && arr[i] < result) { result = arr[i]; found = true; }
      }
    }
    return found ? result : null;
  }

  /**
   * MAX of non-null values.
   */
  max() {
    const arr = this._data;
    const len = this._length;
    let result = -Infinity;
    let found = false;

    if (!this._nullBitmap) {
      for (let i = 0; i < len; i++) {
        if (arr[i] > result) { result = arr[i]; found = true; }
      }
    } else {
      const bitmap = this._nullBitmap;
      for (let i = 0; i < len; i++) {
        if (!bitmap[i] && arr[i] > result) { result = arr[i]; found = true; }
      }
    }
    return found ? result : null;
  }

  /**
   * Filter: return indices where predicate is true.
   * Predicate is (value) => boolean.
   */
  filter(predicate) {
    const arr = this._data;
    const len = this._length;
    const result = new Uint32Array(len);
    let count = 0;

    if (!this._nullBitmap) {
      for (let i = 0; i < len; i++) {
        if (predicate(arr[i])) result[count++] = i;
      }
    } else {
      const bitmap = this._nullBitmap;
      for (let i = 0; i < len; i++) {
        if (!bitmap[i] && predicate(arr[i])) result[count++] = i;
      }
    }

    return result.subarray(0, count);
  }

  /**
   * Equality filter: return indices where value equals target.
   * Specialized tight loop for integer comparison.
   */
  filterEquals(target) {
    const arr = this._data;
    const len = this._length;
    const result = new Uint32Array(len);
    let count = 0;

    for (let i = 0; i < len; i++) {
      if (arr[i] === target) result[count++] = i;
    }

    return result.subarray(0, count);
  }

  /**
   * Range filter: return indices where value is in [lo, hi].
   */
  filterRange(lo, hi) {
    const arr = this._data;
    const len = this._length;
    const result = new Uint32Array(len);
    let count = 0;

    for (let i = 0; i < len; i++) {
      if (arr[i] >= lo && arr[i] <= hi) result[count++] = i;
    }

    return result.subarray(0, count);
  }

  /**
   * Greater-than filter.
   */
  filterGT(threshold) {
    const arr = this._data;
    const len = this._length;
    const result = new Uint32Array(len);
    let count = 0;

    for (let i = 0; i < len; i++) {
      if (arr[i] > threshold) result[count++] = i;
    }

    return result.subarray(0, count);
  }

  /**
   * Less-than filter.
   */
  filterLT(threshold) {
    const arr = this._data;
    const len = this._length;
    const result = new Uint32Array(len);
    let count = 0;

    for (let i = 0; i < len; i++) {
      if (arr[i] < threshold) result[count++] = i;
    }

    return result.subarray(0, count);
  }

  get length() {
    return this._length;
  }

  _grow() {
    const newCapacity = this._capacity * 2;
    const NewArrayType = this._data.constructor;
    const newData = new NewArrayType(newCapacity);
    newData.set(this._data);
    this._data = newData;
    
    if (this._nullBitmap) {
      const newBitmap = new Uint8Array(newCapacity);
      newBitmap.set(this._nullBitmap);
      this._nullBitmap = newBitmap;
    }
    
    this._capacity = newCapacity;
  }

  _ensureNullBitmap() {
    if (!this._nullBitmap) {
      this._nullBitmap = new Uint8Array(this._capacity);
    }
  }
}
