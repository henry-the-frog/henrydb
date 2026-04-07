// composite-key.js — Comparable composite key for multi-column indexes
// Wraps an array of values into an object that supports <, >, === comparisons

export class CompositeKey {
  constructor(values) {
    this.values = values;
    // Create a comparison-friendly representation
    // Pad numbers to fixed width for string-safe comparison
    this._key = values.map(v => {
      if (v == null) return '\x00NULL';
      if (typeof v === 'number') {
        // Handle negative numbers: offset to make all positive, then pad
        const n = v + 1e15; // offset
        return '\x01' + String(n).padStart(20, '0');
      }
      if (typeof v === 'string') return '\x02' + v;
      return '\x03' + String(v);
    }).join('\x00');
  }

  valueOf() { return this._key; }
  toString() { return this._key; }

  // For prefix matching: check if this key starts with the given prefix values
  startsWith(prefixValues) {
    for (let i = 0; i < prefixValues.length; i++) {
      if (i >= this.values.length) return false;
      if (this.values[i] !== prefixValues[i]) return false;
    }
    return true;
  }
}

// Create a composite key from column values
export function makeCompositeKey(values) {
  return new CompositeKey(values);
}
