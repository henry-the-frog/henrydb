/**
 * Uniqueness Types (Clean-style)
 * 
 * A unique reference guarantees there is exactly ONE reference to a value.
 * This enables safe in-place mutation in a pure language:
 *   - Unique Array can be updated in-place (no copy needed)
 *   - After update, old reference is consumed (cannot be used)
 * 
 * Similar to Rust's ownership, but at the type level.
 * 
 * Types: T* (unique T) vs T (shared T)
 * Rules:
 *   - Unique values cannot be duplicated
 *   - Unique → Shared (coercion): lose uniqueness, gain sharing
 *   - Shared → Unique: not allowed (can't regain uniqueness)
 */

class TUnique { constructor(inner) { this.tag = 'TUnique'; this.inner = inner; } toString() { return `${this.inner}*`; } }
class TShared { constructor(inner) { this.tag = 'TShared'; this.inner = inner; } toString() { return `${this.inner}`; } }
class TBase { constructor(name) { this.tag = 'TBase'; this.name = name; } toString() { return this.name; } }
class TArray { constructor(elem) { this.tag = 'TArray'; this.elem = elem; } toString() { return `[${this.elem}]`; } }

const tInt = new TBase('Int');
const tStr = new TBase('Str');

// ============================================================
// Uniqueness Checker
// ============================================================

class UniquenessChecker {
  constructor() {
    this.consumed = new Set(); // Variables that have been consumed
    this.errors = [];
  }

  /**
   * Check if a variable is still alive (not consumed)
   */
  use(varName, isUnique) {
    if (this.consumed.has(varName)) {
      this.errors.push(`Use after consume: ${varName} has already been consumed`);
      return false;
    }
    return true;
  }

  /**
   * Consume a unique variable (marks it as unavailable)
   */
  consume(varName) {
    if (this.consumed.has(varName)) {
      this.errors.push(`Double consume: ${varName}`);
      return false;
    }
    this.consumed.add(varName);
    return true;
  }

  /**
   * Check a sequence of operations
   */
  checkSequence(ops) {
    for (const op of ops) {
      switch (op.kind) {
        case 'create':
          // Create a new unique value
          break;
        case 'use':
          this.use(op.var, op.unique);
          break;
        case 'consume':
          this.consume(op.var);
          break;
        case 'update':
          // In-place update: consume old, create new
          if (this.consume(op.var)) {
            // The result is a new unique reference
          }
          break;
        case 'share':
          // Convert unique to shared: consume uniqueness
          this.consume(op.var);
          break;
      }
    }
    return { ok: this.errors.length === 0, errors: this.errors };
  }
}

// ============================================================
// Unique Array operations
// ============================================================

class UniqueArray {
  constructor(data) {
    this._data = [...data];
    this._consumed = false;
    this._id = UniqueArray._nextId++;
  }
  static _nextId = 0;

  /**
   * Read (non-destructive, preserves uniqueness)
   */
  get(index) {
    if (this._consumed) throw new Error('Use after consume');
    return this._data[index];
  }

  /**
   * In-place update (consumes this, returns new unique array)
   */
  set(index, value) {
    if (this._consumed) throw new Error('Use after consume');
    this._consumed = true;
    const newArr = new UniqueArray(this._data);
    newArr._data[index] = value;
    return newArr;
  }

  /**
   * Share: convert to regular (shared) array, lose uniqueness
   */
  share() {
    if (this._consumed) throw new Error('Use after consume');
    this._consumed = true;
    return [...this._data];
  }

  get length() {
    if (this._consumed) throw new Error('Use after consume');
    return this._data.length;
  }
}

// Subtyping: Unique <: Shared (can always share a unique value)
function isSubtype(t1, t2) {
  // Unique T <: Shared T
  if (t1.tag === 'TUnique' && t2.tag === 'TShared') {
    return typeEquals(t1.inner, t2.inner);
  }
  // Same kind: check inner
  if (t1.tag === t2.tag) {
    if (t1.tag === 'TBase') return t1.name === t2.name;
    if (t1.tag === 'TUnique' || t1.tag === 'TShared') return typeEquals(t1.inner, t2.inner);
    if (t1.tag === 'TArray') return typeEquals(t1.elem, t2.elem);
  }
  return false;
}

function typeEquals(a, b) {
  if (a.tag !== b.tag) return false;
  if (a.tag === 'TBase') return a.name === b.name;
  if (a.tag === 'TArray') return typeEquals(a.elem, b.elem);
  return true;
}

export {
  TUnique, TShared, TBase, TArray, tInt, tStr,
  UniquenessChecker, UniqueArray, isSubtype, typeEquals
};
