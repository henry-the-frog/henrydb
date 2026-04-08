// predicate-index.js — Index by predicate expressions
// Stores rows indexed by evaluated predicates for fast complex queries.

export class PredicateIndex {
  constructor() {
    this._predicates = new Map(); // name → {fn, index: Map<bool, Set<rowId>>}
  }

  /** Register a predicate */
  addPredicate(name, fn) {
    this._predicates.set(name, { fn, trueSet: new Set(), falseSet: new Set() });
  }

  /** Index a row */
  indexRow(rowId, row) {
    for (const [, pred] of this._predicates) {
      if (pred.fn(row)) pred.trueSet.add(rowId);
      else pred.falseSet.add(rowId);
    }
  }

  /** Query by predicate name */
  query(name, value = true) {
    const pred = this._predicates.get(name);
    if (!pred) return new Set();
    return value ? new Set(pred.trueSet) : new Set(pred.falseSet);
  }

  /** AND two predicate results */
  and(name1, name2) {
    const s1 = this.query(name1);
    const s2 = this.query(name2);
    return new Set([...s1].filter(x => s2.has(x)));
  }

  /** OR two predicate results */
  or(name1, name2) {
    const s1 = this.query(name1);
    const s2 = this.query(name2);
    return new Set([...s1, ...s2]);
  }

  get predicateCount() { return this._predicates.size; }
}

/**
 * Persistent B+ Tree — copy-on-write versioned B+ tree.
 * Each mutation creates a new root while sharing unchanged nodes.
 */
export class PersistentBPTree {
  constructor(order = 4) {
    this.order = order;
    this.root = { type: 'leaf', keys: [], values: [] };
    this.version = 0;
    this._versions = [this.root];
  }

  /** Insert returns a new tree version */
  insert(key, value) {
    const newRoot = this._insertNode(this.root, key, value);
    this.version++;
    
    if (newRoot.keys.length >= this.order) {
      const [left, right, splitKey] = this._splitNode(newRoot);
      const root = { type: 'internal', keys: [splitKey], children: [left, right] };
      this._versions.push(root);
      this.root = root;
    } else {
      this._versions.push(newRoot);
      this.root = newRoot;
    }
    
    return this;
  }

  _insertNode(node, key, value) {
    if (node.type === 'leaf') {
      const newKeys = [...node.keys];
      const newValues = [...node.values];
      const idx = newKeys.findIndex(k => k >= key);
      if (idx >= 0 && newKeys[idx] === key) {
        newValues[idx] = value;
      } else if (idx >= 0) {
        newKeys.splice(idx, 0, key);
        newValues.splice(idx, 0, value);
      } else {
        newKeys.push(key);
        newValues.push(value);
      }
      return { type: 'leaf', keys: newKeys, values: newValues };
    }

    // Internal node — COW descent
    const idx = node.keys.findIndex(k => key < k);
    const childIdx = idx >= 0 ? idx : node.children.length - 1;
    const newChild = this._insertNode(node.children[childIdx], key, value);
    
    if (newChild.keys.length >= this.order) {
      const [left, right, splitKey] = this._splitNode(newChild);
      const newKeys = [...node.keys];
      const newChildren = [...node.children];
      newKeys.splice(childIdx, 0, splitKey);
      newChildren.splice(childIdx, 1, left, right);
      return { type: 'internal', keys: newKeys, children: newChildren };
    }
    
    const newChildren = [...node.children];
    newChildren[childIdx] = newChild;
    return { type: 'internal', keys: [...node.keys], children: newChildren };
  }

  _splitNode(node) {
    const mid = Math.floor(node.keys.length / 2);
    if (node.type === 'leaf') {
      return [
        { type: 'leaf', keys: node.keys.slice(0, mid), values: node.values.slice(0, mid) },
        { type: 'leaf', keys: node.keys.slice(mid), values: node.values.slice(mid) },
        node.keys[mid],
      ];
    }
    return [
      { type: 'internal', keys: node.keys.slice(0, mid), children: node.children.slice(0, mid + 1) },
      { type: 'internal', keys: node.keys.slice(mid + 1), children: node.children.slice(mid + 1) },
      node.keys[mid],
    ];
  }

  search(key, root = this.root) {
    let node = root;
    while (node.type === 'internal') {
      const idx = node.keys.findIndex(k => key < k);
      node = idx >= 0 ? node.children[idx] : node.children[node.children.length - 1];
    }
    const idx = node.keys.indexOf(key);
    return idx >= 0 ? node.values[idx] : undefined;
  }

  /** Search at a specific version */
  searchAt(key, version) {
    if (version >= this._versions.length) return undefined;
    return this.search(key, this._versions[version]);
  }

  get versionCount() { return this._versions.length; }
}
