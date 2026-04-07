// docstore.js — Document store for HenryDB (MongoDB-like)
// Stores JSON documents with flexible schema, supports dot-notation queries.

/**
 * Document Store: schema-free JSON document storage.
 */
export class DocumentStore {
  constructor(name) {
    this.name = name;
    this._documents = new Map(); // _id → document
    this._nextId = 1;
    this._indexes = new Map(); // field → Map<value, Set<id>>
  }

  /**
   * Insert a document. Auto-generates _id if not provided.
   */
  insert(doc) {
    const id = doc._id || this._nextId++;
    const stored = { _id: id, ...doc };
    this._documents.set(id, stored);
    this._updateIndexes(stored);
    return id;
  }

  /**
   * Insert multiple documents.
   */
  insertMany(docs) {
    return docs.map(doc => this.insert(doc));
  }

  /**
   * Find documents matching a query object.
   * Supports: equality, $gt, $lt, $gte, $lte, $ne, $in, $exists
   */
  find(query = {}) {
    const results = [];
    for (const doc of this._documents.values()) {
      if (this._matches(doc, query)) results.push({ ...doc });
    }
    return results;
  }

  /**
   * Find one document.
   */
  findOne(query) {
    for (const doc of this._documents.values()) {
      if (this._matches(doc, query)) return { ...doc };
    }
    return null;
  }

  /**
   * Update documents matching query.
   */
  update(query, update) {
    let modified = 0;
    for (const [id, doc] of this._documents) {
      if (this._matches(doc, query)) {
        if (update.$set) {
          for (const [key, val] of Object.entries(update.$set)) {
            this._setNested(doc, key, val);
          }
        }
        if (update.$inc) {
          for (const [key, val] of Object.entries(update.$inc)) {
            const current = this._getNested(doc, key) || 0;
            this._setNested(doc, key, current + val);
          }
        }
        if (update.$unset) {
          for (const key of Object.keys(update.$unset)) {
            delete doc[key];
          }
        }
        modified++;
      }
    }
    return modified;
  }

  /**
   * Delete documents matching query.
   */
  deleteMany(query) {
    let deleted = 0;
    for (const [id, doc] of [...this._documents]) {
      if (this._matches(doc, query)) {
        this._documents.delete(id);
        deleted++;
      }
    }
    return deleted;
  }

  /**
   * Count documents matching query.
   */
  count(query = {}) {
    return this.find(query).length;
  }

  /**
   * Create an index on a field.
   */
  createIndex(field) {
    const idx = new Map();
    for (const doc of this._documents.values()) {
      const val = this._getNested(doc, field);
      if (!idx.has(val)) idx.set(val, new Set());
      idx.get(val).add(doc._id);
    }
    this._indexes.set(field, idx);
  }

  /**
   * Aggregate pipeline (simplified).
   */
  aggregate(pipeline) {
    let docs = [...this._documents.values()];
    
    for (const stage of pipeline) {
      if (stage.$match) {
        docs = docs.filter(doc => this._matches(doc, stage.$match));
      }
      if (stage.$group) {
        const groups = new Map();
        for (const doc of docs) {
          const key = stage.$group._id ? this._getNested(doc, stage.$group._id.replace('$', '')) : null;
          if (!groups.has(key)) groups.set(key, []);
          groups.get(key).push(doc);
        }
        docs = [...groups.entries()].map(([key, groupDocs]) => {
          const result = { _id: key };
          for (const [field, op] of Object.entries(stage.$group)) {
            if (field === '_id') continue;
            if (op.$sum) {
              const path = String(op.$sum).replace('$', '');
              result[field] = groupDocs.reduce((s, d) => s + (this._getNested(d, path) || 0), 0);
            }
            if (op.$count) {
              result[field] = groupDocs.length;
            }
            if (op.$avg) {
              const path = String(op.$avg).replace('$', '');
              const sum = groupDocs.reduce((s, d) => s + (this._getNested(d, path) || 0), 0);
              result[field] = sum / groupDocs.length;
            }
          }
          return result;
        });
      }
      if (stage.$sort) {
        const [field, dir] = Object.entries(stage.$sort)[0];
        docs.sort((a, b) => dir === 1 ? (a[field] > b[field] ? 1 : -1) : (a[field] < b[field] ? 1 : -1));
      }
      if (stage.$limit) docs = docs.slice(0, stage.$limit);
    }
    return docs;
  }

  get size() { return this._documents.size; }

  _matches(doc, query) {
    for (const [key, condition] of Object.entries(query)) {
      const value = this._getNested(doc, key);
      
      if (condition !== null && typeof condition === 'object' && !Array.isArray(condition)) {
        // Operator queries
        if (condition.$gt !== undefined && !(value > condition.$gt)) return false;
        if (condition.$lt !== undefined && !(value < condition.$lt)) return false;
        if (condition.$gte !== undefined && !(value >= condition.$gte)) return false;
        if (condition.$lte !== undefined && !(value <= condition.$lte)) return false;
        if (condition.$ne !== undefined && value === condition.$ne) return false;
        if (condition.$in !== undefined && !condition.$in.includes(value)) return false;
        if (condition.$exists !== undefined) {
          if (condition.$exists && value === undefined) return false;
          if (!condition.$exists && value !== undefined) return false;
        }
      } else {
        // Direct equality
        if (value !== condition) return false;
      }
    }
    return true;
  }

  _getNested(obj, path) {
    const parts = path.split('.');
    let current = obj;
    for (const part of parts) {
      if (current == null) return undefined;
      current = current[part];
    }
    return current;
  }

  _setNested(obj, path, value) {
    const parts = path.split('.');
    let current = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!current[parts[i]]) current[parts[i]] = {};
      current = current[parts[i]];
    }
    current[parts[parts.length - 1]] = value;
  }

  _updateIndexes(doc) {
    for (const [field, idx] of this._indexes) {
      const val = this._getNested(doc, field);
      if (!idx.has(val)) idx.set(val, new Set());
      idx.get(val).add(doc._id);
    }
  }
}
