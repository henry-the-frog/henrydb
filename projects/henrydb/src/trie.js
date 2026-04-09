// trie.js — Trie (prefix tree) for HenryDB
//
// A trie stores strings where each node represents one character.
// Enables efficient prefix searches and autocomplete.
//
// Time complexity: O(k) for insert/search/delete where k = key length
// Space: O(ALPHABET_SIZE * n * k) worst case, but shared prefixes save space

class TrieNode {
  constructor() {
    this.children = new Map();
    this.isEnd = false;
    this.value = undefined;
    this.count = 0; // Number of words passing through this node
  }
}

/**
 * Trie — Prefix tree for string keys.
 */
export class Trie {
  constructor() {
    this._root = new TrieNode();
    this._size = 0;
  }

  get size() { return this._size; }

  /**
   * Insert a key-value pair. O(k).
   */
  insert(key, value = true) {
    let node = this._root;
    for (const ch of key) {
      if (!node.children.has(ch)) {
        node.children.set(ch, new TrieNode());
      }
      node = node.children.get(ch);
      node.count++;
    }
    if (!node.isEnd) this._size++;
    node.isEnd = true;
    node.value = value;
  }

  /**
   * Search for an exact key. O(k).
   */
  get(key) {
    const node = this._findNode(key);
    return node && node.isEnd ? node.value : undefined;
  }

  /**
   * Check if a key exists. O(k).
   */
  has(key) {
    const node = this._findNode(key);
    return node !== null && node.isEnd;
  }

  /**
   * Check if any key starts with the given prefix. O(k).
   */
  hasPrefix(prefix) {
    return this._findNode(prefix) !== null;
  }

  /**
   * Find all keys starting with the given prefix.
   * Returns array of {key, value}.
   */
  prefixSearch(prefix, limit = 100) {
    const node = this._findNode(prefix);
    if (!node) return [];
    
    const results = [];
    this._collect(node, prefix, results, limit);
    return results;
  }

  /**
   * Delete a key. O(k).
   */
  delete(key) {
    if (!this.has(key)) return false;
    
    let node = this._root;
    for (const ch of key) {
      const child = node.children.get(ch);
      child.count--;
      if (child.count === 0) {
        node.children.delete(ch);
        this._size--;
        return true;
      }
      node = child;
    }
    node.isEnd = false;
    node.value = undefined;
    this._size--;
    return true;
  }

  /**
   * Autocomplete: find top completions for a prefix.
   */
  autocomplete(prefix, limit = 10) {
    return this.prefixSearch(prefix, limit).map(r => r.key);
  }

  /**
   * Get the longest common prefix of all keys.
   */
  longestCommonPrefix() {
    let node = this._root;
    let prefix = '';
    
    while (node.children.size === 1 && !node.isEnd) {
      const [ch, child] = [...node.children.entries()][0];
      prefix += ch;
      node = child;
    }
    
    return prefix;
  }

  /**
   * Count keys with a given prefix.
   */
  countPrefix(prefix) {
    const node = this._findNode(prefix);
    return node ? node.count : 0;
  }

  // --- Internal ---

  _findNode(key) {
    let node = this._root;
    for (const ch of key) {
      if (!node.children.has(ch)) return null;
      node = node.children.get(ch);
    }
    return node;
  }

  _collect(node, prefix, results, limit) {
    if (results.length >= limit) return;
    if (node.isEnd) {
      results.push({ key: prefix, value: node.value });
    }
    for (const [ch, child] of node.children) {
      if (results.length >= limit) return;
      this._collect(child, prefix + ch, results, limit);
    }
  }
}
