// wildcard-trie.js — Trie supporting ? (single char) and * (any sequence) wildcards
// Used for LIKE queries, file globbing, topic subscriptions.

class WTNode {
  constructor() { this.children = new Map(); this.value = undefined; this.isEnd = false; }
}

export class WildcardTrie {
  constructor() { this._root = new WTNode(); this._size = 0; }
  get size() { return this._size; }

  insert(pattern, value) {
    let node = this._root;
    for (const ch of pattern) {
      if (!node.children.has(ch)) node.children.set(ch, new WTNode());
      node = node.children.get(ch);
    }
    if (!node.isEnd) this._size++;
    node.isEnd = true;
    node.value = value;
  }

  /** Search with wildcards: ? matches single char, * matches any sequence. */
  match(pattern) {
    const results = [];
    this._match(this._root, pattern, 0, '', results);
    return results;
  }

  /** Check if any stored pattern matches the given text. */
  matchText(text) {
    return this._matchText(this._root, text, 0);
  }

  _match(node, pattern, idx, prefix, results) {
    if (idx === pattern.length) {
      if (node.isEnd) results.push({ key: prefix, value: node.value });
      return;
    }
    
    const ch = pattern[idx];
    if (ch === '?') {
      // Match any single character
      for (const [c, child] of node.children) {
        this._match(child, pattern, idx + 1, prefix + c, results);
      }
    } else if (ch === '*') {
      // Match zero or more characters
      this._match(node, pattern, idx + 1, prefix, results); // Zero match
      for (const [c, child] of node.children) {
        this._match(child, pattern, idx, prefix + c, results); // Keep matching *
        this._match(child, pattern, idx + 1, prefix + c, results); // Move past *
      }
    } else {
      const child = node.children.get(ch);
      if (child) this._match(child, pattern, idx + 1, prefix + ch, results);
    }
  }

  _matchText(node, text, idx) {
    if (!node) return false;
    if (idx === text.length) return node.isEnd;
    
    const ch = text[idx];
    // Check exact match
    if (node.children.has(ch) && this._matchText(node.children.get(ch), text, idx + 1)) return true;
    // Check ? wildcard stored in trie
    if (node.children.has('?') && this._matchText(node.children.get('?'), text, idx + 1)) return true;
    // Check * wildcard stored in trie
    if (node.children.has('*')) {
      const starNode = node.children.get('*');
      for (let i = idx; i <= text.length; i++) {
        if (this._matchText(starNode, text, i)) return true;
      }
    }
    return false;
  }
}
