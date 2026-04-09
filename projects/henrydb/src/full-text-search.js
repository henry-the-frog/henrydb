// full-text-search.js — Full-text search engine for HenryDB
// Implements PostgreSQL-compatible tsvector/tsquery with ranking.

// Common English stop words
const STOP_WORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by', 'do', 'for', 'from',
  'had', 'has', 'have', 'he', 'her', 'his', 'how', 'i', 'if', 'in', 'into',
  'is', 'it', 'its', 'just', 'me', 'my', 'no', 'not', 'of', 'on', 'or', 'our',
  'out', 'over', 'own', 'she', 'so', 'some', 'such', 'than', 'that', 'the',
  'their', 'them', 'then', 'there', 'these', 'they', 'this', 'to', 'too',
  'up', 'us', 'very', 'was', 'we', 'were', 'what', 'when', 'which', 'who',
  'will', 'with', 'would', 'you', 'your',
]);

/**
 * Simple English stemmer (Porter-like, simplified).
 */
function stem(word) {
  let w = word.toLowerCase();
  // Remove common suffixes
  if (w.endsWith('ies') && w.length > 4) w = w.slice(0, -3) + 'y';
  else if (w.endsWith('ing') && w.length > 5) w = w.slice(0, -3);
  else if (w.endsWith('tion') && w.length > 5) w = w.slice(0, -4) + 't';
  else if (w.endsWith('ness') && w.length > 5) w = w.slice(0, -4);
  else if (w.endsWith('ment') && w.length > 5) w = w.slice(0, -4);
  else if (w.endsWith('able') && w.length > 5) w = w.slice(0, -4);
  else if (w.endsWith('ful') && w.length > 4) w = w.slice(0, -3);
  else if (w.endsWith('ous') && w.length > 4) w = w.slice(0, -3);
  else if (w.endsWith('ly') && w.length > 4) w = w.slice(0, -2);
  else if (w.endsWith('ed') && w.length > 4) w = w.slice(0, -2);
  else if (w.endsWith('er') && w.length > 4) w = w.slice(0, -2);
  else if (w.endsWith('es') && w.length > 3) w = w.slice(0, -2);
  else if (w.endsWith('s') && !w.endsWith('ss') && w.length > 3) w = w.slice(0, -1);
  return w;
}

/**
 * TSVector — a sorted list of distinct lexemes with positions.
 */
export class TSVector {
  constructor() {
    this.lexemes = new Map(); // lexeme → { positions: number[], weight: string }
  }

  /**
   * Create a TSVector from text.
   */
  static fromText(text, config = {}) {
    const vec = new TSVector();
    const words = text.toLowerCase().replace(/[^\w\s]/g, ' ').split(/\s+/).filter(Boolean);
    let pos = 0;

    for (const word of words) {
      if (STOP_WORDS.has(word)) continue;
      pos++;
      const lexeme = config.noStem ? word : stem(word);
      if (!vec.lexemes.has(lexeme)) {
        vec.lexemes.set(lexeme, { positions: [], weight: config.weight || 'D' });
      }
      vec.lexemes.get(lexeme).positions.push(pos);
    }

    return vec;
  }

  /**
   * Concatenate two TSVectors (for multi-column search).
   */
  concat(other) {
    const result = new TSVector();
    // Copy this
    for (const [lex, data] of this.lexemes) {
      result.lexemes.set(lex, { positions: [...data.positions], weight: data.weight });
    }
    // Merge other
    const offset = this.maxPosition();
    for (const [lex, data] of other.lexemes) {
      if (result.lexemes.has(lex)) {
        result.lexemes.get(lex).positions.push(...data.positions.map(p => p + offset));
      } else {
        result.lexemes.set(lex, { positions: data.positions.map(p => p + offset), weight: data.weight });
      }
    }
    return result;
  }

  maxPosition() {
    let max = 0;
    for (const data of this.lexemes.values()) {
      for (const p of data.positions) {
        if (p > max) max = p;
      }
    }
    return max;
  }

  /**
   * Check if this vector matches a TSQuery.
   */
  matches(query) {
    return query.evaluate(this);
  }

  toString() {
    return [...this.lexemes.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([lex, data]) => `'${lex}':${data.positions.join(',')}`)
      .join(' ');
  }
}

/**
 * TSQuery — a search query with boolean operators.
 * Supports: & (AND), | (OR), ! (NOT), <-> (followed by / phrase)
 */
export class TSQuery {
  constructor(node) {
    this.node = node;
  }

  /**
   * Parse a search query string.
   * Examples: "cat & dog", "cat | dog", "!cat", "big <-> cat"
   */
  static parse(queryStr) {
    const tokens = tokenizeQuery(queryStr);
    const node = parseQueryExpr(tokens, 0);
    return new TSQuery(node.node);
  }

  /**
   * Create a simple keyword query from plain text.
   * All words are ANDed together.
   */
  static fromPlainText(text) {
    const words = text.toLowerCase().replace(/[^\w\s]/g, ' ').split(/\s+/).filter(Boolean);
    const terms = words.filter(w => !STOP_WORDS.has(w)).map(w => stem(w));

    if (terms.length === 0) return new TSQuery({ type: 'term', value: '' });
    if (terms.length === 1) return new TSQuery({ type: 'term', value: terms[0] });

    let node = { type: 'term', value: terms[0] };
    for (let i = 1; i < terms.length; i++) {
      node = { type: 'and', left: node, right: { type: 'term', value: terms[i] } };
    }
    return new TSQuery(node);
  }

  evaluate(vector) {
    return evalNode(this.node, vector);
  }
}

function evalNode(node, vector) {
  switch (node.type) {
    case 'term':
      return vector.lexemes.has(node.value);
    case 'and':
      return evalNode(node.left, vector) && evalNode(node.right, vector);
    case 'or':
      return evalNode(node.left, vector) || evalNode(node.right, vector);
    case 'not':
      return !evalNode(node.operand, vector);
    case 'phrase': {
      // Check if terms appear adjacent
      const leftPositions = vector.lexemes.get(node.left.value)?.positions || [];
      const rightPositions = vector.lexemes.get(node.right.value)?.positions || [];
      const distance = node.distance || 1;
      return leftPositions.some(lp => rightPositions.some(rp => rp - lp === distance));
    }
    default:
      return false;
  }
}

function tokenizeQuery(str) {
  const tokens = [];
  let i = 0;
  while (i < str.length) {
    if (/\s/.test(str[i])) { i++; continue; }
    if (str[i] === '&') { tokens.push({ type: 'AND' }); i++; continue; }
    if (str[i] === '|') { tokens.push({ type: 'OR' }); i++; continue; }
    if (str[i] === '!') { tokens.push({ type: 'NOT' }); i++; continue; }
    if (str[i] === '(') { tokens.push({ type: 'LPAREN' }); i++; continue; }
    if (str[i] === ')') { tokens.push({ type: 'RPAREN' }); i++; continue; }
    if (str[i] === '<' && str[i + 1] === '-' && str[i + 2] === '>') {
      tokens.push({ type: 'PHRASE' });
      i += 3;
      continue;
    }
    // Term
    let word = '';
    while (i < str.length && /[a-zA-Z0-9_]/.test(str[i])) word += str[i++];
    if (word) tokens.push({ type: 'TERM', value: stem(word.toLowerCase()) });
  }
  return tokens;
}

function parseQueryExpr(tokens, pos) {
  let { node, pos: p } = parseQueryAnd(tokens, pos);
  while (p < tokens.length && tokens[p].type === 'OR') {
    p++;
    const right = parseQueryAnd(tokens, p);
    node = { type: 'or', left: node, right: right.node };
    p = right.pos;
  }
  return { node, pos: p };
}

function parseQueryAnd(tokens, pos) {
  let { node, pos: p } = parseQueryPhrase(tokens, pos);
  while (p < tokens.length && tokens[p].type === 'AND') {
    p++;
    const right = parseQueryPhrase(tokens, p);
    node = { type: 'and', left: node, right: right.node };
    p = right.pos;
  }
  return { node, pos: p };
}

function parseQueryPhrase(tokens, pos) {
  let { node, pos: p } = parseQueryUnary(tokens, pos);
  while (p < tokens.length && tokens[p].type === 'PHRASE') {
    p++;
    const right = parseQueryUnary(tokens, p);
    node = { type: 'phrase', left: node, right: right.node, distance: 1 };
    p = right.pos;
  }
  return { node, pos: p };
}

function parseQueryUnary(tokens, pos) {
  if (pos < tokens.length && tokens[pos].type === 'NOT') {
    const inner = parseQueryUnary(tokens, pos + 1);
    return { node: { type: 'not', operand: inner.node }, pos: inner.pos };
  }
  if (pos < tokens.length && tokens[pos].type === 'LPAREN') {
    const inner = parseQueryExpr(tokens, pos + 1);
    const p = inner.pos < tokens.length && tokens[inner.pos].type === 'RPAREN' ? inner.pos + 1 : inner.pos;
    return { node: inner.node, pos: p };
  }
  if (pos < tokens.length && tokens[pos].type === 'TERM') {
    return { node: { type: 'term', value: tokens[pos].value }, pos: pos + 1 };
  }
  return { node: { type: 'term', value: '' }, pos };
}

/**
 * ts_rank — compute relevance score for a document.
 * Considers term frequency and position weights.
 */
export function tsRank(vector, query, options = {}) {
  const weights = options.weights || { A: 1.0, B: 0.4, C: 0.2, D: 0.1 };
  return rankNode(query.node, vector, weights);
}

function rankNode(node, vector, weights) {
  switch (node.type) {
    case 'term': {
      const data = vector.lexemes.get(node.value);
      if (!data) return 0;
      const w = weights[data.weight] || 0.1;
      // TF-based scoring: log(1 + tf) * weight
      return Math.log(1 + data.positions.length) * w;
    }
    case 'and':
      return rankNode(node.left, vector, weights) + rankNode(node.right, vector, weights);
    case 'or':
      return Math.max(rankNode(node.left, vector, weights), rankNode(node.right, vector, weights));
    case 'not':
      return 0;
    case 'phrase': {
      const leftData = vector.lexemes.get(node.left.value);
      const rightData = vector.lexemes.get(node.right.value);
      if (!leftData || !rightData) return 0;
      // Bonus for phrase matches
      const hasPhrase = leftData.positions.some(lp =>
        rightData.positions.some(rp => rp - lp === (node.distance || 1))
      );
      return hasPhrase ? 1.0 : 0;
    }
    default:
      return 0;
  }
}

/**
 * TextSearchIndex — inverted index for fast full-text search.
 */
export class TextSearchIndex {
  constructor() {
    this._index = new Map(); // lexeme → Set<docId>
    this._vectors = new Map(); // docId → TSVector
    this._docCount = 0;
  }

  addDocument(docId, vector) {
    this._vectors.set(docId, vector);
    for (const lexeme of vector.lexemes.keys()) {
      if (!this._index.has(lexeme)) {
        this._index.set(lexeme, new Set());
      }
      this._index.get(lexeme).add(docId);
    }
    this._docCount++;
  }

  removeDocument(docId) {
    const vector = this._vectors.get(docId);
    if (!vector) return;
    for (const lexeme of vector.lexemes.keys()) {
      const docs = this._index.get(lexeme);
      if (docs) {
        docs.delete(docId);
        if (docs.size === 0) this._index.delete(lexeme);
      }
    }
    this._vectors.delete(docId);
    this._docCount--;
  }

  /**
   * Search the index with a query.
   * Returns matching docIds sorted by rank.
   */
  search(query, options = {}) {
    const limit = options.limit || 100;
    const candidates = this._getCandidates(query.node);
    
    const results = [];
    for (const docId of candidates) {
      const vector = this._vectors.get(docId);
      if (!vector || !vector.matches(query)) continue;
      results.push({ docId, rank: tsRank(vector, query) });
    }

    results.sort((a, b) => b.rank - a.rank);
    return results.slice(0, limit);
  }

  _getCandidates(node) {
    switch (node.type) {
      case 'term':
        return this._index.get(node.value) || new Set();
      case 'and': {
        const left = this._getCandidates(node.left);
        const right = this._getCandidates(node.right);
        return new Set([...left].filter(id => right.has(id)));
      }
      case 'or': {
        const left = this._getCandidates(node.left);
        const right = this._getCandidates(node.right);
        return new Set([...left, ...right]);
      }
      case 'not':
        // NOT can't narrow candidates — return all docs
        return new Set(this._vectors.keys());
      case 'phrase':
        return this._getCandidates(node.left);
      default:
        return new Set();
    }
  }

  getStats() {
    return {
      documents: this._docCount,
      uniqueLexemes: this._index.size,
      avgLexemesPerDoc: this._docCount > 0
        ? +([...this._vectors.values()].reduce((s, v) => s + v.lexemes.size, 0) / this._docCount).toFixed(1)
        : 0,
    };
  }
}
