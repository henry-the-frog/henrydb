// inverted-index.js — Inverted Index for Full-Text Search
//
// Maps terms → list of document IDs containing that term.
// Supports boolean queries (AND, OR, NOT) and TF-IDF ranking.
//
// Used in: Elasticsearch, Lucene, PostgreSQL full-text search, SQLite FTS5.

/**
 * Simple tokenizer: lowercase, split on non-alphanumeric, filter stopwords.
 */
const STOP_WORDS = new Set([
  'a', 'an', 'the', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
  'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
  'should', 'may', 'might', 'shall', 'can', 'need', 'dare', 'ought',
  'to', 'of', 'in', 'for', 'on', 'with', 'at', 'by', 'from', 'as',
  'into', 'through', 'during', 'before', 'after', 'above', 'below',
  'and', 'but', 'or', 'nor', 'not', 'so', 'yet', 'both', 'either',
  'it', 'its', 'this', 'that', 'these', 'those', 'i', 'me', 'my',
  'he', 'she', 'they', 'we', 'you', 'who', 'which', 'what', 'where',
]);

export function tokenize(text) {
  return text.toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(t => t.length > 1 && !STOP_WORDS.has(t));
}

/**
 * InvertedIndex — full-text search index.
 */
export class InvertedIndex {
  constructor() {
    this._index = new Map();       // term → Map<docId, termFreq>
    this._docs = new Map();        // docId → {text, termCount}
    this._totalDocs = 0;
  }

  get docCount() { return this._totalDocs; }

  /**
   * Add a document to the index.
   */
  addDocument(docId, text) {
    const tokens = tokenize(text);
    this._docs.set(docId, { text, termCount: tokens.length });
    this._totalDocs++;
    
    // Count term frequencies
    const tf = new Map();
    for (const token of tokens) {
      tf.set(token, (tf.get(token) || 0) + 1);
    }
    
    // Update inverted index
    for (const [term, freq] of tf) {
      if (!this._index.has(term)) this._index.set(term, new Map());
      this._index.get(term).set(docId, freq);
    }
  }

  /**
   * Remove a document from the index.
   */
  removeDocument(docId) {
    const doc = this._docs.get(docId);
    if (!doc) return;
    
    const tokens = tokenize(doc.text);
    for (const token of new Set(tokens)) {
      const postings = this._index.get(token);
      if (postings) {
        postings.delete(docId);
        if (postings.size === 0) this._index.delete(token);
      }
    }
    this._docs.delete(docId);
    this._totalDocs--;
  }

  /**
   * Search for a single term. Returns array of {docId, score}.
   */
  searchTerm(term) {
    const t = term.toLowerCase();
    const postings = this._index.get(t);
    if (!postings) return [];
    
    const results = [];
    for (const [docId, tf] of postings) {
      const idf = Math.log(this._totalDocs / postings.size);
      const doc = this._docs.get(docId);
      const normalizedTf = tf / (doc?.termCount || 1);
      results.push({ docId, score: normalizedTf * idf });
    }
    
    return results.sort((a, b) => b.score - a.score);
  }

  /**
   * Boolean query: supports AND, OR, NOT operators.
   * Input: string like "database AND indexing" or "search OR query"
   */
  search(query) {
    const tokens = query.toLowerCase().split(/\s+/);
    
    // Parse simple boolean expressions
    if (tokens.includes('and')) {
      const terms = tokens.filter(t => t !== 'and');
      return this._intersect(terms);
    }
    
    if (tokens.includes('or')) {
      const terms = tokens.filter(t => t !== 'or');
      return this._union(terms);
    }
    
    if (tokens[0] === 'not' && tokens.length === 2) {
      return this._not(tokens[1]);
    }
    
    // Single term or phrase
    const terms = tokens.filter(t => !STOP_WORDS.has(t) && t.length > 1);
    if (terms.length === 1) return this.searchTerm(terms[0]);
    return this._intersect(terms); // Default: AND for multi-word
  }

  _intersect(terms) {
    if (terms.length === 0) return [];
    
    let result = new Set(this._getDocIds(terms[0]));
    for (let i = 1; i < terms.length; i++) {
      const other = new Set(this._getDocIds(terms[i]));
      result = new Set([...result].filter(id => other.has(id)));
    }
    
    return this._scoreResults([...result], terms);
  }

  _union(terms) {
    const allDocs = new Set();
    for (const term of terms) {
      for (const id of this._getDocIds(term)) allDocs.add(id);
    }
    return this._scoreResults([...allDocs], terms);
  }

  _not(term) {
    const matching = new Set(this._getDocIds(term));
    const results = [];
    for (const docId of this._docs.keys()) {
      if (!matching.has(docId)) {
        results.push({ docId, score: 1 });
      }
    }
    return results;
  }

  _getDocIds(term) {
    const postings = this._index.get(term);
    return postings ? [...postings.keys()] : [];
  }

  _scoreResults(docIds, terms) {
    return docIds.map(docId => {
      let score = 0;
      for (const term of terms) {
        const postings = this._index.get(term);
        if (postings?.has(docId)) {
          const tf = postings.get(docId);
          const idf = Math.log(this._totalDocs / postings.size);
          const doc = this._docs.get(docId);
          score += (tf / (doc?.termCount || 1)) * idf;
        }
      }
      return { docId, score };
    }).sort((a, b) => b.score - a.score);
  }

  /** Get index statistics. */
  getStats() {
    return {
      documents: this._totalDocs,
      terms: this._index.size,
      avgDocLength: this._totalDocs > 0
        ? [...this._docs.values()].reduce((s, d) => s + d.termCount, 0) / this._totalDocs
        : 0,
    };
  }
}
