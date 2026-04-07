// fulltext.js — Full-text search engine for HenryDB
// Inverted index with tokenization, posting lists, and TF-IDF scoring.

// Common English stop words
const STOP_WORDS = new Set([
  'a', 'an', 'the', 'and', 'or', 'but', 'is', 'are', 'was', 'were',
  'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
  'will', 'would', 'could', 'should', 'may', 'might', 'shall',
  'can', 'to', 'of', 'in', 'for', 'on', 'with', 'at', 'by', 'from',
  'as', 'into', 'through', 'during', 'before', 'after', 'above',
  'below', 'between', 'out', 'off', 'over', 'under', 'again',
  'further', 'then', 'once', 'here', 'there', 'when', 'where',
  'why', 'how', 'all', 'both', 'each', 'few', 'more', 'most',
  'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own',
  'same', 'so', 'than', 'too', 'very', 'just', 'it', 'its',
  'this', 'that', 'these', 'those', 'i', 'me', 'my', 'we', 'our',
  'you', 'your', 'he', 'him', 'his', 'she', 'her', 'they', 'them',
  'their', 'what', 'which', 'who', 'whom',
]);

/**
 * Tokenize text into searchable terms.
 * - Lowercase
 * - Split on non-alphanumeric characters
 * - Remove stop words
 * - Remove tokens shorter than 2 characters
 */
export function tokenize(text) {
  if (!text || typeof text !== 'string') return [];
  return text
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(t => t.length >= 2 && !STOP_WORDS.has(t));
}

/**
 * Tokenize preserving positions (for phrase queries).
 */
export function tokenizeWithPositions(text) {
  if (!text || typeof text !== 'string') return [];
  const tokens = [];
  let pos = 0;
  for (const word of text.toLowerCase().split(/[^a-z0-9]+/)) {
    if (word.length >= 2 && !STOP_WORDS.has(word)) {
      tokens.push({ term: word, position: pos });
    }
    pos++;
  }
  return tokens;
}

/**
 * Posting: a single document occurrence of a term.
 */
class Posting {
  constructor(docId, positions = []) {
    this.docId = docId;
    this.positions = positions; // Array of word positions
    this.tf = positions.length; // Term frequency in this document
  }
}

/**
 * Inverted Index for full-text search.
 * Maps terms → posting lists (sorted by docId).
 */
export class InvertedIndex {
  constructor(name, tableName, column) {
    this.name = name;
    this.tableName = tableName;
    this.column = column;
    this._index = new Map(); // term → Posting[]
    this._docLengths = new Map(); // docId → number of terms
    this._totalDocs = 0;
    this._avgDocLength = 0;
  }

  /**
   * Add a document to the index.
   * @param {*} docId - Document identifier (e.g., row's primary key or rid)
   * @param {string} text - Text content to index
   */
  addDocument(docId, text) {
    const tokens = tokenizeWithPositions(text);
    this._docLengths.set(docId, tokens.length);
    this._totalDocs++;
    this._avgDocLength = 
      [...this._docLengths.values()].reduce((a, b) => a + b, 0) / this._totalDocs;

    // Group positions by term
    const termPositions = new Map();
    for (const { term, position } of tokens) {
      if (!termPositions.has(term)) termPositions.set(term, []);
      termPositions.get(term).push(position);
    }

    // Add postings
    for (const [term, positions] of termPositions) {
      if (!this._index.has(term)) this._index.set(term, []);
      this._index.get(term).push(new Posting(docId, positions));
    }
  }

  /**
   * Remove a document from the index.
   */
  removeDocument(docId) {
    for (const [term, postings] of this._index) {
      this._index.set(term, postings.filter(p => p.docId !== docId));
      if (this._index.get(term).length === 0) this._index.delete(term);
    }
    this._docLengths.delete(docId);
    this._totalDocs = Math.max(0, this._totalDocs - 1);
    if (this._totalDocs > 0) {
      this._avgDocLength = 
        [...this._docLengths.values()].reduce((a, b) => a + b, 0) / this._totalDocs;
    }
  }

  /**
   * Search for documents matching a term.
   * Returns posting list sorted by docId.
   */
  search(term) {
    return this._index.get(term.toLowerCase()) || [];
  }

  /**
   * Search with TF-IDF scoring.
   * Returns [{ docId, score }] sorted by score descending.
   */
  searchWithScore(terms) {
    const queryTerms = typeof terms === 'string' ? tokenize(terms) : terms;
    const scores = new Map(); // docId → score

    for (const term of queryTerms) {
      const postings = this.search(term);
      if (postings.length === 0) continue;

      // IDF: log(N / df) where N = total docs, df = docs containing term
      const idf = Math.log((this._totalDocs + 1) / (postings.length + 1)) + 1;

      for (const posting of postings) {
        // TF-IDF: tf * idf
        // Normalized TF: tf / docLength (prevents bias toward long documents)
        const docLength = this._docLengths.get(posting.docId) || 1;
        const normalizedTf = posting.tf / docLength;
        const score = normalizedTf * idf;

        scores.set(posting.docId, (scores.get(posting.docId) || 0) + score);
      }
    }

    return [...scores.entries()]
      .map(([docId, score]) => ({ docId, score }))
      .sort((a, b) => b.score - a.score);
  }

  /**
   * Boolean AND search: all terms must appear.
   */
  searchAnd(terms) {
    const queryTerms = typeof terms === 'string' ? tokenize(terms) : terms;
    if (queryTerms.length === 0) return [];

    // Get posting lists for each term
    const postingLists = queryTerms.map(t => this.search(t));
    if (postingLists.some(p => p.length === 0)) return []; // AND requires all terms

    // Intersect posting lists (by docId)
    let result = new Set(postingLists[0].map(p => p.docId));
    for (let i = 1; i < postingLists.length; i++) {
      const next = new Set(postingLists[i].map(p => p.docId));
      result = new Set([...result].filter(id => next.has(id)));
    }

    return [...result];
  }

  /**
   * Boolean OR search: any term can appear.
   */
  searchOr(terms) {
    const queryTerms = typeof terms === 'string' ? tokenize(terms) : terms;
    const result = new Set();
    for (const term of queryTerms) {
      for (const posting of this.search(term)) {
        result.add(posting.docId);
      }
    }
    return [...result];
  }

  /**
   * Get index statistics.
   */
  stats() {
    return {
      terms: this._index.size,
      documents: this._totalDocs,
      avgDocLength: this._avgDocLength,
    };
  }
}

export { STOP_WORDS };
