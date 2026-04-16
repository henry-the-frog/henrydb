/**
 * Type Error Messages: Structured Diagnostics
 * 
 * Good error messages include:
 * 1. What went wrong (mismatch, unbound variable, etc.)
 * 2. Where it happened (source span)
 * 3. What was expected vs found
 * 4. Suggestions for fixing it
 */

class Span {
  constructor(line, col, endLine, endCol, source = '') {
    this.line = line; this.col = col;
    this.endLine = endLine || line; this.endCol = endCol || col;
    this.source = source;
  }
  toString() { return `${this.line}:${this.col}`; }
}

class Diagnostic {
  constructor(severity, message, span = null) {
    this.severity = severity; // 'error' | 'warning' | 'hint'
    this.message = message;
    this.span = span;
    this.notes = [];
    this.suggestions = [];
  }
  
  addNote(message, span = null) {
    this.notes.push({ message, span });
    return this;
  }
  
  addSuggestion(message, replacement = null) {
    this.suggestions.push({ message, replacement });
    return this;
  }
  
  format() {
    const lines = [];
    const prefix = this.severity === 'error' ? '❌' : this.severity === 'warning' ? '⚠️' : '💡';
    lines.push(`${prefix} ${this.severity}: ${this.message}`);
    if (this.span) lines.push(`   at ${this.span}`);
    for (const note of this.notes) {
      lines.push(`   note: ${note.message}${note.span ? ` (at ${note.span})` : ''}`);
    }
    for (const sug of this.suggestions) {
      lines.push(`   suggestion: ${sug.message}`);
      if (sug.replacement) lines.push(`   fix: ${sug.replacement}`);
    }
    return lines.join('\n');
  }
}

// ============================================================
// Error generators for common type errors
// ============================================================

function typeMismatch(expected, found, span = null) {
  const d = new Diagnostic('error', `Type mismatch: expected ${expected}, found ${found}`, span);
  if (expected === 'Int' && found === 'String') {
    d.addSuggestion('Use parseInt() to convert string to number', `parseInt(expr)`);
  }
  if (expected === 'String' && found === 'Int') {
    d.addSuggestion('Use toString() to convert number to string', `expr.toString()`);
  }
  return d;
}

function unboundVariable(name, span = null, similar = []) {
  const d = new Diagnostic('error', `Unbound variable: ${name}`, span);
  if (similar.length > 0) {
    d.addSuggestion(`Did you mean: ${similar.join(' or ')}?`);
  }
  return d;
}

function arityMismatch(fn, expected, found, span = null) {
  return new Diagnostic('error', `${fn} expects ${expected} argument(s), but got ${found}`, span);
}

function infiniteType(varName, type, span = null) {
  return new Diagnostic('error', `Infinite type: ${varName} occurs in ${type}`, span)
    .addNote('This would create an infinite recursive type');
}

function missingField(recordType, field, span = null) {
  return new Diagnostic('error', `Record type ${recordType} has no field '${field}'`, span);
}

function unusedVariable(name, span = null) {
  return new Diagnostic('warning', `Variable '${name}' is declared but never used`, span)
    .addSuggestion(`Prefix with _ to suppress: _${name}`);
}

function ambiguousType(name, span = null) {
  return new Diagnostic('error', `Ambiguous type for ${name}`, span)
    .addSuggestion('Add a type annotation');
}

// ============================================================
// Levenshtein distance for "did you mean?" suggestions
// ============================================================

function levenshtein(a, b) {
  const dp = Array(a.length + 1).fill(null).map(() => Array(b.length + 1).fill(0));
  for (let i = 0; i <= a.length; i++) dp[i][0] = i;
  for (let j = 0; j <= b.length; j++) dp[0][j] = j;
  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + (a[i - 1] !== b[j - 1] ? 1 : 0)
      );
    }
  }
  return dp[a.length][b.length];
}

function findSimilar(name, candidates, maxDist = 2) {
  return candidates.filter(c => levenshtein(name, c) <= maxDist).sort((a, b) => levenshtein(name, a) - levenshtein(name, b));
}

export {
  Span, Diagnostic,
  typeMismatch, unboundVariable, arityMismatch, infiniteType,
  missingField, unusedVariable, ambiguousType,
  levenshtein, findSimilar
};
