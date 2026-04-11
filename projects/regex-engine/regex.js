// regex-engine/regex.js — A regex engine from scratch
// Thompson's NFA construction, subset construction (NFA→DFA), Hopcroft minimization
// ─── AST Node Types ───
// { type: 'lit', ch }           — match single character
// { type: 'dot' }               — match any character (except \n)
// { type: 'class', ranges, negated } — character class [abc], [a-z], [^x]
// { type: 'anchor', kind }      — ^ or $
// { type: 'cat', left, right }  — concatenation
// { type: 'alt', left, right }  — alternation (|)
// { type: 'star', child, lazy } — Kleene star (*)
// { type: 'plus', child, lazy } — one or more (+)
// { type: 'opt', child, lazy }  — optional (?)
// { type: 'group', child, index } — capturing group
// { type: 'rep', child, min, max, lazy } — counted repetition {n,m}
// { type: 'empty' }             — epsilon

// ─── Parser ───
// Regex string → AST. Precedence: alt < cat < quantifier < atom
class Parser {
  constructor(src) {
    this.src = src;
    this.pos = 0;
    this.groupCount = 0;
  }

  peek() { return this.pos < this.src.length ? this.src[this.pos] : null; }
  advance() { return this.src[this.pos++]; }
  expect(ch) {
    if (this.peek() !== ch) throw new Error(`Expected '${ch}' at pos ${this.pos}`);
    this.advance();
  }

  parse() {
    const ast = this.parseAlt();
    if (this.pos < this.src.length) throw new Error(`Unexpected '${this.peek()}' at pos ${this.pos}`);
    return ast;
  }

  parseAlt() {
    let left = this.parseCat();
    while (this.peek() === '|') {
      this.advance();
      const right = this.parseCat();
      left = { type: 'alt', left, right };
    }
    return left;
  }

  parseCat() {
    let terms = [];
    while (this.peek() !== null && this.peek() !== '|' && this.peek() !== ')') {
      terms.push(this.parseQuantifier());
    }
    if (terms.length === 0) return { type: 'empty' };
    return terms.reduce((a, b) => ({ type: 'cat', left: a, right: b }));
  }

  parseQuantifier() {
    let child = this.parseAtom();
    while (true) {
      const ch = this.peek();
      if (ch === '*') {
        this.advance();
        const lazy = this.peek() === '?' ? (this.advance(), true) : false;
        child = { type: 'star', child, lazy };
      } else if (ch === '+') {
        this.advance();
        const lazy = this.peek() === '?' ? (this.advance(), true) : false;
        child = { type: 'plus', child, lazy };
      } else if (ch === '?') {
        this.advance();
        const lazy = this.peek() === '?' ? (this.advance(), true) : false;
        child = { type: 'opt', child, lazy };
      } else if (ch === '{') {
        const saved = this.pos;
        this.advance();
        const rep = this.parseRepetition();
        if (rep) {
          const lazy = this.peek() === '?' ? (this.advance(), true) : false;
          child = { type: 'rep', child, min: rep.min, max: rep.max, lazy };
        } else {
          this.pos = saved;
          break;
        }
      } else {
        break;
      }
    }
    return child;
  }

  parseRepetition() {
    // Already consumed '{'. Parse n,m or n or n,
    let numStr = '';
    while (this.peek() && /\d/.test(this.peek())) numStr += this.advance();
    if (numStr === '') return null; // not a repetition
    const min = parseInt(numStr, 10);
    if (this.peek() === '}') {
      this.advance();
      return { min, max: min };
    }
    if (this.peek() !== ',') return null;
    this.advance(); // consume ','
    let maxStr = '';
    while (this.peek() && /\d/.test(this.peek())) maxStr += this.advance();
    if (this.peek() !== '}') return null;
    this.advance();
    const max = maxStr === '' ? Infinity : parseInt(maxStr, 10);
    return { min, max };
  }

  parseAtom() {
    const ch = this.peek();
    if (ch === '(') {
      this.advance();
      // Check for non-capturing group (?:...)
      if (this.peek() === '?' && this.pos + 1 < this.src.length && this.src[this.pos + 1] === ':') {
        this.advance(); // consume '?'
        this.advance(); // consume ':'
        const child = this.parseAlt();
        this.expect(')');
        return child; // non-capturing: just return the inner expression
      }
      const index = ++this.groupCount;
      const child = this.parseAlt();
      this.expect(')');
      return { type: 'group', child, index };
    }
    if (ch === '[') return this.parseClass();
    if (ch === '.') { this.advance(); return { type: 'dot' }; }
    if (ch === '^') { this.advance(); return { type: 'anchor', kind: 'start' }; }
    if (ch === '$') { this.advance(); return { type: 'anchor', kind: 'end' }; }
    if (ch === '\\') return this.parseEscape();
    if (ch === null || ch === ')' || ch === '|') throw new Error(`Unexpected end or '${ch}' at pos ${this.pos}`);
    // Literal
    this.advance();
    return { type: 'lit', ch };
  }

  parseEscape() {
    this.advance(); // consume '\'
    const ch = this.advance();
    if (!ch) throw new Error('Unexpected end after \\');
    // Shorthand classes
    const shorthands = {
      'd': { ranges: [['0', '9']], negated: false },
      'D': { ranges: [['0', '9']], negated: true },
      'w': { ranges: [['a', 'z'], ['A', 'Z'], ['0', '9'], ['_', '_']], negated: false },
      'W': { ranges: [['a', 'z'], ['A', 'Z'], ['0', '9'], ['_', '_']], negated: true },
      's': { ranges: [[' ', ' '], ['\t', '\t'], ['\n', '\n'], ['\r', '\r'], ['\f', '\f'], ['\v', '\v']], negated: false },
      'S': { ranges: [[' ', ' '], ['\t', '\t'], ['\n', '\n'], ['\r', '\r'], ['\f', '\f'], ['\v', '\v']], negated: true },
    };
    if (shorthands[ch]) return { type: 'class', ...shorthands[ch] };
    // Special escapes
    const specials = { 'n': '\n', 't': '\t', 'r': '\r', 'f': '\f', 'v': '\v', '0': '\0' };
    if (specials[ch]) return { type: 'lit', ch: specials[ch] };
    // Literal escape (., *, +, etc.)
    return { type: 'lit', ch };
  }

  parseClass() {
    this.advance(); // consume '['
    const negated = this.peek() === '^' ? (this.advance(), true) : false;
    const ranges = [];
    // Allow ] as first character
    if (this.peek() === ']') {
      ranges.push([']', ']']);
      this.advance();
    }
    // Allow - as first character (literal dash)
    if (this.peek() === '-') {
      ranges.push(['-', '-']);
      this.advance();
    }
    while (this.peek() !== null && this.peek() !== ']') {
      let ch = this.peek();
      if (ch === '\\') {
        this.advance();
        ch = this.advance();
        const specials = { 'n': '\n', 't': '\t', 'r': '\r' };
        ch = specials[ch] || ch;
        // Check for shorthand class inside character class
        const sc = { 'd': [['0', '9']], 'w': [['a', 'z'], ['A', 'Z'], ['0', '9'], ['_', '_']], 's': [[' ', ' '], ['\t', '\t'], ['\n', '\n'], ['\r', '\r']] };
        if (sc[ch]) { ranges.push(...sc[ch]); continue; }
      } else {
        this.advance();
      }
      // Check for range
      if (this.peek() === '-' && this.pos + 1 < this.src.length && this.src[this.pos + 1] !== ']') {
        this.advance(); // consume '-'
        let end = this.peek();
        if (end === '\\') { this.advance(); end = this.advance(); const sp = { 'n': '\n', 't': '\t', 'r': '\r' }; end = sp[end] || end; }
        else this.advance();
        ranges.push([ch, end]);
      } else {
        ranges.push([ch, ch]);
      }
    }
    this.expect(']');
    return { type: 'class', ranges, negated };
  }
}

function parse(pattern) {
  return new Parser(pattern).parse();
}

// ─── NFA ───
// Thompson's construction: each fragment has a start state and a list of dangling arrows.
// States: { id, transitions: [{ on: char|null(epsilon)|fn, to: stateId }], accepting: bool }

let stateId = 0;
function newState(accepting = false) {
  return { id: stateId++, transitions: [], accepting };
}

function resetStateId() { stateId = 0; }

// Build NFA from AST (Thompson's construction)
function astToNfa(ast) {
  resetStateId();
  const { start, end } = buildFragment(ast);
  end.accepting = true;
  return start;
}

function buildFragment(node) {
  switch (node.type) {
    case 'empty': {
      const s = newState();
      return { start: s, end: s };
    }
    case 'lit': {
      const s = newState();
      const e = newState();
      s.transitions.push({ on: node.ch, to: e });
      return { start: s, end: e };
    }
    case 'dot': {
      const s = newState();
      const e = newState();
      s.transitions.push({ on: 'DOT', to: e });
      return { start: s, end: e };
    }
    case 'class': {
      const s = newState();
      const e = newState();
      s.transitions.push({ on: { type: 'class', ranges: node.ranges, negated: node.negated }, to: e });
      return { start: s, end: e };
    }
    case 'anchor': {
      const s = newState();
      const e = newState();
      s.transitions.push({ on: { type: 'anchor', kind: node.kind }, to: e });
      return { start: s, end: e };
    }
    case 'cat': {
      const left = buildFragment(node.left);
      const right = buildFragment(node.right);
      // Connect left end to right start via epsilon
      left.end.transitions.push({ on: null, to: right.start });
      return { start: left.start, end: right.end };
    }
    case 'alt': {
      const s = newState();
      const e = newState();
      const left = buildFragment(node.left);
      const right = buildFragment(node.right);
      s.transitions.push({ on: null, to: left.start });
      s.transitions.push({ on: null, to: right.start });
      left.end.transitions.push({ on: null, to: e });
      right.end.transitions.push({ on: null, to: e });
      return { start: s, end: e };
    }
    case 'star': {
      const s = newState();
      const e = newState();
      const body = buildFragment(node.child);
      s.transitions.push({ on: null, to: body.start });
      s.transitions.push({ on: null, to: e });
      body.end.transitions.push({ on: null, to: body.start });
      body.end.transitions.push({ on: null, to: e });
      return { start: s, end: e };
    }
    case 'plus': {
      const s = newState();
      const e = newState();
      const body = buildFragment(node.child);
      s.transitions.push({ on: null, to: body.start });
      body.end.transitions.push({ on: null, to: body.start });
      body.end.transitions.push({ on: null, to: e });
      return { start: s, end: e };
    }
    case 'opt': {
      const s = newState();
      const e = newState();
      const body = buildFragment(node.child);
      s.transitions.push({ on: null, to: body.start });
      s.transitions.push({ on: null, to: e });
      body.end.transitions.push({ on: null, to: e });
      return { start: s, end: e };
    }
    case 'group': {
      // Add group open/close epsilon transitions for capture tracking
      const sOpen = newState();
      const sClose = newState();
      const body = buildFragment(node.child);
      sOpen.transitions.push({ on: { type: 'groupOpen', index: node.index }, to: body.start });
      body.end.transitions.push({ on: { type: 'groupClose', index: node.index }, to: sClose });
      return { start: sOpen, end: sClose };
    }
    case 'rep': {
      // {min,max}: unroll to min concatenations + (max-min) optionals
      let fragments = [];
      for (let i = 0; i < node.min; i++) {
        fragments.push(buildFragment(node.child));
      }
      if (node.max === Infinity) {
        // min copies + star
        const starNode = { type: 'star', child: node.child, lazy: node.lazy };
        fragments.push(buildFragment(starNode));
      } else {
        for (let i = node.min; i < node.max; i++) {
          const optNode = { type: 'opt', child: node.child, lazy: node.lazy };
          fragments.push(buildFragment(optNode));
        }
      }
      if (fragments.length === 0) {
        const s = newState();
        return { start: s, end: s };
      }
      // Chain all fragments
      let result = fragments[0];
      for (let i = 1; i < fragments.length; i++) {
        result.end.transitions.push({ on: null, to: fragments[i].start });
        result = { start: result.start, end: fragments[i].end };
      }
      return result;
    }
    default:
      throw new Error(`Unknown AST node type: ${node.type}`);
  }
}

// ─── NFA Simulation ───
// Multi-state simulation with epsilon closure

function matchesTransition(trans, ch, pos, inputLen) {
  if (trans === null) return false; // epsilon, handled separately
  if (typeof trans === 'string') {
    if (trans === 'DOT') return ch !== undefined && ch !== '\n';
    return ch === trans;
  }
  if (trans.type === 'class') {
    if (ch === undefined) return false;
    const code = ch.charCodeAt(0);
    let inRange = false;
    for (const [lo, hi] of trans.ranges) {
      if (code >= lo.charCodeAt(0) && code <= hi.charCodeAt(0)) { inRange = true; break; }
    }
    return trans.negated ? !inRange : inRange;
  }
  if (trans.type === 'anchor') {
    // Anchors don't consume characters — they're zero-width
    return false; // handled specially
  }
  return false;
}

function epsilonClosure(states) {
  const stack = [...states];
  const closure = new Set(states.map(s => s.id));
  const result = [];
  while (stack.length > 0) {
    const state = stack.pop();
    result.push(state);
    for (const t of state.transitions) {
      if (t.on === null && !closure.has(t.to.id)) {
        closure.add(t.to.id);
        stack.push(t.to);
      }
      // Group open/close markers are epsilon-like (don't consume input)
      if (t.on && (t.on.type === 'groupOpen' || t.on.type === 'groupClose') && !closure.has(t.to.id)) {
        closure.add(t.to.id);
        stack.push(t.to);
      }
    }
  }
  return result;
}

function anchorClosure(states, pos, inputLen, input) {
  // Follow anchor transitions that match current position
  const stack = [...states];
  const visited = new Set(states.map(s => s.id));
  const result = [...states];
  while (stack.length > 0) {
    const state = stack.pop();
    for (const t of state.transitions) {
      if (t.on && t.on.type === 'anchor' && !visited.has(t.to.id)) {
        let match = false;
        if (t.on.kind === 'start') match = pos === 0;
        else if (t.on.kind === 'end') match = pos === inputLen;
        if (match) {
          visited.add(t.to.id);
          result.push(t.to);
          stack.push(t.to);
        }
      }
    }
  }
  return epsilonClosure(result);
}

function nfaMatch(startState, input) {
  let current = epsilonClosure([startState]);
  current = anchorClosure(current, 0, input.length, input);

  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    const next = [];
    const seen = new Set();
    for (const state of current) {
      for (const t of state.transitions) {
        if (matchesTransition(t.on, ch, i, input.length) && !seen.has(t.to.id)) {
          seen.add(t.to.id);
          next.push(t.to);
        }
      }
    }
    current = epsilonClosure(next);
    current = anchorClosure(current, i + 1, input.length, input);
  }
  return current.some(s => s.accepting);
}

// NFA match with anchor position context (for search in larger strings)
function nfaMatchAnchored(startState, input, startPos, totalLen) {
  function anchorClosureCtx(states, localPos) {
    const pos = startPos + localPos;
    const stack = [...states];
    const visited = new Set(states.map(s => s.id));
    const result = [...states];
    while (stack.length > 0) {
      const state = stack.pop();
      for (const t of state.transitions) {
        if (t.on && t.on.type === 'anchor' && !visited.has(t.to.id)) {
          let match = false;
          if (t.on.kind === 'start') match = pos === 0;
          else if (t.on.kind === 'end') match = pos === totalLen;
          if (match) {
            visited.add(t.to.id);
            result.push(t.to);
            stack.push(t.to);
          }
        }
      }
    }
    return epsilonClosure(result);
  }

  let current = epsilonClosure([startState]);
  current = anchorClosureCtx(current, 0);

  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    const next = [];
    const seen = new Set();
    for (const state of current) {
      for (const t of state.transitions) {
        if (matchesTransition(t.on, ch, i, input.length) && !seen.has(t.to.id)) {
          seen.add(t.to.id);
          next.push(t.to);
        }
      }
    }
    current = epsilonClosure(next);
    current = anchorClosureCtx(current, i + 1);
  }
  return current.some(s => s.accepting);
}

// ─── NFA Match with Capture Groups ───
// Each "thread" tracks its own capture state
function nfaMatchCaptures(startState, input, numGroups) {
  // Thread: { state, captures: [[start, end], ...] }
  function makeThread(state, captures) {
    return { state, captures: captures.map(c => c ? [...c] : null) };
  }

  function epsilonClosureCaptures(threads, pos) {
    const stack = [...threads];
    const visited = new Set();
    const result = [];
    while (stack.length > 0) {
      const thread = stack.pop();
      const key = thread.state.id;
      if (visited.has(key)) continue;
      visited.add(key);
      result.push(thread);
      for (const t of thread.state.transitions) {
        if (t.on === null && !visited.has(t.to.id)) {
          stack.push(makeThread(t.to, thread.captures));
        }
        if (t.on && t.on.type === 'groupOpen' && !visited.has(t.to.id)) {
          const caps = thread.captures.map(c => c ? [...c] : null);
          caps[t.on.index - 1] = [pos, -1];
          stack.push({ state: t.to, captures: caps });
        }
        if (t.on && t.on.type === 'groupClose' && !visited.has(t.to.id)) {
          const caps = thread.captures.map(c => c ? [...c] : null);
          if (caps[t.on.index - 1]) caps[t.on.index - 1][1] = pos;
          stack.push({ state: t.to, captures: caps });
        }
        if (t.on && t.on.type === 'anchor' && !visited.has(t.to.id)) {
          let match = false;
          if (t.on.kind === 'start') match = pos === 0;
          else if (t.on.kind === 'end') match = pos === input.length;
          if (match) stack.push(makeThread(t.to, thread.captures));
        }
      }
    }
    return result;
  }

  const initCaptures = new Array(numGroups).fill(null);
  let current = epsilonClosureCaptures([makeThread(startState, initCaptures)], 0);

  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    const next = [];
    const seen = new Set();
    for (const thread of current) {
      for (const t of thread.state.transitions) {
        if (matchesTransition(t.on, ch, i, input.length) && !seen.has(t.to.id)) {
          seen.add(t.to.id);
          next.push(makeThread(t.to, thread.captures));
        }
      }
    }
    current = epsilonClosureCaptures(next, i + 1);
  }

  // Find accepting thread
  for (const thread of current) {
    if (thread.state.accepting) {
      return thread.captures.map((c, i) => {
        if (!c || c[1] === -1) return undefined;
        return input.slice(c[0], c[1]);
      });
    }
  }
  return null;
}


function nfaToDfa(nfaStart) {
  const startClosure = epsilonClosure([nfaStart]);
  const startKey = stateSetKey(startClosure);

  const dfaStates = new Map(); // key → { id, nfaStates, transitions: [{on, to}], accepting }
  let dfaId = 0;

  const startDfa = {
    id: dfaId++,
    nfaStates: startClosure,
    transitions: [],
    accepting: startClosure.some(s => s.accepting),
  };
  dfaStates.set(startKey, startDfa);

  const worklist = [{ key: startKey, dfa: startDfa }];

  while (worklist.length > 0) {
    const { dfa } = worklist.pop();
    // Collect all possible transitions from this set of NFA states
    // Group by transition key for merging equivalent transitions
    const transMap = new Map(); // serialized key → { on, states[] }

    for (const nfaState of dfa.nfaStates) {
      for (const t of nfaState.transitions) {
        if (t.on === null) continue; // skip epsilon
        if (t.on && t.on.type === 'anchor') continue; // skip anchors for DFA
        const key = transKey(t.on);
        if (!transMap.has(key)) transMap.set(key, { on: t.on, states: [] });
        transMap.get(key).states.push(t.to);
      }
    }

    for (const [, { on, states }] of transMap) {
      const closure = epsilonClosure(states);
      const closureKey = stateSetKey(closure);

      if (!dfaStates.has(closureKey)) {
        const newDfa = {
          id: dfaId++,
          nfaStates: closure,
          transitions: [],
          accepting: closure.some(s => s.accepting),
        };
        dfaStates.set(closureKey, newDfa);
        worklist.push({ key: closureKey, dfa: newDfa });
      }
      dfa.transitions.push({ on, to: dfaStates.get(closureKey) });
    }
  }

  return { start: startDfa, states: [...dfaStates.values()] };
}

function stateSetKey(states) {
  return states.map(s => s.id).sort((a, b) => a - b).join(',');
}

function transKey(on) {
  if (typeof on === 'string') return on === 'DOT' ? 'DOT' : `lit:${on}`;
  if (on.type === 'class') {
    // Use charCode to avoid ambiguity with special chars like '-'
    return `class:${on.negated ? '^' : ''}${on.ranges.map(([a, b]) => `${a.charCodeAt(0)}:${b.charCodeAt(0)}`).join(',')}`;
  }
  return JSON.stringify(on);
}

// DFA simulation
function dfaMatch(dfaStart, input) {
  let current = dfaStart;
  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    // Check all matching transitions (DOT can overlap with literals/classes)
    let matchedTransitions = [];
    for (const { on, to } of current.transitions) {
      if (matchesTransition(on, ch, i, input.length)) {
        matchedTransitions.push(to);
      }
    }
    if (matchedTransitions.length === 0) return false;
    if (matchedTransitions.length === 1) {
      current = matchedTransitions[0];
    } else {
      // Multiple transitions match (DOT + literal overlap) — this shouldn't happen
      // in a correct DFA, but we handle it by checking all paths
      // Take the first match (prefer specifics over DOT by ordering)
      current = matchedTransitions[0];
    }
  }
  return current.accepting;
}

// ─── Hopcroft DFA Minimization ───
function minimizeDfa(dfa) {
  const { states } = dfa;
  if (states.length <= 1) return dfa;

  // Initial partition: accepting vs non-accepting
  const accepting = states.filter(s => s.accepting);
  const nonAccepting = states.filter(s => !s.accepting);
  let partitions = [];
  if (accepting.length > 0) partitions.push(accepting);
  if (nonAccepting.length > 0) partitions.push(nonAccepting);

  // Collect all transition keys (for grouping equivalent transitions)
  const allTKeys = new Set();
  for (const s of states) {
    for (const { on } of s.transitions) allTKeys.add(transKey(on));
  }

  // Refine partitions
  let changed = true;
  while (changed) {
    changed = false;
    const newPartitions = [];
    for (const partition of partitions) {
      if (partition.length <= 1) { newPartitions.push(partition); continue; }

      let currentParts = [partition];
      for (const tKey of allTKeys) {
        const nextParts = [];
        for (const part of currentParts) {
          const groups = new Map();
          for (const s of part) {
            const trans = s.transitions.find(t => transKey(t.on) === tKey);
            const target = trans ? trans.to : null;
            const targetPart = target ? findPartition(partitions, target) : -1;
            if (!groups.has(targetPart)) groups.set(targetPart, []);
            groups.get(targetPart).push(s);
          }
          for (const g of groups.values()) nextParts.push(g);
        }
        currentParts = nextParts;
      }
      if (currentParts.length > 1) changed = true;
      newPartitions.push(...currentParts);
    }
    partitions = newPartitions;
  }

  // Build minimized DFA
  const stateToPartition = new Map();
  const partitionStates = [];
  let minId = 0;
  for (const partition of partitions) {
    const rep = partition[0];
    const minState = {
      id: minId++,
      transitions: [],
      accepting: rep.accepting,
      nfaStates: rep.nfaStates,
    };
    partitionStates.push({ partition, state: minState });
    for (const s of partition) stateToPartition.set(s.id, minState);
  }

  // Wire transitions
  for (const { partition, state } of partitionStates) {
    const rep = partition[0];
    for (const { on, to } of rep.transitions) {
      state.transitions.push({ on, to: stateToPartition.get(to.id) });
    }
  }

  const minStart = stateToPartition.get(dfa.start.id);
  return { start: minStart, states: partitionStates.map(p => p.state) };
}

function findPartition(partitions, state) {
  for (let i = 0; i < partitions.length; i++) {
    if (partitions[i].some(s => s.id === state.id)) return i;
  }
  return -1;
}

// ─── Public API ───
class Regex {
  constructor(pattern) {
    this.pattern = pattern;
    const parser = new Parser(pattern);
    this.ast = parser.parse();
    this.groupCount = parser.groupCount;
    this._nfa = astToNfa(this.ast);
    this._hasAnchors = pattern.includes('^') || pattern.includes('$');
    this._hasStartAnchor = pattern.startsWith('^') || /(?<!\\)\^/.test(pattern);
    this._hasEndAnchor = pattern.endsWith('$');
    this._hasDot = pattern.includes('.');
    this._hasCharClass = pattern.includes('[');
    // Build DFA only for simple patterns (no anchors, dots, or char classes that may overlap)
    if (!this._hasAnchors && !this._hasDot && !this._hasCharClass) {
      const dfa = nfaToDfa(this._nfa);
      this._dfa = minimizeDfa(dfa);
    }
  }

  // Test substring at specific position within larger string
  _testSubstring(sub, startPos, totalLen) {
    // For anchored patterns, simulate NFA with position context
    if (this._hasAnchors) {
      return nfaMatchAnchored(this._nfa, sub, startPos, totalLen);
    }
    if (this._dfa) {
      return dfaMatch(this._dfa.start, sub);
    }
    return nfaMatch(this._nfa, sub);
  }

  // Full match with capture groups (returns array of group matches, or null)
  match(input) {
    if (this.groupCount === 0) {
      return this.test(input) ? [] : null;
    }
    return nfaMatchCaptures(this._nfa, input, this.groupCount);
  }

  // Full match (entire string must match)
  test(input) {
    if (this._dfa && !this._hasAnchors) {
      return dfaMatch(this._dfa.start, input);
    }
    return nfaMatch(this._nfa, input);
  }

  // Search: find first match in string (returns { match, index } or null)
  search(input) {
    // Try each starting position
    for (let i = 0; i <= input.length; i++) {
      // For ^-anchored patterns, only try from position 0
      if (this._hasStartAnchor && i > 0) break;
      // Try each ending position (greedy: longest first)
      for (let j = input.length; j >= i; j--) {
        const sub = input.slice(i, j);
        if (this._testSubstring(sub, i, input.length)) {
          return { match: sub, index: i };
        }
      }
    }
    return null;
  }

  // Find all non-overlapping matches
  findAll(input) {
    const results = [];
    let pos = 0;
    while (pos < input.length) {
      let found = false;
      for (let end = input.length; end > pos; end--) {
        if (this._testSubstring(input.slice(pos, end), pos, input.length)) {
          results.push({ match: input.slice(pos, end), index: pos });
          pos = end;
          found = true;
          break;
        }
      }
      if (!found) pos++;
    }
    return results;
  }

  // Replace first or all occurrences
  replace(input, replacement, all = false) {
    if (!all) {
      const m = this.search(input);
      if (!m) return input;
      return input.slice(0, m.index) + replacement + input.slice(m.index + m.match.length);
    }
    const matches = this.findAll(input);
    if (matches.length === 0) return input;
    let result = '';
    let pos = 0;
    for (const m of matches) {
      result += input.slice(pos, m.index) + replacement;
      pos = m.index + m.match.length;
    }
    result += input.slice(pos);
    return result;
  }
}

export { Regex, Parser, parse, astToNfa, nfaToDfa, minimizeDfa, nfaMatch, dfaMatch,
         newState, resetStateId, epsilonClosure, nfaMatchCaptures, nfaMatchAnchored };
