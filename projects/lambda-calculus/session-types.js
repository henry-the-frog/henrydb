/**
 * Session Types
 * 
 * Types for communication protocols, ensuring both sides of a channel
 * follow a compatible protocol.
 * 
 * Based on Honda-Yoshida-Carbone session types.
 * 
 * A session type describes the protocol of a channel:
 *   !T.S   — Send value of type T, then continue with S
 *   ?T.S   — Receive value of type T, then continue with S
 *   ⊕{l:S} — Internal choice: select a label
 *   &{l:S} — External choice: offer labels
 *   end    — Protocol complete
 *   μX.S   — Recursive protocol
 *   X      — Protocol variable (for recursion)
 * 
 * Key property: DUALITY
 *   dual(!T.S) = ?T.dual(S)
 *   dual(?T.S) = !T.dual(S)
 *   dual(⊕{l:S}) = &{l:dual(S)}
 *   dual(&{l:S}) = ⊕{l:dual(S)}
 *   dual(end) = end
 */

// ============================================================
// Session Type AST
// ============================================================

class Send {
  constructor(type, cont) { this.tag = 'Send'; this.type = type; this.cont = cont; }
  toString() { return `!${this.type}.${this.cont}`; }
}

class Recv {
  constructor(type, cont) { this.tag = 'Recv'; this.type = type; this.cont = cont; }
  toString() { return `?${this.type}.${this.cont}`; }
}

class Select {
  // Internal choice: we choose which branch
  constructor(branches) { this.tag = 'Select'; this.branches = branches; } // Map<label, SessionType>
  toString() { 
    const bs = [...this.branches].map(([l, s]) => `${l}: ${s}`).join(', ');
    return `⊕{${bs}}`;
  }
}

class Offer {
  // External choice: peer chooses which branch
  constructor(branches) { this.tag = 'Offer'; this.branches = branches; }
  toString() {
    const bs = [...this.branches].map(([l, s]) => `${l}: ${s}`).join(', ');
    return `&{${bs}}`;
  }
}

class End {
  constructor() { this.tag = 'End'; }
  toString() { return 'end'; }
}

class RecVar {
  constructor(name) { this.tag = 'RecVar'; this.name = name; }
  toString() { return this.name; }
}

class Rec {
  constructor(name, body) { this.tag = 'Rec'; this.name = name; this.body = body; }
  toString() { return `μ${this.name}.${this.body}`; }
}

// ============================================================
// Duality
// ============================================================

function dual(session) {
  switch (session.tag) {
    case 'Send': return new Recv(session.type, dual(session.cont));
    case 'Recv': return new Send(session.type, dual(session.cont));
    case 'Select': {
      const branches = new Map();
      for (const [l, s] of session.branches) branches.set(l, dual(s));
      return new Offer(branches);
    }
    case 'Offer': {
      const branches = new Map();
      for (const [l, s] of session.branches) branches.set(l, dual(s));
      return new Select(branches);
    }
    case 'End': return new End();
    case 'RecVar': return new RecVar(session.name);
    case 'Rec': return new Rec(session.name, dual(session.body));
  }
}

// ============================================================
// Session Type Equality (up to alpha-equivalence)
// ============================================================

function sessionEquals(a, b) {
  if (a.tag !== b.tag) return false;
  switch (a.tag) {
    case 'Send': case 'Recv':
      return a.type === b.type && sessionEquals(a.cont, b.cont);
    case 'Select': case 'Offer': {
      if (a.branches.size !== b.branches.size) return false;
      for (const [l, s] of a.branches) {
        if (!b.branches.has(l)) return false;
        if (!sessionEquals(s, b.branches.get(l))) return false;
      }
      return true;
    }
    case 'End': return true;
    case 'RecVar': return a.name === b.name;
    case 'Rec': return a.name === b.name && sessionEquals(a.body, b.body);
  }
}

// ============================================================
// Protocol Compliance Checker
// ============================================================

class ProtocolError extends Error {
  constructor(msg) { super(msg); this.name = 'ProtocolError'; }
}

// An action performed on a channel
class SendAction { constructor(type, value) { this.tag = 'SendAction'; this.type = type; this.value = value; } }
class RecvAction { constructor(type) { this.tag = 'RecvAction'; this.type = type; } }
class SelectAction { constructor(label) { this.tag = 'SelectAction'; this.label = label; } }
class OfferAction { constructor(labels) { this.tag = 'OfferAction'; this.labels = labels; } }
class CloseAction { constructor() { this.tag = 'CloseAction'; } }

/**
 * Check if a sequence of actions complies with a session type.
 * @param {Array} actions - sequence of channel actions
 * @param {SessionType} protocol - expected protocol
 * @returns {object} { valid, errors, remaining }
 */
function checkCompliance(actions, protocol) {
  const errors = [];
  let idx = 0;
  let current = protocol;
  const recEnv = new Map(); // for unfolding recursive protocols
  
  function unfold(s) {
    if (s.tag === 'Rec') {
      recEnv.set(s.name, s);
      return unfold(s.body);
    }
    if (s.tag === 'RecVar') {
      const rec = recEnv.get(s.name);
      if (rec) return unfold(rec.body);
      errors.push(`Unbound recursion variable: ${s.name}`);
      return new End();
    }
    return s;
  }
  
  while (idx < actions.length) {
    current = unfold(current);
    const action = actions[idx];
    
    switch (action.tag) {
      case 'SendAction':
        if (current.tag !== 'Send') {
          errors.push(`Step ${idx}: expected ${current.tag}, got Send`);
          return { valid: false, errors, remaining: current };
        }
        if (action.type !== current.type) {
          errors.push(`Step ${idx}: sent ${action.type}, expected ${current.type}`);
        }
        current = current.cont;
        break;
        
      case 'RecvAction':
        if (current.tag !== 'Recv') {
          errors.push(`Step ${idx}: expected ${current.tag}, got Recv`);
          return { valid: false, errors, remaining: current };
        }
        if (action.type !== current.type) {
          errors.push(`Step ${idx}: received ${action.type}, expected ${current.type}`);
        }
        current = current.cont;
        break;
        
      case 'SelectAction':
        if (current.tag !== 'Select') {
          errors.push(`Step ${idx}: expected ${current.tag}, got Select`);
          return { valid: false, errors, remaining: current };
        }
        if (!current.branches.has(action.label)) {
          errors.push(`Step ${idx}: selected '${action.label}', not in {${[...current.branches.keys()].join(', ')}}`);
          return { valid: false, errors, remaining: current };
        }
        current = current.branches.get(action.label);
        break;
        
      case 'OfferAction':
        if (current.tag !== 'Offer') {
          errors.push(`Step ${idx}: expected ${current.tag}, got Offer`);
          return { valid: false, errors, remaining: current };
        }
        // Check that all offered labels are present
        for (const l of action.labels) {
          if (!current.branches.has(l)) {
            errors.push(`Step ${idx}: offered label '${l}' not in protocol`);
          }
        }
        // For now, take first label (in real system, this would be nondeterministic)
        current = current.branches.get(action.labels[0]);
        break;
        
      case 'CloseAction':
        current = unfold(current);
        if (current.tag !== 'End') {
          errors.push(`Step ${idx}: closed channel but protocol expects ${current.tag}`);
        }
        idx++;
        continue;
    }
    
    idx++;
  }
  
  // Check that protocol is complete
  current = unfold(current);
  if (current.tag !== 'End' && errors.length === 0) {
    errors.push(`Protocol incomplete: expected more actions (${current.tag} remaining)`);
  }
  
  return {
    valid: errors.length === 0,
    errors,
    remaining: current,
  };
}

// ============================================================
// Example Protocols
// ============================================================

// Simple request-response: !Request.?Response.end
function requestResponse(reqType, resType) {
  return new Send(reqType, new Recv(resType, new End()));
}

// Calculator: choose operation, send args, receive result
function calculator() {
  return new Select(new Map([
    ['add', new Send('Int', new Send('Int', new Recv('Int', new End())))],
    ['mul', new Send('Int', new Send('Int', new Recv('Int', new End())))],
    ['neg', new Send('Int', new Recv('Int', new End()))],
  ]));
}

// Recursive counter: send 'inc' to increment, 'get' to read, 'done' to stop
function counter() {
  return new Rec('X', new Select(new Map([
    ['inc', new RecVar('X')],
    ['get', new Recv('Int', new RecVar('X'))],
    ['done', new End()],
  ])));
}

// ATM protocol: authenticate, then operations
function atm() {
  return new Send('Card', new Send('PIN', 
    new Select(new Map([
      ['withdraw', new Send('Int', new Recv('Bool', new End()))],
      ['balance', new Recv('Int', new End())],
      ['quit', new End()],
    ]))));
}

// ============================================================
// Exports
// ============================================================

export {
  Send, Recv, Select, Offer, End, RecVar, Rec,
  dual, sessionEquals,
  SendAction, RecvAction, SelectAction, OfferAction, CloseAction,
  checkCompliance, ProtocolError,
  requestResponse, calculator, counter, atm
};
