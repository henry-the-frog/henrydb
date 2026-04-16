/**
 * Phantom Types
 * 
 * Type parameters that exist only at compile time — no runtime representation.
 * Used for: unit-safe arithmetic, state tracking, capability tokens.
 * 
 * Example: Distance<Meters> vs Distance<Feet> — same runtime representation,
 * different types that prevent mixing units.
 */

// ============================================================
// Unit-safe arithmetic
// ============================================================

class Quantity {
  constructor(value, _phantom) {
    this.value = value;
    // _phantom is never stored — exists only in the type
  }
  toString() { return `${this.value}`; }
}

// "Phantom" types — just tags, never instantiated
const Meters = Symbol('Meters');
const Feet = Symbol('Feet');
const Seconds = Symbol('Seconds');
const Kilograms = Symbol('Kilograms');

// Smart constructors with type tags
function meters(n) { return { value: n, unit: Meters }; }
function feet(n) { return { value: n, unit: Feet }; }
function seconds(n) { return { value: n, unit: Seconds }; }
function kilograms(n) { return { value: n, unit: Kilograms }; }

// Type-safe operations
function add(a, b) {
  if (a.unit !== b.unit) throw new Error(`Unit mismatch: cannot add ${a.unit.description} and ${b.unit.description}`);
  return { value: a.value + b.value, unit: a.unit };
}

function sub(a, b) {
  if (a.unit !== b.unit) throw new Error(`Unit mismatch: cannot subtract ${a.unit.description} from ${b.unit.description}`);
  return { value: a.value - b.value, unit: a.unit };
}

function scale(scalar, quantity) {
  return { value: scalar * quantity.value, unit: quantity.unit };
}

// Convert between units (explicit conversion required)
function feetToMeters(ft) {
  if (ft.unit !== Feet) throw new Error('Expected Feet');
  return { value: ft.value * 0.3048, unit: Meters };
}

function metersToFeet(m) {
  if (m.unit !== Meters) throw new Error('Expected Meters');
  return { value: m.value / 0.3048, unit: Feet };
}

// ============================================================
// Capability tokens (phantom for authorization)
// ============================================================

const ReadCap = Symbol('Read');
const WriteCap = Symbol('Write');
const AdminCap = Symbol('Admin');

class Token {
  constructor(cap) { this.cap = cap; }
}

function createReadToken() { return new Token(ReadCap); }
function createWriteToken() { return new Token(WriteCap); }
function createAdminToken() { return new Token(AdminCap); }

function requireRead(token) {
  if (token.cap !== ReadCap && token.cap !== AdminCap) throw new Error('Read capability required');
}

function requireWrite(token) {
  if (token.cap !== WriteCap && token.cap !== AdminCap) throw new Error('Write capability required');
}

function requireAdmin(token) {
  if (token.cap !== AdminCap) throw new Error('Admin capability required');
}

// ============================================================
// Tagged values (type-safe IDs)
// ============================================================

class TaggedId {
  constructor(value, tag) { this.value = value; this.tag = tag; }
}

const UserId = Symbol('User');
const PostId = Symbol('Post');
const CommentId = Symbol('Comment');

function userId(n) { return new TaggedId(n, UserId); }
function postId(n) { return new TaggedId(n, PostId); }
function commentId(n) { return new TaggedId(n, CommentId); }

function lookupUser(id) {
  if (id.tag !== UserId) throw new Error('Expected UserId');
  return { id: id.value, name: `User ${id.value}` };
}

function lookupPost(id) {
  if (id.tag !== PostId) throw new Error('Expected PostId');
  return { id: id.value, title: `Post ${id.value}` };
}

export {
  Quantity, Meters, Feet, Seconds, Kilograms,
  meters, feet, seconds, kilograms,
  add, sub, scale, feetToMeters, metersToFeet,
  ReadCap, WriteCap, AdminCap, Token,
  createReadToken, createWriteToken, createAdminToken,
  requireRead, requireWrite, requireAdmin,
  TaggedId, UserId, PostId, CommentId,
  userId, postId, commentId, lookupUser, lookupPost
};
