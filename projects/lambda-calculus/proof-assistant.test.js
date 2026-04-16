import { strict as assert } from 'assert';
import {
  Star, Box, Var, Pi, Lam, App, Nat, Zero, Succ, NatElim,
  Context, infer, check, normalize, betaEq, arrow, parse, resetNames
} from './coc.js';
import { eqType, refl } from './coc-proofs.js';
import { ProofAssistant } from './proof-assistant.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const nat = new Nat();
const star = new Star();
const zero = new Zero();
const one = new Succ(zero);
const two = new Succ(one);

// ============================================================
// Basic proof state management
// ============================================================

test('create proof assistant', () => {
  const pa = new ProofAssistant();
  assert.ok(pa);
});

test('begin theorem creates goal', () => {
  const pa = new ProofAssistant();
  const state = pa.theorem('test', nat);
  assert.ok(state.includes('goal'));
});

test('show displays state', () => {
  const pa = new ProofAssistant();
  pa.theorem('test', nat);
  const shown = pa.show();
  assert.ok(shown);
});

test('qed fails when goals remain', () => {
  const pa = new ProofAssistant();
  pa.theorem('test', nat);
  assert.throws(() => pa.qed(), /incomplete/);
});

// ============================================================
// exact tactic
// ============================================================

test('exact: provide term directly', () => {
  const pa = new ProofAssistant();
  pa.theorem('zero_is_nat', nat);
  pa.tactic('exact', zero);
  const proof = pa.qed();
  assert.equal(proof.name, 'zero_is_nat');
});

test('exact: provide Star', () => {
  const pa = new ProofAssistant(new Context());
  pa.theorem('star_is_box', new Box());
  pa.tactic('exact', star);
  const proof = pa.qed();
  assert.ok(proof);
});

test('exact: wrong type fails', () => {
  const pa = new ProofAssistant();
  pa.theorem('nat_proof', nat);
  assert.throws(() => pa.tactic('exact', star), /type mismatch|must be/);
});

// ============================================================
// intro tactic
// ============================================================

test('intro: Pi type', () => {
  const pa = new ProofAssistant();
  // Goal: ℕ → ℕ
  pa.theorem('nat_to_nat', arrow(nat, nat));
  pa.tactic('intro', 'n');
  // After intro, goal should be ℕ with n:ℕ in context
  pa.tactic('assumption');
  const proof = pa.qed();
  assert.ok(proof);
});

test('intro: dependent Pi', () => {
  const pa = new ProofAssistant();
  // Goal: Π(A:★). A → A
  pa.theorem('poly_id', new Pi('A', star, arrow(new Var('A'), new Var('A'))));
  pa.tactic('intro', 'A');
  pa.tactic('intro', 'x');
  pa.tactic('assumption');
  const proof = pa.qed();
  assert.ok(proof);
});

test('intro fails on non-Pi', () => {
  const pa = new ProofAssistant();
  pa.theorem('test', nat);
  assert.throws(() => pa.tactic('intro', 'x'), /not a Pi/);
});

test('intros: multiple introductions', () => {
  const pa = new ProofAssistant();
  pa.theorem('const_fn', new Pi('A', star, new Pi('B', star, arrow(new Var('A'), arrow(new Var('B'), new Var('A'))))));
  pa.tactic('intros', ['A', 'B', 'x', 'y']);
  // Goal should be A, with x:A in context
  pa.tactic('assumption');
  const proof = pa.qed();
  assert.ok(proof);
});

// ============================================================
// assumption tactic
// ============================================================

test('assumption: finds matching hypothesis', () => {
  const ctx = new Context().extend('h', nat);
  const pa = new ProofAssistant(ctx);
  pa.theorem('use_h', nat);
  pa.tactic('assumption');
  const proof = pa.qed();
  assert.ok(proof);
});

test('assumption: fails when no match', () => {
  const pa = new ProofAssistant();
  pa.theorem('no_match', nat);
  assert.throws(() => pa.tactic('assumption'), /no matching/);
});

// ============================================================
// simpl tactic
// ============================================================

test('simpl: normalizes goal', () => {
  const pa = new ProofAssistant();
  // Goal: (λ(A:★).A) ℕ — should simplify to ℕ
  const goal = new App(new Lam('A', star, new Var('A')), nat);
  pa.theorem('simpl_test', goal);
  pa.tactic('simpl');
  pa.tactic('exact', zero);
  const proof = pa.qed();
  assert.ok(proof);
});

// ============================================================
// refl tactic
// ============================================================

test('refl: proves Eq ℕ 0 0', () => {
  const pa = new ProofAssistant();
  const goal = eqType(nat, zero, zero);
  pa.theorem('zero_eq_zero', goal);
  pa.tactic('refl');
  const proof = pa.qed();
  assert.ok(proof);
});

test('refl: proves Eq ℕ (S 0) (S 0)', () => {
  const pa = new ProofAssistant();
  pa.theorem('one_eq_one', eqType(nat, one, one));
  pa.tactic('refl');
  const proof = pa.qed();
  assert.ok(proof);
});

test('refl: fails on non-equal', () => {
  const pa = new ProofAssistant();
  pa.theorem('zero_eq_one', eqType(nat, zero, one));
  assert.throws(() => pa.tactic('refl'), /not a reflexive equality/);
});

// ============================================================
// trivial tactic
// ============================================================

test('trivial: solves reflexive equality', () => {
  const pa = new ProofAssistant();
  pa.theorem('trivial_eq', eqType(nat, two, two));
  pa.tactic('trivial');
  const proof = pa.qed();
  assert.ok(proof);
});

test('trivial: uses assumption', () => {
  const ctx = new Context().extend('h', nat);
  const pa = new ProofAssistant(ctx);
  pa.theorem('trivial_assump', nat);
  pa.tactic('trivial');
  const proof = pa.qed();
  assert.ok(proof);
});

// ============================================================
// Combined tactics
// ============================================================

test('intro + exact: identity function', () => {
  const pa = new ProofAssistant();
  pa.theorem('id_nat', arrow(nat, nat));
  pa.tactic('intro', 'n');
  pa.tactic('exact', new Var('n'));
  const proof = pa.qed();
  assert.ok(proof);
});

test('intro + intro + assumption: K combinator', () => {
  const pa = new ProofAssistant();
  const kType = arrow(nat, arrow(nat, nat));
  pa.theorem('K', kType);
  pa.tactic('intro', 'x');
  pa.tactic('intro', 'y');
  pa.tactic('assumption'); // should find x:ℕ
  const proof = pa.qed();
  assert.ok(proof);
});

test('multi-step proof: polymorphic identity', () => {
  const pa = new ProofAssistant();
  const idType = new Pi('A', star, arrow(new Var('A'), new Var('A')));
  pa.theorem('id', idType);
  pa.tactic('intro', 'A');
  pa.tactic('intro', 'x');
  pa.tactic('assumption');
  const proof = pa.qed();
  assert.equal(proof.name, 'id');
  assert.equal(proof.tactics.length, 3);
});

// ============================================================
// Theorem definition and reuse
// ============================================================

test('proved theorem is stored', () => {
  const pa = new ProofAssistant();
  pa.theorem('my_thm', nat);
  pa.tactic('exact', zero);
  pa.qed();
  assert.ok(pa.definitions.has('my_thm'));
});

// ============================================================
// Report
// ============================================================

console.log(`\nProof assistant tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
