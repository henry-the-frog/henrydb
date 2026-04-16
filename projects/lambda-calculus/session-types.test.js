import { strict as assert } from 'assert';
import {
  Send, Recv, Select, Offer, End, RecVar, Rec,
  dual, sessionEquals,
  SendAction, RecvAction, SelectAction, OfferAction, CloseAction,
  checkCompliance,
  requestResponse, calculator, counter, atm
} from './session-types.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const end = new End();

// ============================================================
// Duality
// ============================================================

test('dual of Send is Recv', () => {
  const s = new Send('Int', end);
  const d = dual(s);
  assert.equal(d.tag, 'Recv');
  assert.equal(d.type, 'Int');
  assert.equal(d.cont.tag, 'End');
});

test('dual of Recv is Send', () => {
  const s = new Recv('String', end);
  const d = dual(s);
  assert.equal(d.tag, 'Send');
  assert.equal(d.type, 'String');
});

test('dual of Select is Offer', () => {
  const s = new Select(new Map([['a', end], ['b', end]]));
  const d = dual(s);
  assert.equal(d.tag, 'Offer');
  assert.ok(d.branches.has('a'));
  assert.ok(d.branches.has('b'));
});

test('dual of Offer is Select', () => {
  const s = new Offer(new Map([['x', end]]));
  const d = dual(s);
  assert.equal(d.tag, 'Select');
});

test('dual of End is End', () => {
  assert.equal(dual(end).tag, 'End');
});

test('dual is involutive: dual(dual(S)) = S', () => {
  const s = new Send('Int', new Recv('Bool', end));
  assert.ok(sessionEquals(dual(dual(s)), s));
});

test('dual of complex protocol', () => {
  const proto = requestResponse('Request', 'Response');
  const d = dual(proto);
  // dual(!Req.?Res.end) = ?Req.!Res.end
  assert.equal(d.tag, 'Recv');
  assert.equal(d.type, 'Request');
  assert.equal(d.cont.tag, 'Send');
  assert.equal(d.cont.type, 'Response');
});

// ============================================================
// Session Equality
// ============================================================

test('equal sessions', () => {
  const a = new Send('Int', end);
  const b = new Send('Int', end);
  assert.ok(sessionEquals(a, b));
});

test('unequal sessions: different type', () => {
  const a = new Send('Int', end);
  const b = new Send('Bool', end);
  assert.ok(!sessionEquals(a, b));
});

test('unequal sessions: different structure', () => {
  const a = new Send('Int', end);
  const b = new Recv('Int', end);
  assert.ok(!sessionEquals(a, b));
});

// ============================================================
// Protocol Compliance
// ============================================================

test('request-response: correct sequence', () => {
  const proto = requestResponse('Query', 'Result');
  const actions = [
    new SendAction('Query', 'SELECT * FROM users'),
    new RecvAction('Result'),
    new CloseAction(),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(result.valid, `Expected valid: ${result.errors.join(', ')}`);
});

test('request-response: missing response', () => {
  const proto = requestResponse('Query', 'Result');
  const actions = [
    new SendAction('Query', 'SELECT *'),
    new CloseAction(),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(!result.valid);
});

test('request-response: wrong send type', () => {
  const proto = requestResponse('Query', 'Result');
  const actions = [
    new SendAction('Int', 42),
    new RecvAction('Result'),
    new CloseAction(),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(result.errors.some(e => e.includes('sent Int, expected Query')));
});

test('request-response: recv when should send', () => {
  const proto = requestResponse('Query', 'Result');
  const actions = [
    new RecvAction('Query'),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(!result.valid);
});

// ============================================================
// Calculator protocol
// ============================================================

test('calculator: add 3 + 4', () => {
  const proto = calculator();
  const actions = [
    new SelectAction('add'),
    new SendAction('Int', 3),
    new SendAction('Int', 4),
    new RecvAction('Int'),
    new CloseAction(),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(result.valid, result.errors.join(', '));
});

test('calculator: neg 5', () => {
  const proto = calculator();
  const actions = [
    new SelectAction('neg'),
    new SendAction('Int', 5),
    new RecvAction('Int'),
    new CloseAction(),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(result.valid, result.errors.join(', '));
});

test('calculator: invalid label', () => {
  const proto = calculator();
  const actions = [
    new SelectAction('sqrt'),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(!result.valid);
  assert.ok(result.errors.some(e => e.includes("'sqrt'")));
});

// ============================================================
// ATM protocol
// ============================================================

test('ATM: authenticate and check balance', () => {
  const proto = atm();
  const actions = [
    new SendAction('Card', '1234-5678'),
    new SendAction('PIN', '0000'),
    new SelectAction('balance'),
    new RecvAction('Int'),
    new CloseAction(),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(result.valid, result.errors.join(', '));
});

test('ATM: authenticate and withdraw', () => {
  const proto = atm();
  const actions = [
    new SendAction('Card', '1234-5678'),
    new SendAction('PIN', '0000'),
    new SelectAction('withdraw'),
    new SendAction('Int', 100),
    new RecvAction('Bool'),
    new CloseAction(),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(result.valid, result.errors.join(', '));
});

test('ATM: quit immediately after auth', () => {
  const proto = atm();
  const actions = [
    new SendAction('Card', 'x'),
    new SendAction('PIN', 'x'),
    new SelectAction('quit'),
    new CloseAction(),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(result.valid, result.errors.join(', '));
});

// ============================================================
// Recursive protocols
// ============================================================

test('counter: inc, inc, get, done', () => {
  const proto = counter();
  const actions = [
    new SelectAction('inc'),
    new SelectAction('inc'),
    new SelectAction('get'),
    new RecvAction('Int'),
    new SelectAction('done'),
    new CloseAction(),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(result.valid, result.errors.join(', '));
});

test('counter: just done', () => {
  const proto = counter();
  const actions = [
    new SelectAction('done'),
    new CloseAction(),
  ];
  const result = checkCompliance(actions, proto);
  assert.ok(result.valid, result.errors.join(', '));
});

// ============================================================
// Pretty printing
// ============================================================

test('Send toString', () => {
  assert.equal(new Send('Int', end).toString(), '!Int.end');
});

test('Recv toString', () => {
  assert.equal(new Recv('Bool', end).toString(), '?Bool.end');
});

test('Select toString', () => {
  const s = new Select(new Map([['a', end], ['b', end]]));
  assert.ok(s.toString().includes('⊕'));
});

test('Rec toString', () => {
  const s = new Rec('X', new Send('Int', new RecVar('X')));
  assert.equal(s.toString(), 'μX.!Int.X');
});

// ============================================================
// Report
// ============================================================

console.log(`\nSession type tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
