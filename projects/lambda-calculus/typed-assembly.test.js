import { strict as assert } from 'assert';
import { TWord, TCode, TALProgram, mov, add, jmp, bnz } from './typed-assembly.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('TWord: string', () => assert.equal(new TWord().toString(), 'word'));
test('TCode: register types', () => assert.ok(new TCode({ r1: 'word' }).toString().includes('r1')));

test('typecheck: valid program', () => {
  const p = new TALProgram();
  p.addBlock('main', { r1: 'word', r2: 'word' }, [add('r3', 'r1', 'r2'), jmp('end')]);
  p.addBlock('end', { r3: 'word' }, []);
  assert.equal(p.typecheck().length, 0);
});

test('typecheck: add needs word', () => {
  const p = new TALProgram();
  p.addBlock('main', { r1: 'code' }, [add('r3', 'r1', 'r1')]);
  assert.ok(p.typecheck().length > 0);
});

test('mov: sets type', () => {
  const p = new TALProgram();
  p.addBlock('main', {}, [mov('r1', 42, 'word'), add('r2', 'r1', 'r1')]);
  assert.equal(p.typecheck().length, 0);
});

test('bnz: needs word', () => {
  const p = new TALProgram();
  p.addBlock('main', { r1: 'code' }, [bnz('r1', 'end')]);
  p.addBlock('end', {}, []);
  assert.ok(p.typecheck().length > 0);
});

test('bnz: word ok', () => {
  const p = new TALProgram();
  p.addBlock('main', { r1: 'word' }, [bnz('r1', 'end')]);
  p.addBlock('end', {}, []);
  assert.equal(p.typecheck().length, 0);
});

test('empty program: valid', () => {
  const p = new TALProgram();
  assert.equal(p.typecheck().length, 0);
});

test('empty block: valid', () => {
  const p = new TALProgram();
  p.addBlock('main', {}, []);
  assert.equal(p.typecheck().length, 0);
});

test('jmp: always valid', () => {
  const p = new TALProgram();
  p.addBlock('main', {}, [jmp('end')]);
  assert.equal(p.typecheck().length, 0);
});

console.log(`\nTyped assembly tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
