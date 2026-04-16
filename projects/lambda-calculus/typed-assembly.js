/**
 * Typed Assembly Language (TAL): Types in assembly
 */
class TWord { constructor() { this.tag='TWord'; } toString() { return 'word'; } }
class TCode { constructor(regs) { this.tag='TCode'; this.regs = regs; } toString() { return `code{${Object.entries(this.regs).map(([k,v]) => `${k}:${v}`).join(',')}}`; } }

class TALProgram {
  constructor() { this.blocks = new Map(); this.types = new Map(); }
  addBlock(label, regTypes, instrs) { this.blocks.set(label, { regTypes, instrs }); this.types.set(label, new TCode(regTypes)); }
  typecheck() {
    const errors = [];
    for (const [label, block] of this.blocks) {
      let env = { ...block.regTypes };
      for (const instr of block.instrs) {
        const err = checkInstr(instr, env);
        if (err) errors.push({ label, instr: instr.op, error: err });
        env = updateEnv(instr, env);
      }
    }
    return errors;
  }
}

function checkInstr(instr, env) {
  switch(instr.op) {
    case 'mov': return null;
    case 'add': return (env[instr.src1] !== 'word' || env[instr.src2] !== 'word') ? `add: need word operands` : null;
    case 'jmp': return null;
    case 'bnz': return env[instr.reg] !== 'word' ? `bnz: need word` : null;
    default: return null;
  }
}

function updateEnv(instr, env) {
  const newEnv = { ...env };
  if (instr.op === 'mov') newEnv[instr.dst] = instr.type || 'word';
  if (instr.op === 'add') newEnv[instr.dst] = 'word';
  return newEnv;
}

const mov = (dst, src, type) => ({ op: 'mov', dst, src, type });
const add = (dst, src1, src2) => ({ op: 'add', dst, src1, src2 });
const jmp = label => ({ op: 'jmp', label });
const bnz = (reg, label) => ({ op: 'bnz', reg, label });

export { TWord, TCode, TALProgram, mov, add, jmp, bnz };
