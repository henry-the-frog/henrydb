#!/usr/bin/env node
// monkey-riscv.js — Monkey-lang → RISC-V Compilation CLI
//
// Usage:
//   node monkey-riscv.js <file.monkey>           # Compile and run
//   node monkey-riscv.js --dump <file.monkey>     # Show assembly listing
//   node monkey-riscv.js --disasm <file.monkey>   # Show disassembly
//   node monkey-riscv.js --run <file.monkey>      # Compile, assemble, and execute
//   node monkey-riscv.js --opt <file.monkey>      # Compile with register allocation + peephole
//   node monkey-riscv.js -e "puts(42)"            # Compile expression

import { readFileSync } from 'fs';
import { Lexer } from '/Users/henry/repos/monkey-lang/src/lexer.js';
import { Parser } from '/Users/henry/repos/monkey-lang/src/parser.js';
import { RiscVCodeGen } from './monkey-codegen.js';
import { inferTypes } from './type-infer.js';
import { analyzeFreeVars } from './closure-analysis.js';
import { peepholeOptimize } from './riscv-peephole.js';
import { Assembler } from './assembler.js';
import { CPU } from './cpu.js';
import { disassemble, disassembleWord } from './disassembler.js';

function compilePipeline(source, { useRegisters = false, optimize = false } = {}) {
  // Parse
  const lexer = new Lexer(source);
  const parser = new Parser(lexer);
  const program = parser.parseProgram();
  if (parser.errors.length > 0) {
    console.error('Parse errors:');
    parser.errors.forEach(e => console.error('  ' + e));
    process.exit(1);
  }

  // Type inference
  const typeInfo = inferTypes(program);
  const closureInfo = analyzeFreeVars(program);

  // Code generation
  const codegen = new RiscVCodeGen({ useRegisters });
  let asm = codegen.compile(program, typeInfo, closureInfo);
  if (codegen.errors.length > 0) {
    console.error('Codegen errors:');
    codegen.errors.forEach(e => console.error('  ' + e));
    process.exit(1);
  }

  // Peephole optimization
  let peepholeStats = null;
  if (optimize) {
    const result = peepholeOptimize(asm);
    asm = result.optimized;
    peepholeStats = result.stats;
  }

  // Assemble
  const assembler = new Assembler();
  const assembled = assembler.assemble(asm);
  if (assembled.errors.length > 0) {
    console.error('Assembly errors:');
    assembled.errors.forEach(e => console.error('  ' + (e.message || e)));
    process.exit(1);
  }

  return { asm, words: assembled.words, labels: assembled.labels, typeInfo, closureInfo, peepholeStats };
}

// Parse args
const args = process.argv.slice(2);
let mode = 'run';
let source = null;
let optimize = false;

for (let i = 0; i < args.length; i++) {
  switch (args[i]) {
    case '--dump': mode = 'dump'; break;
    case '--disasm': mode = 'disasm'; break;
    case '--run': mode = 'run'; break;
    case '--opt': optimize = true; break;
    case '-e': source = args[++i]; break;
    default:
      if (!args[i].startsWith('-')) {
        source = readFileSync(args[i], 'utf-8');
      }
  }
}

if (!source) {
  console.log('Usage: node monkey-riscv.js [--dump|--disasm|--run|--opt] [-e expr | file.monkey]');
  process.exit(0);
}

const { asm, words, labels, typeInfo, closureInfo, peepholeStats } = compilePipeline(source, { 
  useRegisters: optimize, 
  optimize 
});

switch (mode) {
  case 'dump':
    console.log('=== Monkey → RISC-V Assembly ===');
    console.log(asm);
    if (peepholeStats) {
      console.log('\n=== Peephole Stats ===');
      console.log(`  Removed: ${peepholeStats.removed} instructions`);
      console.log(`  Patterns: ${JSON.stringify(peepholeStats.patterns)}`);
    }
    console.log(`\n${words.length} words (${words.length * 4} bytes)`);
    break;

  case 'disasm':
    console.log('=== RISC-V Machine Code Disassembly ===');
    console.log(disassemble(words));
    console.log(`\n${words.length} words (${words.length * 4} bytes)`);
    break;

  case 'run':
  default:
    const cpu = new CPU();
    cpu.loadProgram(words);
    cpu.regs.set(2, 0x100000 - 4);
    const start = performance.now();
    cpu.run(10000000);
    const elapsed = performance.now() - start;
    
    if (cpu.output.length > 0) {
      process.stdout.write(cpu.output.join(''));
      if (!cpu.output[cpu.output.length - 1].endsWith('\n')) {
        process.stdout.write('\n');
      }
    }
    
    console.error(`[${cpu.cycles} cycles, ${elapsed.toFixed(1)}ms, ${words.length} instructions]`);
    break;
}
