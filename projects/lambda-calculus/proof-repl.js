#!/usr/bin/env node

/**
 * Proof Assistant REPL
 * 
 * Interactive command-line proof environment.
 * 
 * Commands:
 *   theorem <name> : <type>  - Begin a new proof
 *   intro [name]            - Introduce a variable
 *   intros [n1 n2 ...]      - Introduce multiple variables
 *   apply <name>            - Apply a hypothesis
 *   exact <term>            - Provide exact proof term
 *   refl                    - Prove reflexive equality
 *   assumption              - Use a matching hypothesis
 *   simpl                   - Normalize the goal
 *   trivial                 - Auto-solve
 *   qed                     - Complete proof
 *   show                    - Show current state
 *   print <name>            - Print a definition
 *   list                    - List all definitions
 *   help                    - Show help
 *   quit                    - Exit
 */

import readline from 'readline';
import {
  Star, Box, Var, Pi, Lam, App, Nat, Zero, Succ, NatElim,
  Context, infer, check, normalize, betaEq, arrow, parse, resetNames
} from './coc.js';
import { eqType, refl } from './coc-proofs.js';
import { ProofAssistant } from './proof-assistant.js';

const pa = new ProofAssistant();

// ANSI colors
const BOLD = '\x1b[1m';
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[34m';
const CYAN = '\x1b[36m';
const RESET = '\x1b[0m';

function colorize(text) {
  return text
    .replace(/★/g, `${YELLOW}★${RESET}`)
    .replace(/□/g, `${BLUE}□${RESET}`)
    .replace(/ℕ/g, `${CYAN}ℕ${RESET}`)
    .replace(/Π/g, `${BOLD}Π${RESET}`)
    .replace(/λ/g, `${BOLD}λ${RESET}`)
    .replace(/→/g, `${BOLD}→${RESET}`)
    .replace(/⊢/g, `${GREEN}⊢${RESET}`)
    .replace(/✓/g, `${GREEN}✓${RESET}`)
    .replace(/─+/g, (m) => `${YELLOW}${m}${RESET}`);
}

function printHelp() {
  console.log(`
${BOLD}Proof Assistant Commands:${RESET}

  ${CYAN}theorem${RESET} <name> : <type>  Begin a new proof
  ${CYAN}intro${RESET} [name]            Introduce a Π-bound variable
  ${CYAN}intros${RESET} [n1 n2 ...]      Introduce multiple variables
  ${CYAN}apply${RESET} <name>            Apply a hypothesis
  ${CYAN}exact${RESET} <term>            Provide exact proof term
  ${CYAN}refl${RESET}                    Prove reflexive equality
  ${CYAN}assumption${RESET}              Use a matching hypothesis
  ${CYAN}simpl${RESET}                   Normalize the goal
  ${CYAN}trivial${RESET}                 Auto-solve with refl or assumption
  ${CYAN}qed${RESET}                     Complete the proof
  ${CYAN}show${RESET}                    Show current state
  ${CYAN}print${RESET} <name>            Print a proved theorem
  ${CYAN}list${RESET}                    List all proved theorems
  ${CYAN}help${RESET}                    Show this help
  ${CYAN}quit${RESET}                    Exit

${BOLD}Types:${RESET}
  ★ (Type), □ (Kind), ℕ (Nat), 0, S n
  Π(x:A).B, λ(x:A).t, f x
  A → B (non-dependent function)

${BOLD}Examples:${RESET}
  theorem id : Π(A:★).A → A
  intro A
  intro x
  assumption
  qed
`);
}

function processCommand(input) {
  const trimmed = input.trim();
  if (!trimmed) return;
  
  const parts = trimmed.split(/\s+/);
  const cmd = parts[0].toLowerCase();
  
  try {
    switch (cmd) {
      case 'help':
      case '?':
        printHelp();
        break;
        
      case 'quit':
      case 'exit':
        console.log(`${GREEN}Goodbye!${RESET}`);
        process.exit(0);
        
      case 'theorem':
      case 'lemma': {
        // theorem name : type
        const colonIdx = trimmed.indexOf(':');
        if (colonIdx === -1) {
          console.log(`${RED}Usage: theorem <name> : <type>${RESET}`);
          break;
        }
        const name = trimmed.slice(cmd.length, colonIdx).trim();
        const typeStr = trimmed.slice(colonIdx + 1).trim();
        try {
          const goalType = parse(typeStr);
          const state = pa.theorem(name, goalType);
          console.log(colorize(`\n${BOLD}Proving: ${name}${RESET}\n${state}\n`));
        } catch (e) {
          console.log(`${RED}Parse error: ${e.message}${RESET}`);
        }
        break;
      }
      
      case 'intro': {
        const name = parts[1] || undefined;
        const state = pa.tactic('intro', name);
        console.log(colorize(state));
        break;
      }
      
      case 'intros': {
        const names = parts.slice(1);
        const state = pa.tactic('intros', names);
        console.log(colorize(state));
        break;
      }
      
      case 'apply': {
        const name = parts[1];
        if (!name) { console.log(`${RED}Usage: apply <hypothesis>${RESET}`); break; }
        const state = pa.tactic('apply', name);
        console.log(colorize(state));
        break;
      }
      
      case 'exact': {
        const termStr = parts.slice(1).join(' ');
        try {
          const term = parse(termStr);
          const state = pa.tactic('exact', term);
          console.log(colorize(state));
        } catch (e) {
          console.log(`${RED}Parse error: ${e.message}${RESET}`);
        }
        break;
      }
      
      case 'refl': {
        const state = pa.tactic('refl');
        console.log(colorize(state));
        break;
      }
      
      case 'assumption': {
        const state = pa.tactic('assumption');
        console.log(colorize(state));
        break;
      }
      
      case 'simpl': {
        const state = pa.tactic('simpl');
        console.log(colorize(state));
        break;
      }
      
      case 'trivial': {
        const state = pa.tactic('trivial');
        console.log(colorize(state));
        break;
      }
      
      case 'induction': {
        const name = parts[1];
        if (!name) { console.log(`${RED}Usage: induction <variable>${RESET}`); break; }
        const state = pa.tactic('induction', name);
        console.log(colorize(state));
        break;
      }
      
      case 'qed': {
        const proof = pa.qed();
        console.log(`${GREEN}${BOLD}✓ ${proof.name}${RESET} ${GREEN}proved!${RESET}`);
        console.log(`  Type: ${colorize(proof.type.toString())}`);
        console.log(`  Tactics: ${proof.tactics.map(t => t.tactic).join(', ')}`);
        break;
      }
      
      case 'show': {
        console.log(colorize(pa.show()));
        break;
      }
      
      case 'print': {
        const name = parts[1];
        const def = pa.definitions.get(name);
        if (!def) { console.log(`${RED}Unknown: ${name}${RESET}`); break; }
        console.log(`${BOLD}${name}${RESET} : ${colorize(def.type.toString())}`);
        break;
      }
      
      case 'list': {
        if (pa.definitions.size === 0) {
          console.log('No definitions yet.');
        } else {
          for (const [name, def] of pa.definitions) {
            console.log(`  ${BOLD}${name}${RESET} : ${colorize(def.type.toString())}`);
          }
        }
        break;
      }
      
      default:
        console.log(`${RED}Unknown command: ${cmd}. Type 'help' for commands.${RESET}`);
    }
  } catch (e) {
    console.log(`${RED}Error: ${e.message}${RESET}`);
  }
}

// ============================================================
// Main REPL Loop
// ============================================================

console.log(`${BOLD}${CYAN}╔════════════════════════════════════╗${RESET}`);
console.log(`${BOLD}${CYAN}║   Mini Proof Assistant v1.0       ║${RESET}`);
console.log(`${BOLD}${CYAN}║   Based on Calculus of Constructions║${RESET}`);
console.log(`${BOLD}${CYAN}╚════════════════════════════════════╝${RESET}`);
console.log(`Type ${CYAN}help${RESET} for commands.\n`);

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: pa.state ? `${YELLOW}proof>${RESET} ` : `${GREEN}CoC>${RESET} `
});

rl.prompt();

rl.on('line', (line) => {
  processCommand(line);
  rl.setPrompt(pa.state ? `${YELLOW}proof>${RESET} ` : `${GREEN}CoC>${RESET} `);
  rl.prompt();
});

rl.on('close', () => {
  console.log(`\n${GREEN}Goodbye!${RESET}`);
  process.exit(0);
});
