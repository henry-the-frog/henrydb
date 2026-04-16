/**
 * Game Semantics: Types as games, programs as strategies
 * 
 * A game is played between Proponent (program) and Opponent (environment).
 * Types describe the rules, terms describe winning strategies.
 * 
 * Base type: O asks, P answers (question-answer)
 * A → B: Opponent starts in B, Proponent responds by playing in A
 * A × B: Proponent chooses which component to play
 */

class Move {
  constructor(player, label) {
    this.player = player; // 'O' (opponent) or 'P' (proponent)
    this.label = label;
  }
  toString() { return `${this.player}:${this.label}`; }
}

class Game {
  constructor(name, moves) {
    this.name = name;
    this.moves = moves; // Array of allowed moves
  }
}

class Strategy {
  constructor(name, respond) {
    this.name = name;
    this.respond = respond; // (opponentMove, history) → proponentMove
  }
}

// Play a game
function play(game, strategy, opponentMoves, maxRounds = 10) {
  const history = [];
  let round = 0;
  
  for (const oMove of opponentMoves) {
    if (round >= maxRounds) break;
    history.push(new Move('O', oMove));
    
    const pResponse = strategy.respond(oMove, history);
    if (pResponse === null) break; // Strategy terminates
    history.push(new Move('P', pResponse));
    round++;
  }
  
  return history;
}

// Simple games
const unitGame = new Game('Unit', []);
const boolGame = new Game('Bool', ['?', 'true', 'false']);
const natGame = new Game('Nat', ['?', '0', 'S', 'n']);

// Strategies for basic types
const trueStrategy = new Strategy('true', (move, _) => move === '?' ? 'true' : null);
const falseStrategy = new Strategy('false', (move, _) => move === '?' ? 'false' : null);
const constStrategy = (value) => new Strategy(`const(${value})`, (move, _) => move === '?' ? String(value) : null);

// Composition of strategies
function compose(s1, s2) {
  return new Strategy(`${s1.name} ∘ ${s2.name}`, (move, history) => {
    const intermediate = s2.respond(move, history);
    if (intermediate === null) return null;
    return s1.respond(intermediate, history);
  });
}

// Parallel composition (for products)
function parallel(s1, s2) {
  return new Strategy(`${s1.name} ⊗ ${s2.name}`, (move, history) => {
    const r1 = s1.respond(move, history);
    if (r1 !== null) return r1;
    return s2.respond(move, history);
  });
}

// Check if a strategy is winning (responds to all opponent moves)
function isWinning(strategy, opponentMoves) {
  for (const move of opponentMoves) {
    const response = strategy.respond(move, []);
    if (response === null) return false;
  }
  return true;
}

// Count interactions
function interactionCount(history) {
  return { questions: history.filter(m => m.player === 'O').length, answers: history.filter(m => m.player === 'P').length };
}

export { Move, Game, Strategy, play, unitGame, boolGame, natGame, trueStrategy, falseStrategy, constStrategy, compose, parallel, isWinning, interactionCount };
