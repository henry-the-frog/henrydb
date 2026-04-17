import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Move, Game, Strategy, play, unitGame, boolGame, natGame, trueStrategy, falseStrategy, constStrategy, compose, parallel, isWinning, interactionCount } from './game-semantics.js';

describe('Game Semantics', () => {
  describe('basic games', () => {
    it('unitGame has no moves', () => {
      assert.equal(unitGame.moves.length, 0);
    });

    it('boolGame has question and answers', () => {
      assert.ok(boolGame.moves.includes('?'));
      assert.ok(boolGame.moves.includes('true'));
      assert.ok(boolGame.moves.includes('false'));
    });
  });

  describe('strategies', () => {
    it('trueStrategy responds true to question', () => {
      const history = play(boolGame, trueStrategy, ['?']);
      assert.equal(history.length, 2);
      assert.equal(history[0].player, 'O');
      assert.equal(history[0].label, '?');
      assert.equal(history[1].player, 'P');
      assert.equal(history[1].label, 'true');
    });

    it('falseStrategy responds false to question', () => {
      const history = play(boolGame, falseStrategy, ['?']);
      assert.equal(history[1].label, 'false');
    });

    it('constStrategy produces constant value', () => {
      const s = constStrategy(42);
      const history = play(natGame, s, ['?']);
      assert.equal(history[1].label, '42');
    });

    it('strategy terminates on null response', () => {
      const s = new Strategy('once', (move, _) => move === '?' ? 'done' : null);
      const history = play(boolGame, s, ['?', 'extra']);
      assert.ok(history.length <= 4);
    });
  });

  describe('composition', () => {
    it('compose chains strategies', () => {
      const s1 = new Strategy('upper', (m) => m ? m.toUpperCase() : null);
      const s2 = new Strategy('prefix', (m) => m === '?' ? 'hello' : null);
      const composed = compose(s1, s2);
      const resp = composed.respond('?', []);
      assert.equal(resp, 'HELLO');
    });

    it('parallel tries both strategies', () => {
      const s1 = new Strategy('a', (m) => m === 'x' ? 'a' : null);
      const s2 = new Strategy('b', (m) => m === 'y' ? 'b' : null);
      const par = parallel(s1, s2);
      assert.equal(par.respond('x', []), 'a');
      assert.equal(par.respond('y', []), 'b');
    });
  });

  describe('isWinning', () => {
    it('trueStrategy is winning for question moves', () => {
      assert.ok(isWinning(trueStrategy, ['?']));
    });

    it('constStrategy is winning for question moves', () => {
      assert.ok(isWinning(constStrategy(0), ['?']));
    });
  });

  describe('interaction count', () => {
    it('counts interactions correctly', () => {
      const history = play(boolGame, trueStrategy, ['?', '?', '?']);
      const count = interactionCount(history);
      assert.ok(count.questions >= 1);
    });
  });

  describe('play mechanics', () => {
    it('respects maxRounds', () => {
      const infinite = new Strategy('inf', () => 'ok');
      const history = play(boolGame, infinite, Array(100).fill('?'), 5);
      assert.ok(history.length <= 10); // 5 rounds × 2 moves
    });

    it('Move toString formats correctly', () => {
      const m = new Move('O', 'question');
      assert.equal(m.toString(), 'O:question');
    });
  });
});
