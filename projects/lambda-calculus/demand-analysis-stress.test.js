/**
 * Demand Analysis Stress Tests
 * Using correct exports from demand-analysis.js
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { D_ABSENT, D_LAZY, D_HEAD, D_STRICT, D_SEQ, DCall, Var, Num, App, Lam, Case, Let, analyzeDemand, lubDemand, demandString } from './demand-analysis.js';

describe('Demand Analysis', () => {
  describe('basic demands', () => {
    it('variable reference is strict', () => {
      const d = analyzeDemand(new Var('x'), 'x');
      assert.equal(d.tag, 'Strict');
    });

    it('unmentioned variable is absent', () => {
      const d = analyzeDemand(new Var('y'), 'x');
      assert.equal(d.tag, 'Absent');
    });

    it('literal is absent for any variable', () => {
      const d = analyzeDemand(new Num(42), 'x');
      assert.equal(d.tag, 'Absent');
    });

    it('lambda body is absent (not yet evaluated)', () => {
      const d = analyzeDemand(new Lam('y', new Var('x')), 'x');
      assert.equal(d.tag, 'Absent');
    });
  });

  describe('application demands', () => {
    it('function application of variable gives call demand', () => {
      // f x → C(1) for f
      const d = analyzeDemand(new App(new Var('f'), new Var('x')), 'f');
      assert.equal(d.tag, 'Call');
      assert.equal(d.arity, 1);
    });

    it('double application gives call demand 2', () => {
      // f x y → C(2) for f
      const d = analyzeDemand(new App(new App(new Var('f'), new Var('x')), new Var('y')), 'f');
      assert.equal(d.tag, 'Call');
      assert.equal(d.arity, 2);
    });

    it('argument of application is strict', () => {
      // f x → Strict for x
      const d = analyzeDemand(new App(new Var('f'), new Var('x')), 'x');
      assert.equal(d.tag, 'Strict');
    });

    it('unrelated variable in application is absent', () => {
      const d = analyzeDemand(new App(new Var('f'), new Var('y')), 'x');
      assert.equal(d.tag, 'Absent');
    });
  });

  describe('case expression demands', () => {
    it('case scrutinee is head-strict', () => {
      // case x of { ... } → Head for x
      const d = analyzeDemand(new Case(new Var('x'), [new Num(1)]), 'x');
      assert.equal(d.tag, 'Head');
    });

    it('variable in case alternatives combines with lub', () => {
      // case y of { x; x } → strict (x mentioned in alts)
      const d = analyzeDemand(new Case(new Var('y'), [new Var('x'), new Var('x')]), 'x');
      assert.equal(d.tag, 'Strict');
    });

    it('variable in scrutinee and alt gives head', () => {
      // case x of { ... } even if x is in alternatives
      const d = analyzeDemand(new Case(new Var('x'), [new Var('z')]), 'x');
      assert.equal(d.tag, 'Head');
    });
  });

  describe('let expression demands', () => {
    it('variable in let init is strict', () => {
      // let y = x in z → Strict for x
      const d = analyzeDemand(new Let('y', new Var('x'), new Var('z')), 'x');
      assert.equal(d.tag, 'Strict');
    });

    it('variable in let body is strict', () => {
      // let y = z in x → Strict for x
      const d = analyzeDemand(new Let('y', new Var('z'), new Var('x')), 'x');
      assert.equal(d.tag, 'Strict');
    });

    it('variable not in let is absent', () => {
      const d = analyzeDemand(new Let('y', new Num(1), new Var('y')), 'x');
      assert.equal(d.tag, 'Absent');
    });
  });

  describe('lub (least upper bound)', () => {
    it('absent is identity', () => {
      assert.equal(lubDemand(D_ABSENT, D_STRICT).tag, 'Strict');
      assert.equal(lubDemand(D_STRICT, D_ABSENT).tag, 'Strict');
      assert.equal(lubDemand(D_ABSENT, D_ABSENT).tag, 'Absent');
    });

    it('strict dominates', () => {
      assert.equal(lubDemand(D_STRICT, D_HEAD).tag, 'Strict');
      assert.equal(lubDemand(D_HEAD, D_STRICT).tag, 'Strict');
      assert.equal(lubDemand(D_STRICT, D_LAZY).tag, 'Strict');
    });

    it('head dominates lazy', () => {
      assert.equal(lubDemand(D_HEAD, D_LAZY).tag, 'Head');
      assert.equal(lubDemand(D_LAZY, D_HEAD).tag, 'Head');
    });
  });

  describe('demandString', () => {
    it('formats demands correctly', () => {
      assert.equal(demandString(D_ABSENT), 'Absent');
      assert.equal(demandString(D_STRICT), 'Strict');
      assert.equal(demandString(D_HEAD), 'Head');
      assert.equal(demandString(new DCall(2)), 'C(2)');
    });
  });

  describe('complex expressions', () => {
    it('nested let preserves demand', () => {
      // let a = x in let b = a in b → Strict for x
      const expr = new Let('a', new Var('x'), new Let('b', new Var('a'), new Var('b')));
      assert.equal(analyzeDemand(expr, 'x').tag, 'Strict');
    });

    it('deeply nested application', () => {
      // f (g x) → Strict for x (used as argument)
      const expr = new App(new Var('f'), new App(new Var('g'), new Var('x')));
      assert.equal(analyzeDemand(expr, 'x').tag, 'Strict');
    });

    it('case in let init', () => {
      // let y = (case x of { ... }) in z → Head for x
      const expr = new Let('y', new Case(new Var('x'), [new Num(1)]), new Var('z'));
      assert.equal(analyzeDemand(expr, 'x').tag, 'Head');
    });
  });
});
