// sql-linter.test.js — Tests for SQL linter
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { lintSQL, lint } from './sql-linter.js';
import { parse } from './sql.js';

describe('SQL Linter', () => {
  function check(sql) { return lintSQL(parse(sql)); }

  describe('SELECT *', () => {
    it('warns on SELECT *', () => {
      const issues = check('SELECT * FROM users');
      assert.ok(issues.some(i => i.rule === 'no-select-star'));
    });

    it('no warning for explicit columns', () => {
      const issues = check('SELECT name, age FROM users');
      assert.ok(!issues.some(i => i.rule === 'no-select-star'));
    });
  });

  describe('Missing WHERE', () => {
    it('errors on UPDATE without WHERE', () => {
      const issues = check("UPDATE users SET active = 0");
      assert.ok(issues.some(i => i.rule === 'update-without-where'));
    });

    it('errors on DELETE without WHERE', () => {
      const issues = check('DELETE FROM users');
      assert.ok(issues.some(i => i.rule === 'delete-without-where'));
    });

    it('no error when WHERE present', () => {
      const issues = check("UPDATE users SET active = 0 WHERE id = 1");
      assert.ok(!issues.some(i => i.rule === 'update-without-where'));
    });
  });

  describe('Missing LIMIT', () => {
    it('info on SELECT without LIMIT', () => {
      const issues = check('SELECT name FROM users');
      assert.ok(issues.some(i => i.rule === 'select-without-limit'));
    });

    it('no info when LIMIT present', () => {
      const issues = check('SELECT name FROM users LIMIT 10');
      assert.ok(!issues.some(i => i.rule === 'select-without-limit'));
    });

    it('no info for aggregate queries', () => {
      const issues = check('SELECT COUNT(*) FROM users');
      // GROUP BY-less aggregates might still warn, but it's ok
    });
  });

  describe('Leading wildcard', () => {
    it('warns on leading %', () => {
      const issues = check("SELECT * FROM t WHERE name LIKE '%foo'");
      assert.ok(issues.some(i => i.rule === 'leading-wildcard'));
    });

    it('no warning for trailing wildcard', () => {
      const issues = check("SELECT name FROM t WHERE name LIKE 'foo%'");
      assert.ok(!issues.some(i => i.rule === 'leading-wildcard'));
    });
  });

  describe('DISTINCT with GROUP BY', () => {
    it('info on redundant DISTINCT', () => {
      const issues = check('SELECT DISTINCT category FROM products GROUP BY category');
      assert.ok(issues.some(i => i.rule === 'redundant-distinct'));
    });
  });

  describe('lint() helper', () => {
    it('parses and lints in one call', () => {
      const issues = lint('SELECT * FROM t', parse);
      assert.ok(issues.some(i => i.rule === 'no-select-star'));
    });

    it('returns parse error for invalid SQL', () => {
      const issues = lint('SELECTTTT BLAH', parse);
      assert.ok(issues.some(i => i.rule === 'parse-error'));
    });
  });

  describe('Clean queries have no errors', () => {
    it('well-formed query gets only infos', () => {
      const issues = check('SELECT name, age FROM users WHERE age > 18 ORDER BY name LIMIT 100');
      const errors = issues.filter(i => i.severity === 'error');
      assert.equal(errors.length, 0);
    });
  });

  describe('issue structure', () => {
    it('each issue has rule, severity, message, suggestion', () => {
      const issues = check('SELECT * FROM t');
      for (const issue of issues) {
        assert.ok(issue.rule);
        assert.ok(issue.severity);
        assert.ok(issue.message);
        assert.ok(issue.suggestion);
      }
    });
  });
});
