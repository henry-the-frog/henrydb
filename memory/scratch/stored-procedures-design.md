# Stored Procedures / CREATE FUNCTION Design

uses: 0
created: 2026-04-18
tags: database, henrydb, stored-procedures, functions

## Recommended Approach: Phased

### Phase 1: SQL Scalar Functions
```sql
CREATE FUNCTION full_name(first TEXT, last TEXT) RETURNS TEXT
AS $$ SELECT first || ' ' || last $$;

SELECT full_name(first_name, last_name) FROM users;
```
- Body is a single SQL expression
- Parameters bound by name
- Immutable (no side effects)
- Parser: extend CREATE statement, store in function catalog

### Phase 2: JS Functions
```sql
CREATE FUNCTION calculate_tax(amount DECIMAL, rate DECIMAL) RETURNS DECIMAL
LANGUAGE js AS $$ return amount * rate; $$;
```
- Execute via `new Function()` or `vm.runInContext()`
- Sandbox: no require, no globals
- Immutable by default, VOLATILE keyword for side effects

### Phase 3: Table-Returning Functions
```sql
CREATE FUNCTION get_team_members(team_id INT) RETURNS TABLE(name TEXT, role TEXT)
AS $$ SELECT name, role FROM employees WHERE team = team_id $$;

SELECT * FROM get_team_members(1);
```

### Phase 4: Procedures (CALL)
```sql
CREATE PROCEDURE transfer(from_id INT, to_id INT, amount INT) AS $$
  UPDATE accounts SET balance = balance - amount WHERE id = from_id;
  UPDATE accounts SET balance = balance + amount WHERE id = to_id;
$$;

CALL transfer(1, 2, 100);
```

## Implementation Sketch
1. `this._functions = new Map()` in Database constructor
2. Parser: handle `CREATE FUNCTION name(params) RETURNS type AS $$ body $$`
3. `_evalExpr`: when encountering a function call, check _functions before built-ins
4. For SQL functions: substitute params and eval the expression
5. For JS functions: compile once, cache, execute with params

## Concerns
- SQL injection in function bodies (mitigated by dollar-quoting)
- Recursive functions (need stack depth limit)
- Type checking (can defer to runtime initially)
