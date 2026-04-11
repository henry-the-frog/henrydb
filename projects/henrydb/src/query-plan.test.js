// query-plan.test.js — Tests for tree-structured query plan
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  PlanNode, SeqScanNode, IndexScanNode, BTreePKLookupNode,
  HashJoinNode, NestedLoopNode, HashNode, AggregateNode, WindowAggNode,
  SortNode, UniqueNode, LimitNode, FilterNode, CTEScanNode, AppendNode,
  PlanBuilder, PlanFormatter,
} from './query-plan.js';
import { Database } from './db.js';

describe('PlanNode basics', () => {
  it('creates nodes with estimates', () => {
    const node = new SeqScanNode('users', { estimatedRows: 1000, estimatedCost: 35.0 });
    assert.equal(node.type, 'Seq Scan');
    assert.equal(node.table, 'users');
    assert.equal(node.estimatedRows, 1000);
    assert.equal(node.estimatedCost, 35.0);
    assert.equal(node.actualRows, null);
  });

  it('sets actuals for EXPLAIN ANALYZE', () => {
    const node = new SeqScanNode('users', { estimatedRows: 1000 });
    node.setActuals(987, 3.5);
    assert.equal(node.actualRows, 987);
    assert.equal(node.actualTime, 3.5);
    assert.equal(node.actualLoops, 1);
  });

  it('builds a tree with children', () => {
    const scan = new SeqScanNode('orders', { estimatedRows: 5000 });
    const hash = new HashNode({ estimatedRows: 500 });
    hash.addChild(new SeqScanNode('users', { estimatedRows: 500 }));
    
    const join = new HashJoinNode('INNER', 'orders.user_id = users.id', { estimatedRows: 5000 });
    join.addChild(scan);
    join.addChild(hash);
    
    assert.equal(join.children.length, 2);
    assert.equal(join.children[0].table, 'orders');
    assert.equal(join.children[1].type, 'Hash');
    assert.equal(join.children[1].children[0].table, 'users');
  });
});

describe('PlanFormatter - text output', () => {
  it('formats a simple seq scan', () => {
    const scan = new SeqScanNode('users', { estimatedRows: 1000, estimatedCost: 35.0, startupCost: 0 });
    const lines = PlanFormatter.format(scan);
    assert.equal(lines.length, 1);
    assert.ok(lines[0].includes('Seq Scan on users'));
    assert.ok(lines[0].includes('cost=0.00..35.00'));
    assert.ok(lines[0].includes('rows=1000'));
  });

  it('formats a scan with filter', () => {
    const scan = new SeqScanNode('users', { estimatedRows: 100, estimatedCost: 35.0 });
    scan.filter = 'age > 21';
    const lines = PlanFormatter.format(scan);
    assert.ok(lines.length >= 2);
    assert.ok(lines[0].includes('Seq Scan on users'));
    assert.ok(lines[1].includes('Filter: age > 21'));
  });

  it('formats index scan with condition', () => {
    const scan = new IndexScanNode('users', 'idx_users_email', {
      estimatedRows: 1, estimatedCost: 4.01, indexCond: 'email = $1',
    });
    const lines = PlanFormatter.format(scan);
    assert.ok(lines[0].includes('Index Scan using idx_users_email on users'));
    assert.ok(lines[1].includes('Index Cond: email = $1'));
  });

  it('formats hash join tree', () => {
    const ordersScan = new SeqScanNode('orders', { estimatedRows: 5000, estimatedCost: 85.0 });
    const usersScan = new SeqScanNode('users', { estimatedRows: 500, estimatedCost: 25.0 });
    const hash = new HashNode({ estimatedRows: 500, estimatedCost: 10.0 });
    hash.addChild(usersScan);
    
    const join = new HashJoinNode('INNER', 'orders.user_id = users.id', {
      estimatedRows: 5000, estimatedCost: 120.0,
    });
    join.addChild(ordersScan);
    join.addChild(hash);
    
    const lines = PlanFormatter.format(join);
    assert.ok(lines[0].includes('Hash Join'));
    assert.ok(lines.some(l => l.includes('Hash Cond: orders.user_id = users.id')));
    assert.ok(lines.some(l => l.includes('Seq Scan on orders')));
    assert.ok(lines.some(l => l.includes('Hash')));
    assert.ok(lines.some(l => l.includes('Seq Scan on users')));
  });

  it('formats sort with keys', () => {
    const scan = new SeqScanNode('products', { estimatedRows: 200, estimatedCost: 10.0 });
    const sort = new SortNode([{ column: 'price', direction: 'DESC' }], {
      estimatedRows: 200, estimatedCost: 25.0,
    });
    sort.addChild(scan);
    
    const lines = PlanFormatter.format(sort);
    assert.ok(lines[0].includes('Sort'));
    assert.ok(lines.some(l => l.includes('Sort Key: price DESC')));
    assert.ok(lines.some(l => l.includes('Seq Scan on products')));
  });

  it('formats aggregate with group keys', () => {
    const scan = new SeqScanNode('orders', { estimatedRows: 10000, estimatedCost: 100.0 });
    const agg = new AggregateNode('Hashed', {
      estimatedRows: 50, estimatedCost: 200.0, groupKeys: ['customer_id'],
    });
    agg.addChild(scan);
    
    const lines = PlanFormatter.format(agg);
    assert.ok(lines[0].includes('HashedAggregate'));
    assert.ok(lines.some(l => l.includes('Group Key: customer_id')));
  });

  it('formats EXPLAIN ANALYZE with actuals', () => {
    const scan = new SeqScanNode('users', { estimatedRows: 1000, estimatedCost: 35.0 });
    scan.setActuals(987, 3.456);
    
    const lines = PlanFormatter.format(scan, { analyze: true });
    assert.ok(lines[0].includes('actual rows=987'));
    assert.ok(lines[0].includes('time=3.456ms'));
  });

  it('formats complex multi-level plan', () => {
    // SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE u.age > 21 ORDER BY o.total DESC LIMIT 10
    const ordersScan = new SeqScanNode('orders', { estimatedRows: 5000, estimatedCost: 85.0 });
    const usersScan = new SeqScanNode('users', { estimatedRows: 500, estimatedCost: 25.0 });
    usersScan.filter = 'age > 21';
    
    const hash = new HashNode({ estimatedRows: 350, estimatedCost: 7.0 });
    hash.addChild(usersScan);
    
    const join = new HashJoinNode('INNER', 'o.user_id = u.id', { estimatedRows: 3500, estimatedCost: 120.0 });
    join.addChild(ordersScan);
    join.addChild(hash);
    
    const sort = new SortNode([{ column: 'total', direction: 'DESC' }], { estimatedRows: 3500, estimatedCost: 160.0 });
    sort.addChild(join);
    
    const limit = new LimitNode(10, { estimatedRows: 10, estimatedCost: 160.1 });
    limit.addChild(sort);
    
    const lines = PlanFormatter.format(limit);
    assert.ok(lines[0].includes('Limit'));
    assert.ok(lines.some(l => l.includes('Sort')));
    assert.ok(lines.some(l => l.includes('Hash Join')));
    assert.ok(lines.some(l => l.includes('Seq Scan on orders')));
    assert.ok(lines.some(l => l.includes('Seq Scan on users')));
    assert.ok(lines.some(l => l.includes('Filter: age > 21')));
    // Verify tree structure (indentation increases)
    assert.ok(lines.length >= 7);
  });

  it('formats LEFT JOIN', () => {
    const left = new SeqScanNode('orders', { estimatedRows: 100 });
    const right = new SeqScanNode('returns', { estimatedRows: 10 });
    const hash = new HashNode({ estimatedRows: 10 });
    hash.addChild(right);
    const join = new HashJoinNode('LEFT', 'orders.id = returns.order_id', { estimatedRows: 100 });
    join.addChild(left);
    join.addChild(hash);
    
    const lines = PlanFormatter.format(join);
    assert.ok(lines[0].includes('Hash LEFT Join'));
  });
});

describe('PlanFormatter - JSON output', () => {
  it('converts plan to JSON', () => {
    const scan = new SeqScanNode('users', { estimatedRows: 500, estimatedCost: 25.0 });
    scan.filter = 'active = true';
    
    const json = PlanFormatter.toJSON(scan);
    assert.equal(json['Node Type'], 'Seq Scan');
    assert.equal(json['Relation Name'], 'users');
    assert.equal(json['Plan Rows'], 500);
    assert.equal(json['Total Cost'], 25.0);
    assert.equal(json['Filter'], 'active = true');
  });

  it('converts join tree to JSON with nested Plans', () => {
    const left = new SeqScanNode('a', { estimatedRows: 100 });
    const right = new SeqScanNode('b', { estimatedRows: 50 });
    const hash = new HashNode();
    hash.addChild(right);
    const join = new HashJoinNode('INNER', 'a.id = b.a_id', { estimatedRows: 100 });
    join.addChild(left);
    join.addChild(hash);
    
    const json = PlanFormatter.toJSON(join);
    assert.equal(json['Node Type'], 'Hash Join');
    assert.equal(json['Hash Cond'], 'a.id = b.a_id');
    assert.ok(json['Plans']);
    assert.equal(json['Plans'].length, 2);
    assert.equal(json['Plans'][0]['Relation Name'], 'a');
    assert.equal(json['Plans'][1]['Node Type'], 'Hash');
  });
});

describe('PlanBuilder - from live database', () => {
  function makeDB() {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO users VALUES (${i}, 'user${i}', ${20 + (i % 40)})`);
    }
    for (let i = 1; i <= 500; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${1 + (i % 100)}, ${(i * 9.99).toFixed(2)}, '${i % 3 === 0 ? "shipped" : "pending"}')`);
    }
    return db;
  }

  it('builds plan for simple SELECT', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const ast = { type: 'SELECT', columns: [{ type: 'star' }], from: { table: 'users' } };
    const plan = builder.buildPlan(ast);
    
    assert.equal(plan.type, 'Seq Scan');
    assert.equal(plan.table, 'users');
    assert.equal(plan.estimatedRows, 100);
    assert.ok(plan.estimatedCost > 0);
  });

  it('builds plan for SELECT with WHERE', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const ast = {
      type: 'SELECT',
      columns: [{ type: 'star' }],
      from: { table: 'users' },
      where: { type: 'binary', operator: '>', left: { type: 'column_ref', column: 'age' }, right: { type: 'number', value: 30 } },
    };
    const plan = builder.buildPlan(ast);
    
    assert.equal(plan.type, 'Seq Scan');
    assert.ok(plan.filter); // filter attached
    assert.ok(plan.estimatedRows < 100); // selectivity applied
  });

  it('builds plan for JOIN', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const ast = {
      type: 'SELECT',
      columns: [{ type: 'star' }],
      from: { table: 'orders' },
      joins: [{
        type: 'INNER',
        table: { table: 'users' },
        on: {
          type: 'binary', operator: '=',
          left: { type: 'column_ref', table: 'orders', column: 'user_id' },
          right: { type: 'column_ref', table: 'users', column: 'id' },
        },
      }],
    };
    const plan = builder.buildPlan(ast);
    
    assert.equal(plan.type, 'Hash Join');
    assert.ok(plan.hashCond.includes('user_id'));
    assert.equal(plan.children.length, 2);
    assert.equal(plan.children[0].type, 'Seq Scan');
    assert.equal(plan.children[0].table, 'orders');
    assert.equal(plan.children[1].type, 'Hash');
    assert.equal(plan.children[1].children[0].table, 'users');
  });

  it('builds plan for GROUP BY', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const ast = {
      type: 'SELECT',
      columns: [
        { type: 'column_ref', column: 'status' },
        { type: 'aggregate', func: 'COUNT', args: [{ type: 'star' }] },
      ],
      from: { table: 'orders' },
      groupBy: ['status'],
    };
    const plan = builder.buildPlan(ast);
    
    assert.equal(plan.type, 'Aggregate');
    assert.equal(plan.strategy, 'Hashed');
    assert.deepEqual(plan.groupKeys, ['status']);
    assert.ok(plan.estimatedRows < 500); // grouped
    assert.equal(plan.children[0].type, 'Seq Scan');
  });

  it('builds plan for ORDER BY + LIMIT', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const ast = {
      type: 'SELECT',
      columns: [{ type: 'star' }],
      from: { table: 'orders' },
      orderBy: [{ column: 'total', direction: 'DESC' }],
      limit: 10,
    };
    const plan = builder.buildPlan(ast);
    
    assert.equal(plan.type, 'Limit');
    assert.equal(plan.count, 10);
    assert.equal(plan.estimatedRows, 10);
    assert.equal(plan.children[0].type, 'Sort');
    assert.equal(plan.children[0].children[0].type, 'Seq Scan');
  });

  it('builds plan for DISTINCT', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const ast = {
      type: 'SELECT',
      distinct: true,
      columns: [{ type: 'column_ref', column: 'status' }],
      from: { table: 'orders' },
    };
    const plan = builder.buildPlan(ast);
    assert.equal(plan.type, 'Unique');
    assert.ok(plan.children[0].type === 'Seq Scan');
  });

  it('full pipeline: build + format', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const ast = {
      type: 'SELECT',
      columns: [{ type: 'star' }],
      from: { table: 'orders' },
      joins: [{
        type: 'INNER',
        table: { table: 'users' },
        on: {
          type: 'binary', operator: '=',
          left: { type: 'column_ref', table: 'orders', column: 'user_id' },
          right: { type: 'column_ref', table: 'users', column: 'id' },
        },
      }],
      orderBy: [{ column: 'total', direction: 'DESC' }],
      limit: 10,
    };
    
    const plan = builder.buildPlan(ast);
    const lines = PlanFormatter.format(plan);
    
    // Should produce a readable tree
    assert.ok(lines.length >= 5);
    assert.ok(lines[0].includes('Limit'));
    assert.ok(lines.some(l => l.includes('Sort')));
    assert.ok(lines.some(l => l.includes('Hash Join')));
    assert.ok(lines.some(l => l.includes('Seq Scan on orders')));
    assert.ok(lines.some(l => l.includes('Seq Scan on users')));
    
    // Also test JSON format
    const json = PlanFormatter.toJSON(plan);
    assert.equal(json['Node Type'], 'Limit');
    assert.ok(json['Plans']);
  });

  it('formats with EXPLAIN ANALYZE actuals', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const ast = {
      type: 'SELECT',
      columns: [{ type: 'star' }],
      from: { table: 'users' },
      where: { type: 'binary', operator: '>', left: { type: 'column_ref', column: 'age' }, right: { type: 'number', value: 50 } },
    };
    
    const plan = builder.buildPlan(ast);
    // Simulate execution actuals
    plan.setActuals(23, 1.234);
    
    const lines = PlanFormatter.format(plan, { analyze: true });
    assert.ok(lines[0].includes('actual rows=23'));
    assert.ok(lines[0].includes('time=1.234ms'));
  });

  it('handles SELECT without FROM', () => {
    const db = new Database();
    const builder = new PlanBuilder(db);
    const ast = { type: 'SELECT', columns: [{ type: 'number', value: 1 }] };
    const plan = builder.buildPlan(ast);
    assert.equal(plan.type, 'Result');
  });

  it('builds plan for non-SELECT', () => {
    const db = new Database();
    const builder = new PlanBuilder(db);
    const plan = builder.buildPlan({ type: 'INSERT' });
    assert.equal(plan.type, 'Result');
  });

  it('handles HAVING filter', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const ast = {
      type: 'SELECT',
      columns: [
        { type: 'column_ref', column: 'status' },
        { type: 'aggregate', func: 'COUNT', args: [{ type: 'star' }] },
      ],
      from: { table: 'orders' },
      groupBy: ['status'],
      having: { type: 'binary', operator: '>', left: { type: 'aggregate', func: 'COUNT' }, right: { type: 'number', value: 10 } },
    };
    const plan = builder.buildPlan(ast);
    assert.equal(plan.type, 'Filter');
    assert.equal(plan.children[0].type, 'Aggregate');
  });

  it('handles window functions', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const ast = {
      type: 'SELECT',
      columns: [
        { type: 'column_ref', column: 'id' },
        { type: 'window', func: 'ROW_NUMBER', over: { orderBy: [{ column: 'total' }] } },
      ],
      from: { table: 'orders' },
    };
    const plan = builder.buildPlan(ast);
    assert.equal(plan.type, 'WindowAgg');
    assert.equal(plan.children[0].type, 'Seq Scan');
  });
});

describe('PlanBuilder - cost model sanity', () => {
  function makeDB() {
    const db = new Database();
    db.execute('CREATE TABLE big (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO big VALUES (${i}, ${i})`);
    return db;
  }

  it('seq scan cost scales with row count', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const plan = builder.buildPlan({
      type: 'SELECT', columns: [{ type: 'star' }], from: { table: 'big' },
    });
    assert.ok(plan.estimatedCost > 5); // ~1000 rows should have non-trivial cost
    assert.ok(plan.estimatedRows >= 990 && plan.estimatedRows <= 1000); // ~1000 rows
  });

  it('sort cost is higher than scan cost', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const plan = builder.buildPlan({
      type: 'SELECT', columns: [{ type: 'star' }], from: { table: 'big' },
      orderBy: [{ column: 'val', direction: 'ASC' }],
    });
    const scanCost = plan.children[0].estimatedCost;
    assert.ok(plan.estimatedCost > scanCost); // sort adds cost
  });

  it('limit reduces estimated rows', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const plan = builder.buildPlan({
      type: 'SELECT', columns: [{ type: 'star' }], from: { table: 'big' },
      limit: 5,
    });
    assert.equal(plan.estimatedRows, 5);
    assert.equal(plan.type, 'Limit');
  });

  it('WHERE reduces estimated rows via selectivity', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const plan = builder.buildPlan({
      type: 'SELECT', columns: [{ type: 'star' }], from: { table: 'big' },
      where: { type: 'binary', operator: '=', left: { type: 'column_ref', column: 'val' }, right: { type: 'number', value: 42 } },
    });
    assert.ok(plan.estimatedRows < 1000); // selectivity applied
  });

  it('AND condition has lower selectivity than single condition', () => {
    const db = makeDB();
    const builder = new PlanBuilder(db);
    const singleWhere = {
      type: 'binary', operator: '>', left: { type: 'column_ref', column: 'val' }, right: { type: 'number', value: 500 },
    };
    const andWhere = {
      type: 'AND',
      left: singleWhere,
      right: { type: 'binary', operator: '<', left: { type: 'column_ref', column: 'val' }, right: { type: 'number', value: 800 } },
    };
    
    const planSingle = builder.buildPlan({
      type: 'SELECT', columns: [{ type: 'star' }], from: { table: 'big' }, where: singleWhere,
    });
    const planAnd = builder.buildPlan({
      type: 'SELECT', columns: [{ type: 'star' }], from: { table: 'big' }, where: andWhere,
    });
    
    assert.ok(planAnd.estimatedRows < planSingle.estimatedRows);
  });
});

describe('PlanFormatter - DOT output', () => {
  it('generates valid DOT graph', () => {
    const scan = new SeqScanNode('users', { estimatedRows: 100, estimatedCost: 10 });
    const dot = PlanFormatter.toDOT(scan);
    assert.ok(dot.includes('digraph QueryPlan'));
    assert.ok(dot.includes('Seq Scan on users'));
    assert.ok(dot.includes('rows=100'));
  });

  it('generates edges for join tree', () => {
    const left = new SeqScanNode('orders', { estimatedRows: 500 });
    const right = new SeqScanNode('users', { estimatedRows: 100 });
    const hash = new HashNode({ estimatedRows: 100 });
    hash.addChild(right);
    const join = new HashJoinNode('INNER', 'orders.user_id = users.id', { estimatedRows: 500 });
    join.addChild(left);
    join.addChild(hash);
    
    const dot = PlanFormatter.toDOT(join);
    assert.ok(dot.includes('->'), 'Should have edges');
    assert.ok(dot.includes('Hash Join'));
    assert.ok(dot.includes('orders'));
    assert.ok(dot.includes('users'));
    assert.ok(dot.includes('fillcolor'));
  });

  it('shows filter in DOT label', () => {
    const scan = new SeqScanNode('t', { estimatedRows: 50 });
    scan.filter = 'age > 21';
    const dot = PlanFormatter.toDOT(scan);
    assert.ok(dot.includes('age > 21'));
  });
});

describe('PlanFormatter - YAML output', () => {
  it('generates valid YAML', () => {
    const scan = new SeqScanNode('users', { estimatedRows: 100, estimatedCost: 10 });
    scan.filter = 'active = 1';
    const yaml = PlanFormatter.toYAML(scan);
    assert.ok(yaml.includes('Node Type:'));
    assert.ok(yaml.includes('Seq Scan on users'));
    assert.ok(yaml.includes('Plan Rows: 100'));
    assert.ok(yaml.includes('Filter: "active = 1"'));
  });

  it('nests children under Plans:', () => {
    const scan = new SeqScanNode('t', { estimatedRows: 50 });
    const sort = new SortNode([{ column: 'id', direction: 'ASC' }], { estimatedRows: 50 });
    sort.addChild(scan);
    const yaml = PlanFormatter.toYAML(sort);
    assert.ok(yaml.includes('Plans:'));
    assert.ok(yaml.includes('Sort Key:'));
    assert.ok(yaml.includes('Seq Scan'));
  });
});
