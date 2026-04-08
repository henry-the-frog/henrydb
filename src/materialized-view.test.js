// materialized-view.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MaterializedViewManager } from './materialized-view.js';

describe('MaterializedView', () => {
  it('basic create and read', () => {
    const mgr = new MaterializedViewManager();
    const data = [{ id: 1 }, { id: 2 }];
    mgr.create('test_view', () => [...data], ['base_table'], { refreshMode: 'eager' });
    
    const result = mgr.get('test_view');
    assert.equal(result.length, 2);
  });

  it('lazy refresh on first read', () => {
    const mgr = new MaterializedViewManager();
    let callCount = 0;
    mgr.create('lazy_view', () => { callCount++; return [{ x: 1 }]; }, [], { refreshMode: 'lazy' });
    
    assert.equal(callCount, 0); // Not refreshed yet
    mgr.get('lazy_view');
    assert.equal(callCount, 1); // Refreshed on read
  });

  it('invalidation on table change', () => {
    const mgr = new MaterializedViewManager();
    let counter = 0;
    mgr.create('dep_view', () => [{ val: ++counter }], ['orders'], { refreshMode: 'eager' });
    
    assert.equal(mgr.get('dep_view')[0].val, 1);
    mgr.notifyTableChange('orders');
    assert.equal(mgr.get('dep_view')[0].val, 2); // Refreshed
  });

  it('manual refresh mode', () => {
    const mgr = new MaterializedViewManager();
    let val = 'old';
    mgr.create('manual_view', () => [{ val }], ['t'], { refreshMode: 'manual' });
    
    mgr.get('manual_view'); // First read triggers initial load
    val = 'new';
    mgr.notifyTableChange('t'); // Invalidates but doesn't refresh
    
    // Read returns stale data (manual mode)
    const result = mgr.get('manual_view');
    assert.equal(result[0].val, 'old'); // Still stale
    
    mgr.refresh('manual_view');
    assert.equal(mgr.get('manual_view')[0].val, 'new');
  });

  it('multiple views on same table', () => {
    const mgr = new MaterializedViewManager();
    const data = [{ a: 1 }, { a: 2 }, { a: 3 }];
    
    mgr.create('v1', () => data.filter(r => r.a > 1), ['t'], { refreshMode: 'eager' });
    mgr.create('v2', () => data.length, ['t'], { refreshMode: 'eager' });
    
    mgr.notifyTableChange('t');
    // Both should have refreshed
  });

  it('drop view', () => {
    const mgr = new MaterializedViewManager();
    mgr.create('temp', () => [], []);
    mgr.drop('temp');
    assert.equal(mgr.get('temp'), null);
  });

  it('list views', () => {
    const mgr = new MaterializedViewManager();
    mgr.create('v1', () => [1, 2, 3], [], { refreshMode: 'eager' });
    mgr.create('v2', () => [4, 5], [], { refreshMode: 'lazy' });
    
    const list = mgr.list();
    assert.equal(list.length, 2);
    assert.ok(list.find(v => v.name === 'v1'));
  });

  it('refreshAll', () => {
    const mgr = new MaterializedViewManager();
    let c1 = 0, c2 = 0;
    mgr.create('a', () => [{ c: ++c1 }], [], { refreshMode: 'eager' });
    mgr.create('b', () => [{ c: ++c2 }], [], { refreshMode: 'eager' });
    
    mgr.refreshAll();
    assert.ok(c1 >= 2);
    assert.ok(c2 >= 2);
  });
});
