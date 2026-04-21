/**
 * TransactionalMVCCHeap — Clean MVCC wrapper for TransactionalDatabase.
 * 
 * Replaces the monkey-patching approach in _installScanInterceptors().
 * Provides MVCC-aware scan(), delete(), findByPK(), and get() methods
 * while preserving access to the physical heap via physical*() methods.
 * 
 * Fragility risks eliminated:
 * 1. Prototype pollution — no longer mutating heap objects
 * 2. Method identity — wrapped methods are class methods, not closures
 * 3. Re-wrapping — instanceof check replaces _mvccWrapped flag
 * 4. Original method access — physicalScan/physicalDelete always available
 * 5. Stale closure refs — tdb/versionMap accessed dynamically, not captured
 */
export class TransactionalMVCCHeap {
  /**
   * @param {Object} physicalHeap - The underlying heap (HeapFile, BTreeTable, etc.)
   * @param {string} tableName - Table name for version map lookups
   * @param {Object} tdb - TransactionalDatabase reference
   */
  constructor(physicalHeap, tableName, tdb) {
    this._physical = physicalHeap;
    this._tableName = tableName;
    this._tdb = tdb;
    this._mvccWrapped = true;

    // Compute PK column indices once
    const tableObj = tdb._db.tables.get(tableName);
    this._pkIndices = [];
    if (tableObj && tableObj.schema) {
      for (let i = 0; i < tableObj.schema.length; i++) {
        if (tableObj.schema[i].primaryKey) this._pkIndices.push(i);
      }
    }

    this._syntheticPageSize = physicalHeap._syntheticPageSize || 1000;
  }

  // --- PK helpers ---

  _extractPK(values) {
    if (this._pkIndices.length === 0) return null;
    if (this._pkIndices.length === 1) return values[this._pkIndices[0]];
    return this._pkIndices.map(i => String(values[i])).join('\0');
  }

  _dedupByPK(rows) {
    if (this._pkIndices.length === 0) return rows;
    const pkMap = new Map();
    const result = [];
    for (const row of rows) {
      if (!row.values) { result.push(row); continue; }
      const pk = this._extractPK(row.values);
      if (pk == null) { result.push(row); continue; }
      if (pkMap.has(pk)) {
        result[pkMap.get(pk)] = null;
      }
      pkMap.set(pk, result.length);
      result.push(row);
    }
    return result.filter(r => r !== null);
  }

  // --- Physical access (for VACUUM, recovery, internal use) ---

  physicalScan() { return this._physical.scan(); }
  physicalDelete(pageId, slotIdx) { return this._physical.delete(pageId, slotIdx); }
  physicalGet(pageId, slotIdx) {
    return typeof this._physical.get === 'function' ? this._physical.get(pageId, slotIdx) : null;
  }
  physicalFindByPK(pkValue) {
    return typeof this._physical.findByPK === 'function' ? this._physical.findByPK(pkValue) : null;
  }

  // Backward compat: code that accesses heap._origScan etc.
  get _origScan() { return () => this._physical.scan(); }
  get _origDelete() { return (p, s) => this._physical.delete(p, s); }
  get _origGet() { return (p, s) => this.physicalGet(p, s); }
  get _origFindByPK() { return (pk) => this.physicalFindByPK(pk); }

  // --- MVCC-aware scan ---

  *scan() {
    const tx = this._tdb._activeTx;
    const vm = this._tdb._versionMaps.get(this._tableName);
    const visMap = this._tdb._visibilityMap;

    if (!tx) {
      const visibleRows = [];
      for (const row of this._physical.scan()) {
        if (vm) {
          const key = `${row.pageId}:${row.slotIdx}`;
          const ver = vm.get(key);
          if (ver && ver.xmax !== 0 && (this._tdb._mvcc.committedTxns.has(ver.xmax) || ver.xmax === -1)) {
            continue;
          }
        }
        visibleRows.push(row);
      }
      yield* this._dedupByPK(visibleRows);
      return;
    }

    const visibleRows = [];
    for (const row of this._physical.scan()) {
      if (!vm) { visibleRows.push(row); continue; }

      // Visibility map optimization
      if (visMap.isAllVisible(this._tableName, row.pageId)) {
        if (this._tdb._mvcc.recordRead && !tx.suppressReadTracking) {
          const key = `${row.pageId}:${row.slotIdx}`;
          this._tdb._mvcc.recordRead(tx.txId, `${this._tableName}:${key}`, 0);
        }
        visibleRows.push(row);
        continue;
      }

      const key = `${row.pageId}:${row.slotIdx}`;
      const ver = vm.get(key);

      if (!ver) {
        if (this._tdb._mvcc.recordRead && !tx.suppressReadTracking) {
          this._tdb._mvcc.recordRead(tx.txId, `${this._tableName}:${key}`, 0);
        }
        visibleRows.push(row);
        continue;
      }

      const created = this._tdb._mvcc.isVisible(ver.xmin, tx);
      const deleted = ver.xmax !== 0 && this._tdb._mvcc.isVisible(ver.xmax, tx);

      if (created && !deleted) {
        if (this._tdb._mvcc.recordRead && !tx.suppressReadTracking) {
          this._tdb._mvcc.recordRead(tx.txId, `${this._tableName}:${key}`, ver.xmin);
        }
        visibleRows.push(row);
      }
    }
    yield* this._dedupByPK(visibleRows);
  }

  // --- MVCC-aware delete ---

  delete(pageId, slotIdx) {
    const tx = this._tdb._activeTx;
    const vm = this._tdb._versionMaps.get(this._tableName);

    if (!tx) {
      if (vm) {
        const key = `${pageId}:${slotIdx}`;
        const ver = vm.get(key);
        if (ver) ver.xmax = -1;
      }
      return this._physical.delete(pageId, slotIdx);
    }

    if (vm) {
      const key = `${pageId}:${slotIdx}`;
      const ver = vm.get(key);
      if (ver) {
        // Write-write conflict
        if (ver.xmax !== 0 && ver.xmax !== tx.txId) {
          const otherTx = tx.manager.activeTxns.get(ver.xmax);
          if (otherTx && !otherTx.committed && !otherTx.aborted) {
            throw new Error(`Write-write conflict on ${this._tableName}:${key}`);
          }
        }

        // PK-level write-write conflict check
        if (this._pkIndices.length > 0) {
          const currentValues = this.physicalGet(pageId, slotIdx);
          if (currentValues) {
            const vals = Array.isArray(currentValues) ? currentValues : (currentValues.values || currentValues);
            const currentPK = this._extractPK(vals);
            if (currentPK != null) {
              for (const [otherKey, otherVer] of vm) {
                if (otherKey === key) continue;
                if (otherVer.xmax !== 0 && otherVer.xmax !== tx.txId) {
                  const otherTx2 = tx.manager.activeTxns.get(otherVer.xmax);
                  if (otherTx2 && !otherTx2.committed && !otherTx2.aborted) {
                    const [otherPageId, otherSlotIdx] = otherKey.split(':').map(Number);
                    const otherValues = this.physicalGet(otherPageId, otherSlotIdx);
                    if (otherValues) {
                      const otherVals = Array.isArray(otherValues) ? otherValues : (otherValues.values || otherValues);
                      const otherPK = this._extractPK(otherVals);
                      if (otherPK != null && otherPK === currentPK) {
                        throw new Error(`Write-write conflict on ${this._tableName}:${key} (PK-level: same logical row modified by tx ${otherVer.xmax})`);
                      }
                    }
                  }
                }
              }
            }
          }
        }

        const oldXmax = ver.xmax;
        ver.xmax = tx.txId;
        tx.writeSet.add(`${this._tableName}:${key}:del`);
        this._tdb._visibilityMap.onPageModified(this._tableName, pageId);
        if (this._tdb._mvcc.recordWrite) {
          this._tdb._mvcc.recordWrite(tx.txId, `${this._tableName}:${key}`);
        }
        tx.undoLog.push(() => { ver.xmax = oldXmax; });
      } else {
        const newVer = { xmin: 1, xmax: tx.txId };
        vm.set(key, newVer);
        tx.writeSet.add(`${this._tableName}:${key}:del`);
        this._tdb._visibilityMap.onPageModified(this._tableName, pageId);
        if (this._tdb._mvcc.recordWrite) {
          this._tdb._mvcc.recordWrite(tx.txId, `${this._tableName}:${key}`);
        }
        tx.undoLog.push(() => { vm.delete(key); });
      }
    }
  }

  // --- MVCC-aware findByPK ---

  findByPK(pkValue) {
    if (typeof this._physical.findByPK !== 'function') return null;

    const values = this._physical.findByPK(pkValue);
    if (!values) {
      // B+tree doesn't have this key - scan for older version
      for (const row of this.scan()) {
        const rowValues = row.values || row;
        const pk = this._pkIndices.length === 1 ? rowValues[this._pkIndices[0]] :
          this._pkIndices.map(i => String(rowValues[i])).join('\0');
        if (pk === pkValue) return rowValues;
      }
      return null;
    }

    const vm = this._tdb._versionMaps.get(this._tableName);
    if (!vm) return values;

    const pkToRid = this._physical._pkToRid;
    if (pkToRid) {
      const rid = pkToRid.get(pkValue);
      if (rid !== undefined) {
        const ridNum = typeof rid === 'number' ? rid : (rid.pageId * this._syntheticPageSize + rid.slotIdx);
        const pageId = Math.floor(ridNum / this._syntheticPageSize);
        const slotIdx = ridNum % this._syntheticPageSize;
        const key = `${pageId}:${slotIdx}`;
        const ver = vm.get(key);

        if (ver) {
          const tx = this._tdb._activeTx;
          if (tx) {
            const created = this._tdb._mvcc.isVisible(ver.xmin, tx);
            const deleted = ver.xmax !== 0 && this._tdb._mvcc.isVisible(ver.xmax, tx);
            if (!created || deleted) {
              for (const row of this.scan()) {
                const rowValues = row.values || row;
                const pk = this._pkIndices.length === 1 ? rowValues[this._pkIndices[0]] :
                  this._pkIndices.map(i => String(rowValues[i])).join('\0');
                if (pk === pkValue) return rowValues;
              }
              return null;
            }
          } else {
            if (ver.xmax !== 0 && this._tdb._mvcc.committedTxns.has(ver.xmax)) return null;
          }
        }
      }
    }
    return values;
  }

  // --- MVCC-aware get ---

  get(pageId, slotIdx) {
    if (typeof this._physical.get !== 'function') return null;

    const values = this._physical.get(pageId, slotIdx);
    if (!values) return null;

    const vm = this._tdb._versionMaps.get(this._tableName);
    if (!vm) return values;

    const key = `${pageId}:${slotIdx}`;
    const ver = vm.get(key);
    if (ver) {
      const tx = this._tdb._activeTx;
      if (tx) {
        const created = this._tdb._mvcc.isVisible(ver.xmin, tx);
        const deleted = ver.xmax !== 0 && this._tdb._mvcc.isVisible(ver.xmax, tx);
        if (!created || deleted) return null;
      } else {
        if (ver.xmax !== 0 && this._tdb._mvcc.committedTxns.has(ver.xmax)) return null;
      }
    }
    return values;
  }

  // --- Forwarded insert (physical, not MVCC-intercepted) ---

  insert(values) { return this._physical.insert(values); }
  update(pageId, slotIdx, values) {
    if (typeof this._physical.update === 'function') {
      return this._physical.update(pageId, slotIdx, values);
    }
  }
}

/**
 * Create a Proxy-wrapped TransactionalMVCCHeap for full backward compatibility.
 * Unknown property accesses are forwarded to the physical heap.
 */
export function createTransactionalMVCCHeap(physicalHeap, tableName, tdb) {
  const mvccHeap = new TransactionalMVCCHeap(physicalHeap, tableName, tdb);

  return new Proxy(mvccHeap, {
    get(target, prop, receiver) {
      // Symbol properties
      if (typeof prop === 'symbol') return Reflect.get(target, prop, receiver);
      
      // Check MVCCHeap instance first
      if (prop in target) {
        const val = Reflect.get(target, prop, receiver);
        return val;
      }
      // Check prototype
      if (prop in TransactionalMVCCHeap.prototype) {
        const val = TransactionalMVCCHeap.prototype[prop];
        if (typeof val === 'function') return val.bind(target);
        return val;
      }
      // Forward to physical heap
      const physVal = target._physical[prop];
      if (typeof physVal === 'function') return physVal.bind(target._physical);
      return physVal;
    },
    set(target, prop, value) {
      if (prop in target) {
        target[prop] = value;
      } else {
        target._physical[prop] = value;
      }
      return true;
    },
    has(target, prop) {
      return prop in target || prop in target._physical;
    }
  });
}
