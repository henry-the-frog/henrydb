#!/usr/bin/env node
// henrydb-txn-server.js — PostgreSQL wire protocol server with ACID transactions
// Each TCP connection gets its own TransactionSession with snapshot isolation.
//
// Usage: node henrydb-txn-server.js [port] [dbdir]
// Connect: psql -h localhost -p 5434 -U henrydb

import { createServer } from 'node:net';
import { TransactionalDatabase } from './src/transactional-db.js';
import { buildPlan, explainPlan } from './src/volcano-planner.js';
import { parse } from './src/sql.js';
import {
  writeAuthenticationOk, writeParameterStatus, writeBackendKeyData,
  writeReadyForQuery, writeRowDescription, writeDataRow,
  writeCommandComplete, writeErrorResponse,
  parseStartupMessage, parseQueryMessage, inferTypeOid,
} from './src/pg-protocol.js';

const PORT = parseInt(process.argv[2]) || 5434;
const DB_DIR = process.argv[3] || './henrydb-data';

const db = TransactionalDatabase.open(DB_DIR);

console.log(`HenryDB Transactional Server listening on port ${PORT}`);
console.log(`Data directory: ${DB_DIR}`);
console.log(`Connect with: psql -h localhost -p ${PORT} -U henrydb\n`);

let connId = 0;

const server = createServer((socket) => {
  const id = ++connId;
  const session = db.session();
  let txState = 'I'; // I=idle, T=in transaction, E=failed transaction
  
  console.log(`[${id}] Client connected`);
  
  let state = 'init';
  let buffer = Buffer.alloc(0);
  
  socket.on('data', (data) => {
    buffer = Buffer.concat([buffer, data]);
    processBuffer();
  });
  
  socket.on('error', (err) => {
    console.log(`[${id}] Socket error: ${err.message}`);
  });
  
  socket.on('close', () => {
    session.close();
    console.log(`[${id}] Client disconnected`);
  });
  
  function processBuffer() {
    while (buffer.length >= 4) {
      if (state === 'init') {
        if (buffer.length >= 8) {
          const len = buffer.readInt32BE(0);
          const code = buffer.readInt32BE(4);
          
          if (code === 80877103) {
            socket.write(Buffer.from('N'));
            buffer = buffer.subarray(len);
            continue;
          }
          
          if (buffer.length >= len) {
            buffer = buffer.subarray(len);
            handleStartup();
            continue;
          }
        }
        break;
      }
      
      if (state === 'ready') {
        if (buffer.length < 5) break;
        const msgType = buffer[0];
        const msgLen = buffer.readInt32BE(1);
        const totalLen = 1 + msgLen;
        if (buffer.length < totalLen) break;
        const msgBuf = buffer.subarray(1, totalLen);
        buffer = buffer.subarray(totalLen);
        handleMessage(msgType, msgBuf);
      }
    }
  }
  
  function handleStartup() {
    socket.write(writeAuthenticationOk());
    socket.write(writeParameterStatus('server_version', '15.0 (HenryDB-Txn)'));
    socket.write(writeParameterStatus('server_encoding', 'UTF8'));
    socket.write(writeParameterStatus('client_encoding', 'UTF8'));
    socket.write(writeParameterStatus('DateStyle', 'ISO, MDY'));
    socket.write(writeParameterStatus('integer_datetimes', 'on'));
    socket.write(writeBackendKeyData(process.pid, id));
    socket.write(writeReadyForQuery(txState));
    state = 'ready';
  }
  
  function handleMessage(type, buf) {
    const typeChar = String.fromCharCode(type);
    switch (typeChar) {
      case 'Q': {
        const query = parseQueryMessage(buf);
        console.log(`[${id}] Query: ${query.substring(0, 100)}`);
        handleQuery(query);
        break;
      }
      case 'X': {
        session.close();
        socket.end();
        break;
      }
      default:
        console.log(`[${id}] Unknown message type: ${typeChar}`);
        break;
    }
  }
  
  function handleQuery(sql) {
    const trimmed = sql.trim().toUpperCase();
    
    try {
      // Handle EXPLAIN queries via volcano engine
      if (trimmed.startsWith('EXPLAIN ANALYZE')) {
        const innerSql = sql.trim().replace(/^EXPLAIN\s+ANALYZE\s+/i, '');
        handleExplainAnalyze(innerSql);
        return;
      }
      if (trimmed.startsWith('EXPLAIN')) {
        const innerSql = sql.trim().replace(/^EXPLAIN\s+/i, '');
        handleExplain(innerSql);
        return;
      }
      
      const result = session.execute(sql);
      
      // Update transaction state
      if (trimmed === 'BEGIN' || trimmed === 'BEGIN TRANSACTION' || trimmed === 'START TRANSACTION') {
        txState = 'T';
      } else if (trimmed === 'COMMIT' || trimmed === 'ROLLBACK' || trimmed === 'ABORT') {
        txState = 'I';
      }
      
      if (result && result.rows && result.rows.length > 0) {
        const cols = Object.keys(result.rows[0]);
        const colDescs = cols.map(name => ({
          name,
          typeOid: inferTypeOid(result.rows[0][name]),
          typeSize: typeof result.rows[0][name] === 'number' ? 4 : -1,
        }));
        socket.write(writeRowDescription(colDescs));
        for (const row of result.rows) {
          socket.write(writeDataRow(cols.map(c => row[c])));
        }
        socket.write(writeCommandComplete(`SELECT ${result.rows.length}`));
      } else if (result && result.rows && result.rows.length === 0) {
        // Empty result set
        socket.write(writeRowDescription([]));
        socket.write(writeCommandComplete('SELECT 0'));
      } else {
        socket.write(writeCommandComplete(result?.message || 'OK'));
      }
    } catch (err) {
      console.log(`[${id}] Error: ${err.message}`);
      socket.write(writeErrorResponse('ERROR', '42000', err.message));
      if (txState === 'T') txState = 'E'; // Transaction failed
    }
    
    socket.write(writeReadyForQuery(txState));
  }
  
  function handleExplain(sql) {
    try {
      const ast = parse(sql);
      const plan = explainPlan(ast, db._db.tables, db._db.indexCatalog);
      // Return plan as rows
      const lines = plan.split('\n');
      socket.write(writeRowDescription([{
        name: 'QUERY PLAN',
        typeOid: 25, // TEXT
        typeSize: -1,
      }]));
      for (const line of lines) {
        socket.write(writeDataRow([line]));
      }
      socket.write(writeCommandComplete(`EXPLAIN ${lines.length}`));
    } catch (err) {
      socket.write(writeErrorResponse('ERROR', '42000', err.message));
    }
    socket.write(writeReadyForQuery(txState));
  }
  
  function handleExplainAnalyze(sql) {
    try {
      const ast = parse(sql);
      const plan = buildPlan(ast, db._db.tables, db._db.indexCatalog);
      
      // Run volcano engine
      const vStart = performance.now();
      const vRows = plan.toArray();
      const vElapsed = (performance.now() - vStart).toFixed(2);
      
      // Run standard engine
      const sStart = performance.now();
      const sResult = session.execute(sql);
      const sElapsed = (performance.now() - sStart).toFixed(2);
      
      // Build output
      const planStr = explainPlan(ast, db._db.tables, db._db.indexCatalog);
      const lines = [
        ...planStr.split('\n'),
        '',
        `Volcano: ${vRows.length} rows, ${vElapsed}ms`,
        `Standard: ${sResult?.rows?.length || 0} rows, ${sElapsed}ms`,
        `Speedup: ${(parseFloat(sElapsed) / parseFloat(vElapsed)).toFixed(1)}x`,
      ];
      
      socket.write(writeRowDescription([{
        name: 'QUERY PLAN',
        typeOid: 25,
        typeSize: -1,
      }]));
      for (const line of lines) {
        socket.write(writeDataRow([line]));
      }
      socket.write(writeCommandComplete(`EXPLAIN ${lines.length}`));
    } catch (err) {
      socket.write(writeErrorResponse('ERROR', '42000', err.message));
    }
    socket.write(writeReadyForQuery(txState));
  }
});

server.listen(PORT, '127.0.0.1');

process.on('SIGINT', () => {
  console.log('\nShutting down...');
  db.close();
  server.close();
  process.exit(0);
});
