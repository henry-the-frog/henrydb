#!/usr/bin/env node
// henrydb-server.js — PostgreSQL wire protocol server for HenryDB
// Usage: node henrydb-server.js [port] [dbdir]
//
// Connects with psql: psql -h localhost -p 5433 -U henrydb

import { createServer } from 'node:net';
import { Database } from './src/db.js';
import {
  writeAuthenticationOk, writeParameterStatus, writeBackendKeyData,
  writeReadyForQuery, writeRowDescription, writeDataRow,
  writeCommandComplete, writeErrorResponse,
  parseStartupMessage, parseQueryMessage, inferTypeOid,
} from './src/pg-protocol.js';

const PORT = parseInt(process.argv[2]) || 5433;
const db = new Database();

// Pre-populate with some test data
db.execute('CREATE TABLE demo (id INT PRIMARY KEY, name TEXT, value INT)');
for (let i = 0; i < 10; i++) {
  db.execute(`INSERT INTO demo VALUES (${i}, 'item_${i}', ${i * 100})`);
}

console.log(`HenryDB PostgreSQL Server listening on port ${PORT}`);
console.log(`Connect with: psql -h localhost -p ${PORT} -U henrydb`);
console.log(`Test table 'demo' has 10 rows.\n`);

let connId = 0;

const server = createServer((socket) => {
  const id = ++connId;
  console.log(`[${id}] Client connected from ${socket.remoteAddress}:${socket.remotePort}`);
  
  let state = 'init'; // init → startup → ready
  let buffer = Buffer.alloc(0);
  
  socket.on('data', (data) => {
    buffer = Buffer.concat([buffer, data]);
    processBuffer();
  });
  
  socket.on('error', (err) => {
    console.log(`[${id}] Socket error: ${err.message}`);
  });
  
  socket.on('close', () => {
    console.log(`[${id}] Client disconnected`);
  });
  
  function processBuffer() {
    while (buffer.length >= 4) {
      if (state === 'init') {
        // First message: could be SSL request or StartupMessage
        // SSL request: [Int32 len=8] [Int32 code=80877103]
        if (buffer.length >= 8) {
          const len = buffer.readInt32BE(0);
          const code = buffer.readInt32BE(4);
          
          if (code === 80877103) {
            // SSL request — reject with 'N'
            console.log(`[${id}] SSL request — rejecting`);
            socket.write(Buffer.from('N'));
            buffer = buffer.subarray(len);
            continue;
          }
          
          // StartupMessage
          if (buffer.length >= len) {
            const startupBuf = buffer.subarray(0, len);
            buffer = buffer.subarray(len);
            handleStartup(startupBuf);
            continue;
          }
        }
        break; // Need more data
      }
      
      if (state === 'ready') {
        // Messages: [byte type] [Int32 length] [payload]
        if (buffer.length < 5) break;
        
        const msgType = buffer[0];
        const msgLen = buffer.readInt32BE(1);
        const totalLen = 1 + msgLen;
        
        if (buffer.length < totalLen) break;
        
        const msgBuf = buffer.subarray(1, totalLen); // After type byte
        buffer = buffer.subarray(totalLen);
        
        handleMessage(msgType, msgBuf);
      }
    }
  }
  
  function handleStartup(buf) {
    const startup = parseStartupMessage(buf);
    console.log(`[${id}] Startup: protocol ${startup.protocolVersion.major}.${startup.protocolVersion.minor}, user=${startup.params.user || 'unknown'}`);
    
    // Send auth OK + parameters
    socket.write(writeAuthenticationOk());
    socket.write(writeParameterStatus('server_version', '15.0 (HenryDB)'));
    socket.write(writeParameterStatus('server_encoding', 'UTF8'));
    socket.write(writeParameterStatus('client_encoding', 'UTF8'));
    socket.write(writeParameterStatus('DateStyle', 'ISO, MDY'));
    socket.write(writeParameterStatus('integer_datetimes', 'on'));
    socket.write(writeBackendKeyData(process.pid, id));
    socket.write(writeReadyForQuery('I'));
    
    state = 'ready';
  }
  
  function handleMessage(type, buf) {
    const typeChar = String.fromCharCode(type);
    
    switch (typeChar) {
      case 'Q': { // Query
        const query = parseQueryMessage(buf);
        console.log(`[${id}] Query: ${query.substring(0, 100)}${query.length > 100 ? '...' : ''}`);
        handleQuery(query);
        break;
      }
      case 'X': { // Terminate
        console.log(`[${id}] Client terminated`);
        socket.end();
        break;
      }
      default:
        console.log(`[${id}] Unknown message type: ${typeChar} (0x${type.toString(16)})`);
        break;
    }
  }
  
  function handleQuery(sql) {
    try {
      const result = db.execute(sql);
      
      if (result && result.rows && result.rows.length > 0) {
        // SELECT result — send RowDescription + DataRows
        const cols = Object.keys(result.rows[0]);
        const firstRow = result.rows[0];
        
        const colDescs = cols.map(name => ({
          name,
          typeOid: inferTypeOid(firstRow[name]),
          typeSize: typeof firstRow[name] === 'number' ? 4 : -1,
        }));
        
        socket.write(writeRowDescription(colDescs));
        
        for (const row of result.rows) {
          const values = cols.map(c => row[c]);
          socket.write(writeDataRow(values));
        }
        
        socket.write(writeCommandComplete(`SELECT ${result.rows.length}`));
      } else if (result && result.type === 'PLAN') {
        // EXPLAIN result
        const plan = JSON.stringify(result.plan, null, 2);
        socket.write(writeRowDescription([{ name: 'QUERY PLAN' }]));
        for (const line of plan.split('\n')) {
          socket.write(writeDataRow([line]));
        }
        socket.write(writeCommandComplete('EXPLAIN'));
      } else {
        // DDL or DML result
        const tag = result?.message || 'OK';
        socket.write(writeCommandComplete(tag));
      }
    } catch (err) {
      console.log(`[${id}] Error: ${err.message}`);
      socket.write(writeErrorResponse('ERROR', '42000', err.message));
    }
    
    socket.write(writeReadyForQuery('I'));
  }
});

server.listen(PORT, '127.0.0.1');

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down...');
  server.close();
  process.exit(0);
});
