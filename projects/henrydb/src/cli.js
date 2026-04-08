#!/usr/bin/env node
// cli.js — HenryDB interactive SQL client
// A psql-like REPL that connects to the HenryDB server via PostgreSQL wire protocol.

import net from 'node:net';
import readline from 'node:readline';

const host = process.argv[2] || '127.0.0.1';
const port = parseInt(process.argv[3]) || 5433;

function sendStartup(socket, user = 'henrydb', database = 'henrydb') {
  const params = `user\0${user}\0database\0${database}\0\0`;
  const paramsBuf = Buffer.from(params, 'utf8');
  const len = 4 + 4 + paramsBuf.length;
  const buf = Buffer.alloc(len);
  buf.writeInt32BE(len, 0);
  buf.writeInt32BE(196608, 4);
  paramsBuf.copy(buf, 8);
  socket.write(buf);
}

function sendQuery(socket, sql) {
  const queryBuf = Buffer.from(sql + '\0', 'utf8');
  const len = 4 + queryBuf.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x51;
  buf.writeInt32BE(len, 1);
  queryBuf.copy(buf, 5);
  socket.write(buf);
}

function sendTerminate(socket) {
  const buf = Buffer.alloc(5);
  buf[0] = 0x58;
  buf.writeInt32BE(4, 1);
  socket.write(buf);
}

function parseMessages(buf) {
  const messages = [];
  let offset = 0;
  while (offset < buf.length) {
    if (offset + 5 > buf.length) break;
    const type = String.fromCharCode(buf[offset]);
    const len = buf.readInt32BE(offset + 1);
    const totalLen = 1 + len;
    if (offset + totalLen > buf.length) break;
    messages.push({ type, body: buf.subarray(offset + 1, offset + totalLen) });
    offset += totalLen;
  }
  return messages;
}

function extractColumns(msg) {
  const body = msg.body;
  const fieldCount = body.readInt16BE(4);
  const names = [];
  let off = 6;
  for (let i = 0; i < fieldCount; i++) {
    const nameEnd = body.indexOf(0, off);
    names.push(body.toString('utf8', off, nameEnd));
    off = nameEnd + 1 + 4 + 2 + 4 + 2 + 4 + 2;
  }
  return names;
}

function extractDataRow(msg) {
  const body = msg.body;
  const fieldCount = body.readInt16BE(4);
  const values = [];
  let off = 6;
  for (let i = 0; i < fieldCount; i++) {
    const fieldLen = body.readInt32BE(off);
    off += 4;
    if (fieldLen === -1) { values.push(null); }
    else { values.push(body.toString('utf8', off, off + fieldLen)); off += fieldLen; }
  }
  return values;
}

function formatTable(columns, rows) {
  if (columns.length === 0) return '';
  const widths = columns.map((c, i) => {
    let max = c.length;
    for (const row of rows) {
      const val = row[i] === null ? 'NULL' : String(row[i]);
      if (val.length > max) max = val.length;
    }
    return max;
  });

  const sep = widths.map(w => '-'.repeat(w + 2)).join('+');
  const header = columns.map((c, i) => ` ${c.padEnd(widths[i])} `).join('|');
  const lines = [header, sep];
  for (const row of rows) {
    lines.push(row.map((v, i) => {
      const s = v === null ? 'NULL' : String(v);
      return ` ${s.padEnd(widths[i])} `;
    }).join('|'));
  }
  return lines.join('\n');
}

// Connect
const socket = net.createConnection({ host, port }, () => {
  console.log(`Connected to HenryDB at ${host}:${port}`);
  sendStartup(socket);
});

let buffer = Buffer.alloc(0);
let started = false;
let columns = [];
let rows = [];
let rl;

socket.on('data', (data) => {
  buffer = Buffer.concat([buffer, data]);
  
  // Try to parse complete messages
  const msgs = parseMessages(buffer);
  if (msgs.length === 0) return;

  // Advance buffer past parsed messages
  let consumed = 0;
  for (const msg of msgs) { consumed += 1 + msg.body.length; }
  buffer = buffer.subarray(consumed);

  for (const msg of msgs) {
    switch (msg.type) {
      case 'R': // AuthenticationOk
        break;
      case 'S': // ParameterStatus
        break;
      case 'K': // BackendKeyData
        break;
      case 'Z': // ReadyForQuery
        if (!started) {
          started = true;
          startRepl();
        }
        // Print any accumulated results
        if (columns.length > 0) {
          console.log(formatTable(columns, rows));
          console.log(`(${rows.length} row${rows.length !== 1 ? 's' : ''})\n`);
        }
        columns = [];
        rows = [];
        if (rl) rl.prompt();
        break;
      case 'T': // RowDescription
        columns = extractColumns(msg);
        rows = [];
        break;
      case 'D': // DataRow
        rows.push(extractDataRow(msg));
        break;
      case 'C': { // CommandComplete
        const tag = msg.body.toString('utf8', 4).replace(/\0/g, '');
        if (!tag.startsWith('SELECT')) console.log(tag);
        break;
      }
      case 'E': { // ErrorResponse
        const text = msg.body.toString('utf8', 4);
        const mMatch = text.match(/M([^\0]+)/);
        console.log(`ERROR: ${mMatch ? mMatch[1] : 'Unknown error'}`);
        break;
      }
    }
  }
});

socket.on('error', (err) => {
  console.error(`Connection error: ${err.message}`);
  process.exit(1);
});

socket.on('close', () => {
  console.log('Connection closed.');
  process.exit(0);
});

function startRepl() {
  rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'henrydb> ',
    terminal: true,
  });

  let multiline = '';

  rl.prompt();

  rl.on('line', (line) => {
    const trimmed = line.trim();
    
    if (trimmed === '\\q' || trimmed === 'exit' || trimmed === 'quit') {
      sendTerminate(socket);
      rl.close();
      return;
    }

    if (trimmed === '\\dt') {
      sendQuery(socket, 'SHOW TABLES');
      return;
    }

    multiline += (multiline ? ' ' : '') + line;

    if (multiline.trim().endsWith(';') || /^(BEGIN|COMMIT|ROLLBACK)$/i.test(multiline.trim())) {
      sendQuery(socket, multiline.trim());
      multiline = '';
    } else if (multiline.trim()) {
      process.stdout.write('       -> ');
    }
  });

  rl.on('close', () => {
    sendTerminate(socket);
  });
}
