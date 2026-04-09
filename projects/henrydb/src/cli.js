#!/usr/bin/env node
// cli.js — HenryDB CLI: psql-like REPL for HenryDB
// Connects via PostgreSQL wire protocol to HenryDB server.

import net from 'node:net';
import readline from 'node:readline';

const DEFAULT_HOST = '127.0.0.1';
const DEFAULT_PORT = 5433;
const DEFAULT_USER = 'henrydb';
const DEFAULT_DB = 'henrydb';

// Parse command-line args
function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { host: DEFAULT_HOST, port: DEFAULT_PORT, user: DEFAULT_USER, dbname: DEFAULT_DB };
  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '-h': case '--host': opts.host = args[++i]; break;
      case '-p': case '--port': opts.port = parseInt(args[++i], 10); break;
      case '-U': case '--user': opts.user = args[++i]; break;
      case '-d': case '--dbname': opts.dbname = args[++i]; break;
      case '--help': 
        console.log(`Usage: henrydb-cli [options]
Options:
  -h, --host <host>    Server host (default: ${DEFAULT_HOST})
  -p, --port <port>    Server port (default: ${DEFAULT_PORT})
  -U, --user <user>    Username (default: ${DEFAULT_USER})
  -d, --dbname <name>  Database name (default: ${DEFAULT_DB})
  --help               Show this help`);
        process.exit(0);
    }
  }
  return opts;
}

// PostgreSQL wire protocol helpers
function buildStartupMessage(user, database) {
  const params = Buffer.from(`user\0${user}\0database\0${database}\0\0`);
  const len = 4 + 4 + params.length;
  const buf = Buffer.alloc(len);
  buf.writeInt32BE(len, 0);
  buf.writeInt32BE(196608, 4); // Protocol v3.0
  params.copy(buf, 8);
  return buf;
}

function buildQueryMessage(sql) {
  const sqlBuf = Buffer.from(sql + '\0', 'utf8');
  const len = 4 + sqlBuf.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x51; // 'Q'
  buf.writeInt32BE(len, 1);
  sqlBuf.copy(buf, 5);
  return buf;
}

function buildTerminateMessage() {
  const buf = Buffer.alloc(5);
  buf[0] = 0x58; // 'X'
  buf.writeInt32BE(4, 1);
  return buf;
}

// Format table output like psql
function formatTable(columns, rows) {
  if (!columns || columns.length === 0) return '';
  
  // Calculate column widths
  const widths = columns.map((col, i) => {
    let maxLen = col.length;
    for (const row of rows) {
      const val = row[i] === null ? 'NULL' : String(row[i]);
      maxLen = Math.max(maxLen, val.length);
    }
    return maxLen;
  });
  
  // Header
  const header = columns.map((col, i) => col.padEnd(widths[i])).join(' | ');
  const separator = widths.map(w => '-'.repeat(w)).join('-+-');
  
  // Rows
  const dataRows = rows.map(row =>
    row.map((val, i) => {
      const s = val === null ? 'NULL' : String(val);
      return s.padEnd(widths[i]);
    }).join(' | ')
  );
  
  return [' ' + header, '-' + separator, ...dataRows.map(r => ' ' + r)].join('\n');
}

class HenryDBCli {
  constructor(opts) {
    this.opts = opts;
    this.socket = null;
    this.rl = null;
    this.buffer = Buffer.alloc(0);
    this._resolveQuery = null;
    this._columns = [];
    this._rows = [];
    this._commandTag = '';
    this._error = null;
    this._multilineBuffer = '';
    this._ready = false;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection(this.opts.port, this.opts.host, () => {
        // Send startup message
        this.socket.write(buildStartupMessage(this.opts.user, this.opts.dbname));
      });

      this.socket.on('data', (chunk) => this._handleData(chunk));
      this.socket.on('error', (err) => {
        if (!this._ready) {
          reject(err);
        } else {
          console.error(`\nConnection error: ${err.message}`);
          process.exit(1);
        }
      });
      this.socket.on('close', () => {
        if (!this._ready) {
          reject(new Error('Connection closed before ready'));
        } else {
          console.log('\nConnection closed.');
          process.exit(0);
        }
      });

      // Wait for ReadyForQuery
      this._onReady = () => {
        this._ready = true;
        resolve();
      };
    });
  }

  _handleData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    
    while (this.buffer.length >= 5) {
      const msgType = String.fromCharCode(this.buffer[0]);
      const msgLen = this.buffer.readInt32BE(1);
      const totalLen = 1 + msgLen;
      
      if (this.buffer.length < totalLen) break; // Wait for more data
      
      const msgBody = this.buffer.subarray(5, totalLen);
      this.buffer = this.buffer.subarray(totalLen);
      
      this._handleMessage(msgType, msgBody);
    }
  }

  _handleMessage(type, body) {
    switch (type) {
      case 'R': // Authentication
        this._handleAuth(body);
        break;
      case 'S': // ParameterStatus
        // Ignore for now
        break;
      case 'K': // BackendKeyData
        // Ignore for now
        break;
      case 'Z': // ReadyForQuery
        if (this._onReady) {
          const cb = this._onReady;
          this._onReady = null;
          cb();
        }
        if (this._resolveQuery) {
          const resolve = this._resolveQuery;
          this._resolveQuery = null;
          resolve({
            columns: this._columns,
            rows: this._rows,
            commandTag: this._commandTag,
            error: this._error,
          });
        }
        break;
      case 'T': { // RowDescription
        const numFields = body.readInt16BE(0);
        this._columns = [];
        let offset = 2;
        for (let i = 0; i < numFields; i++) {
          const end = body.indexOf(0, offset);
          const name = body.subarray(offset, end).toString('utf8');
          this._columns.push(name);
          offset = end + 1 + 18; // Skip tableOid(4) + colNum(2) + typeOid(4) + typeSize(2) + typeMod(4) + format(2)
        }
        break;
      }
      case 'D': { // DataRow
        const numCols = body.readInt16BE(0);
        const row = [];
        let offset = 2;
        for (let i = 0; i < numCols; i++) {
          const colLen = body.readInt32BE(offset);
          offset += 4;
          if (colLen === -1) {
            row.push(null);
          } else {
            row.push(body.subarray(offset, offset + colLen).toString('utf8'));
            offset += colLen;
          }
        }
        this._rows.push(row);
        break;
      }
      case 'C': { // CommandComplete
        const end = body.indexOf(0);
        this._commandTag = body.subarray(0, end).toString('utf8');
        break;
      }
      case 'E': { // ErrorResponse
        // Parse error fields
        let offset = 0;
        let message = '';
        while (offset < body.length) {
          const fieldType = String.fromCharCode(body[offset]);
          offset++;
          if (fieldType === '\0') break;
          const end = body.indexOf(0, offset);
          const val = body.subarray(offset, end).toString('utf8');
          offset = end + 1;
          if (fieldType === 'M') message = val;
        }
        this._error = message || 'Unknown error';
        break;
      }
      case 'I': // EmptyQueryResponse
        break;
      case 'N': // NoticeResponse
        break;
      default:
        // Unknown message type — skip
        break;
    }
  }

  _handleAuth(body) {
    const authType = body.readInt32BE(0);
    if (authType === 0) {
      // AuthenticationOk — nothing to do
    } else if (authType === 5) {
      // MD5 password — not supported in CLI yet
      console.error('MD5 authentication not yet supported in CLI');
      process.exit(1);
    }
  }

  query(sql) {
    return new Promise((resolve) => {
      this._columns = [];
      this._rows = [];
      this._commandTag = '';
      this._error = null;
      this._resolveQuery = resolve;
      this.socket.write(buildQueryMessage(sql));
    });
  }

  async startRepl() {
    this.rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      prompt: 'henrydb> ',
      terminal: true,
    });

    console.log(`HenryDB CLI — Connected to ${this.opts.host}:${this.opts.port}`);
    console.log('Type "help" for help, "\\q" to quit.\n');

    this.rl.prompt();

    for await (const line of this.rl) {
      const trimmed = line.trim();

      // Handle meta-commands
      if (trimmed === '\\q' || trimmed === 'quit' || trimmed === 'exit') {
        this.socket.write(buildTerminateMessage());
        this.socket.end();
        process.exit(0);
      }

      if (trimmed === 'help' || trimmed === '\\?') {
        console.log(`
HenryDB CLI Commands:
  \\q, quit, exit    Quit
  \\dt               List tables
  \\di               List indexes
  \\d <table>        Describe table
  \\?                Show this help
  
SQL statements end with a semicolon (;).
Multi-line input is supported.
`);
        this.rl.prompt();
        continue;
      }

      if (trimmed === '\\dt') {
        await this._execAndPrint("SELECT name FROM _tables");
        this.rl.prompt();
        continue;
      }

      if (trimmed === '\\di') {
        await this._execAndPrint("SELECT name, table_name, columns FROM _indexes");
        this.rl.prompt();
        continue;
      }

      if (trimmed.startsWith('\\d ')) {
        const table = trimmed.slice(3).trim();
        await this._execAndPrint(`SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = '${table}'`);
        this.rl.prompt();
        continue;
      }

      // Accumulate SQL (support multi-line)
      this._multilineBuffer += (this._multilineBuffer ? '\n' : '') + line;

      if (!this._multilineBuffer.trim().endsWith(';') && this._multilineBuffer.trim().length > 0) {
        // Wait for semicolon
        this.rl.setPrompt('      -> ');
        this.rl.prompt();
        continue;
      }

      // Execute SQL
      const sql = this._multilineBuffer.trim();
      this._multilineBuffer = '';
      this.rl.setPrompt('henrydb> ');

      if (!sql) {
        this.rl.prompt();
        continue;
      }

      await this._execAndPrint(sql);
      this.rl.prompt();
    }
  }

  async _execAndPrint(sql) {
    const startTime = Date.now();
    const result = await this.query(sql);
    const elapsed = Date.now() - startTime;

    if (result.error) {
      console.error(`ERROR: ${result.error}`);
      return;
    }

    if (result.columns.length > 0 && result.rows.length > 0) {
      console.log(formatTable(result.columns, result.rows));
      console.log(`(${result.rows.length} row${result.rows.length !== 1 ? 's' : ''}) — ${elapsed}ms\n`);
    } else if (result.commandTag) {
      console.log(result.commandTag + ` — ${elapsed}ms\n`);
    }
  }

  close() {
    if (this.socket) {
      this.socket.write(buildTerminateMessage());
      this.socket.end();
    }
  }
}

// Main
async function main() {
  const opts = parseArgs();
  const cli = new HenryDBCli(opts);
  
  try {
    await cli.connect();
  } catch (err) {
    console.error(`Failed to connect to ${opts.host}:${opts.port}: ${err.message}`);
    console.error('Is HenryDB server running? Start it with: node src/server.js');
    process.exit(1);
  }
  
  await cli.startRepl();
}

main().catch(err => {
  console.error(err.message);
  process.exit(1);
});
