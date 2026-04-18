# HenryDB Wire Protocol Design

uses: 0
created: 2026-04-18
tags: database, henrydb, wire-protocol, networking

## Option A: PostgreSQL Wire Protocol (v3.0)

### Advantages
- Any PG client driver works (psql, pg, node-postgres, Python psycopg2, etc.)
- Ecosystem compatibility — vast tooling
- Standard and well-documented

### Implementation (Node.js net module)
```js
const server = net.createServer(socket => {
  // 1. Read StartupMessage (no type byte, just length + version + params)
  // 2. Send AuthenticationOk (type 'R', int32 0)
  // 3. Send ParameterStatus for server_version, etc.
  // 4. Send ReadyForQuery (type 'Z', byte 'I')
  // 5. Loop: read Query → execute → send RowDescription + DataRow[] + CommandComplete + ReadyForQuery
});
server.listen(5433); // Use non-standard port to avoid conflict with real PG
```

### Key Message Types
| Type | Name | Description |
|------|------|-------------|
| (none) | StartupMessage | Client → Server: version + user + database |
| R | Authentication | Server → Client: auth request/ok |
| Q | Query | Client → Server: SQL text |
| T | RowDescription | Server → Client: column names + types |
| D | DataRow | Server → Client: one row of data |
| C | CommandComplete | Server → Client: "SELECT 5" / "INSERT 0 1" |
| Z | ReadyForQuery | Server → Client: ready for next query |
| E | ErrorResponse | Server → Client: error with severity, message |

### Type Mapping (HenryDB → PG OIDs)
| HenryDB | PG OID | Name |
|---------|--------|------|
| INT | 23 | int4 |
| TEXT | 25 | text |
| FLOAT | 701 | float8 |
| BOOLEAN | 16 | bool |
| NULL | 0 | unknown |

### Effort: ~200 lines for basic simple query protocol

## Option B: HTTP/JSON API

### Advantages
- Simpler to implement
- Web-native — works from browsers, curl, fetch
- No binary protocol parsing

### Endpoints
```
POST /query
  Body: { "sql": "SELECT * FROM t WHERE id = 1" }
  Response: { "type": "ROWS", "columns": ["id", "val"], "rows": [{"id":1,"val":100}], "rowCount": 1 }

POST /execute
  Body: { "sql": "INSERT INTO t VALUES (1, 100)" }
  Response: { "type": "OK", "message": "INSERT 1", "rowCount": 1 }

GET /health
  Response: { "status": "ok", "version": "0.1.0" }
```

### Effort: ~50 lines with Express/http

## Recommendation
**Start with HTTP/JSON** (simpler, faster to ship). Then add **PG wire protocol** (for ecosystem compatibility). They can coexist on different ports.

## Dependencies
- `net` module (built-in) for PG wire
- `http` module (built-in) for HTTP
- No external dependencies needed
