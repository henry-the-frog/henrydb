// json-parser.js — JSON parser from scratch (RFC 8259)
// Recursive descent parser with proper error reporting.

/**
 * Parse a JSON string into a JavaScript value.
 * @param {string} input — JSON string
 * @returns {*} — parsed value
 */
export function jsonParse(input) {
  if (typeof input !== 'string') throw new SyntaxError('Input must be a string');
  
  let pos = 0;
  
  function error(msg) {
    const context = input.slice(Math.max(0, pos - 10), pos + 10);
    throw new SyntaxError(`${msg} at position ${pos} (near "${context}")`);
  }
  
  function peek() { return input[pos]; }
  function advance() { return input[pos++]; }
  
  function skipWhitespace() {
    while (pos < input.length && ' \t\n\r'.includes(input[pos])) pos++;
  }
  
  function expect(ch) {
    if (input[pos] !== ch) error(`Expected '${ch}', got '${input[pos] || 'EOF'}'`);
    pos++;
  }
  
  function parseValue() {
    skipWhitespace();
    if (pos >= input.length) error('Unexpected end of input');
    
    const ch = peek();
    if (ch === '"') return parseString();
    if (ch === '{') return parseObject();
    if (ch === '[') return parseArray();
    if (ch === 't') return parseTrue();
    if (ch === 'f') return parseFalse();
    if (ch === 'n') return parseNull();
    if (ch === '-' || (ch >= '0' && ch <= '9')) return parseNumber();
    
    error(`Unexpected character '${ch}'`);
  }
  
  function parseString() {
    expect('"');
    let result = '';
    
    while (pos < input.length) {
      const ch = advance();
      
      if (ch === '"') return result;
      
      if (ch === '\\') {
        if (pos >= input.length) error('Unterminated escape sequence');
        const esc = advance();
        switch (esc) {
          case '"': result += '"'; break;
          case '\\': result += '\\'; break;
          case '/': result += '/'; break;
          case 'b': result += '\b'; break;
          case 'f': result += '\f'; break;
          case 'n': result += '\n'; break;
          case 'r': result += '\r'; break;
          case 't': result += '\t'; break;
          case 'u': {
            let hex = '';
            for (let i = 0; i < 4; i++) {
              if (pos >= input.length) error('Unterminated unicode escape');
              hex += advance();
            }
            if (!/^[0-9a-fA-F]{4}$/.test(hex)) error(`Invalid unicode escape: \\u${hex}`);
            const codePoint = parseInt(hex, 16);
            
            // Handle surrogate pairs
            if (codePoint >= 0xD800 && codePoint <= 0xDBFF) {
              // High surrogate — expect low surrogate
              if (input[pos] === '\\' && input[pos + 1] === 'u') {
                pos += 2; // skip \u
                let hex2 = '';
                for (let i = 0; i < 4; i++) hex2 += advance();
                const low = parseInt(hex2, 16);
                if (low >= 0xDC00 && low <= 0xDFFF) {
                  result += String.fromCodePoint(((codePoint - 0xD800) << 10) + (low - 0xDC00) + 0x10000);
                } else {
                  result += String.fromCharCode(codePoint) + String.fromCharCode(low);
                }
              } else {
                result += String.fromCharCode(codePoint);
              }
            } else {
              result += String.fromCharCode(codePoint);
            }
            break;
          }
          default: error(`Invalid escape character: \\${esc}`);
        }
      } else if (ch.charCodeAt(0) < 0x20) {
        error('Control characters must be escaped in strings');
      } else {
        result += ch;
      }
    }
    
    error('Unterminated string');
  }
  
  function parseNumber() {
    const start = pos;
    
    // Optional minus
    if (peek() === '-') advance();
    
    // Integer part
    if (peek() === '0') {
      advance();
      // Leading zeros not allowed (except 0 itself)
    } else if (peek() >= '1' && peek() <= '9') {
      advance();
      while (pos < input.length && peek() >= '0' && peek() <= '9') advance();
    } else {
      error('Invalid number');
    }
    
    // Fractional part
    if (peek() === '.') {
      advance();
      if (pos >= input.length || peek() < '0' || peek() > '9') {
        error('Expected digit after decimal point');
      }
      while (pos < input.length && peek() >= '0' && peek() <= '9') advance();
    }
    
    // Exponent
    if (peek() === 'e' || peek() === 'E') {
      advance();
      if (peek() === '+' || peek() === '-') advance();
      if (pos >= input.length || peek() < '0' || peek() > '9') {
        error('Expected digit in exponent');
      }
      while (pos < input.length && peek() >= '0' && peek() <= '9') advance();
    }
    
    const numStr = input.slice(start, pos);
    const num = Number(numStr);
    if (!isFinite(num)) error(`Number out of range: ${numStr}`);
    return num;
  }
  
  function parseObject() {
    expect('{');
    skipWhitespace();
    
    const obj = {};
    
    if (peek() === '}') { advance(); return obj; }
    
    while (true) {
      skipWhitespace();
      if (peek() !== '"') error('Expected string key in object');
      const key = parseString();
      
      skipWhitespace();
      expect(':');
      
      const value = parseValue();
      obj[key] = value;
      
      skipWhitespace();
      if (peek() === '}') { advance(); return obj; }
      expect(',');
    }
  }
  
  function parseArray() {
    expect('[');
    skipWhitespace();
    
    const arr = [];
    
    if (peek() === ']') { advance(); return arr; }
    
    while (true) {
      arr.push(parseValue());
      
      skipWhitespace();
      if (peek() === ']') { advance(); return arr; }
      expect(',');
    }
  }
  
  function parseTrue() {
    if (input.slice(pos, pos + 4) !== 'true') error('Expected "true"');
    pos += 4;
    return true;
  }
  
  function parseFalse() {
    if (input.slice(pos, pos + 5) !== 'false') error('Expected "false"');
    pos += 5;
    return false;
  }
  
  function parseNull() {
    if (input.slice(pos, pos + 4) !== 'null') error('Expected "null"');
    pos += 4;
    return null;
  }
  
  // Parse top-level value
  const result = parseValue();
  skipWhitespace();
  
  if (pos < input.length) {
    error(`Unexpected character after value: '${input[pos]}'`);
  }
  
  return result;
}

/**
 * Stringify a JavaScript value to JSON.
 * @param {*} value
 * @param {number} [indent] — spaces for indentation
 * @returns {string}
 */
export function jsonStringify(value, indent = 0) {
  return _stringify(value, indent, 0);
}

function _stringify(value, indent, depth) {
  if (value === null) return 'null';
  if (value === undefined) return undefined;
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') {
    if (!isFinite(value)) return 'null';
    return Object.is(value, -0) ? '0' : String(value);
  }
  if (typeof value === 'string') return _stringifyString(value);
  
  if (Array.isArray(value)) {
    if (value.length === 0) return '[]';
    const items = value.map(v => _stringify(v, indent, depth + 1) ?? 'null');
    if (indent > 0) {
      const pad = ' '.repeat(indent * (depth + 1));
      const closePad = ' '.repeat(indent * depth);
      return '[\n' + items.map(i => pad + i).join(',\n') + '\n' + closePad + ']';
    }
    return '[' + items.join(',') + ']';
  }
  
  if (typeof value === 'object') {
    const entries = Object.entries(value).filter(([, v]) => v !== undefined);
    if (entries.length === 0) return '{}';
    const pairs = entries.map(([k, v]) => {
      const val = _stringify(v, indent, depth + 1);
      return val !== undefined ? `${_stringifyString(k)}:${indent > 0 ? ' ' : ''}${val}` : null;
    }).filter(Boolean);
    if (indent > 0) {
      const pad = ' '.repeat(indent * (depth + 1));
      const closePad = ' '.repeat(indent * depth);
      return '{\n' + pairs.map(p => pad + p).join(',\n') + '\n' + closePad + '}';
    }
    return '{' + pairs.join(',') + '}';
  }
  
  return undefined;
}

function _stringifyString(str) {
  let result = '"';
  for (let i = 0; i < str.length; i++) {
    const ch = str[i];
    const code = ch.charCodeAt(0);
    if (ch === '"') result += '\\"';
    else if (ch === '\\') result += '\\\\';
    else if (ch === '\b') result += '\\b';
    else if (ch === '\f') result += '\\f';
    else if (ch === '\n') result += '\\n';
    else if (ch === '\r') result += '\\r';
    else if (ch === '\t') result += '\\t';
    else if (code < 0x20) result += '\\u' + code.toString(16).padStart(4, '0');
    else result += ch;
  }
  return result + '"';
}
