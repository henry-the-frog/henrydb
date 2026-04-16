/**
 * Type-Safe Printf
 * 
 * Parse format strings at "compile time" to derive the correct function type.
 *   printf "%s has %d items" : String → Int → String
 *   printf "%d + %d = %d"   : Int → Int → Int → String
 * 
 * This demonstrates dependent types / type-level computation in practice.
 */

// Format specifiers
const SPEC_INT = { tag: 'SpecInt', format: '%d', typeName: 'Int' };
const SPEC_STR = { tag: 'SpecStr', format: '%s', typeName: 'String' };
const SPEC_FLOAT = { tag: 'SpecFloat', format: '%f', typeName: 'Float' };
const SPEC_BOOL = { tag: 'SpecBool', format: '%b', typeName: 'Bool' };
const SPEC_CHAR = { tag: 'SpecChar', format: '%c', typeName: 'Char' };

// ============================================================
// Parse format string → list of segments
// ============================================================

function parseFormat(fmt) {
  const segments = [];
  let i = 0;
  let literal = '';
  
  while (i < fmt.length) {
    if (fmt[i] === '%' && i + 1 < fmt.length) {
      if (literal) { segments.push({ tag: 'Literal', text: literal }); literal = ''; }
      
      switch (fmt[i + 1]) {
        case 'd': segments.push({ tag: 'Spec', spec: SPEC_INT }); break;
        case 's': segments.push({ tag: 'Spec', spec: SPEC_STR }); break;
        case 'f': segments.push({ tag: 'Spec', spec: SPEC_FLOAT }); break;
        case 'b': segments.push({ tag: 'Spec', spec: SPEC_BOOL }); break;
        case 'c': segments.push({ tag: 'Spec', spec: SPEC_CHAR }); break;
        case '%': literal += '%'; i += 2; continue; // Escaped %
        default: literal += fmt[i] + fmt[i + 1]; i += 2; continue;
      }
      i += 2;
    } else {
      literal += fmt[i];
      i++;
    }
  }
  
  if (literal) segments.push({ tag: 'Literal', text: literal });
  return segments;
}

// ============================================================
// Derive type from format string
// ============================================================

function deriveType(fmt) {
  const segments = parseFormat(fmt);
  const paramTypes = segments
    .filter(s => s.tag === 'Spec')
    .map(s => s.spec.typeName);
  return { params: paramTypes, ret: 'String' };
}

function typeSignature(fmt) {
  const { params, ret } = deriveType(fmt);
  if (params.length === 0) return ret;
  return `${params.join(' → ')} → ${ret}`;
}

// ============================================================
// Type-checked printf
// ============================================================

function printf(fmt, ...args) {
  const segments = parseFormat(fmt);
  let argIdx = 0;
  let result = '';
  
  for (const seg of segments) {
    if (seg.tag === 'Literal') {
      result += seg.text;
    } else {
      if (argIdx >= args.length) throw new Error(`Too few arguments: expected arg for ${seg.spec.format}`);
      const arg = args[argIdx++];
      
      // Runtime type check
      switch (seg.spec.tag) {
        case 'SpecInt':
          if (typeof arg !== 'number' || !Number.isInteger(arg)) throw new Error(`%d expects integer, got ${typeof arg}`);
          result += String(arg);
          break;
        case 'SpecStr':
          if (typeof arg !== 'string') throw new Error(`%s expects string, got ${typeof arg}`);
          result += arg;
          break;
        case 'SpecFloat':
          if (typeof arg !== 'number') throw new Error(`%f expects number, got ${typeof arg}`);
          result += arg.toFixed(2);
          break;
        case 'SpecBool':
          if (typeof arg !== 'boolean') throw new Error(`%b expects boolean, got ${typeof arg}`);
          result += String(arg);
          break;
        case 'SpecChar':
          if (typeof arg !== 'string' || arg.length !== 1) throw new Error(`%c expects single char`);
          result += arg;
          break;
      }
    }
  }
  
  if (argIdx < args.length) throw new Error(`Too many arguments: ${args.length - argIdx} extra`);
  return result;
}

export { parseFormat, deriveType, typeSignature, printf, SPEC_INT, SPEC_STR, SPEC_FLOAT, SPEC_BOOL, SPEC_CHAR };
