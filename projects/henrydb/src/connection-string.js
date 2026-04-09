// connection-string.js — Parse PostgreSQL-style connection strings and URLs
// Supports: postgres://user:pass@host:port/db?ssl=true
//           postgresql://... (alias)
//           Key-value DSN: host=localhost port=5432 dbname=mydb user=admin

/**
 * Parse a connection string into components.
 * @param {string} str - Connection URL or DSN
 * @returns {Object} { host, port, database, user, password, ssl, options }
 */
export function parseConnectionString(str) {
  if (!str) return defaults();
  str = str.trim();
  
  // URL format: postgres://user:pass@host:port/db?params
  if (str.startsWith('postgres://') || str.startsWith('postgresql://')) {
    return parseURL(str);
  }
  
  // Key-value DSN format
  if (str.includes('=')) {
    return parseDSN(str);
  }
  
  // Assume it's just a host
  return { ...defaults(), host: str };
}

function defaults() {
  return {
    host: 'localhost',
    port: 5432,
    database: 'postgres',
    user: 'postgres',
    password: '',
    ssl: false,
    options: {},
  };
}

function parseURL(url) {
  const result = defaults();
  
  // Remove protocol
  const withoutProto = url.replace(/^postgres(ql)?:\/\//, '');
  
  // Split by ?
  const [main, query] = withoutProto.split('?');
  
  // Parse query params
  if (query) {
    const params = new URLSearchParams(query);
    for (const [key, val] of params) {
      if (key === 'ssl' || key === 'sslmode') {
        result.ssl = val === 'true' || val === 'require' || val === 'verify-full';
      } else if (key === 'connect_timeout') {
        result.options.connectTimeout = parseInt(val);
      } else if (key === 'application_name') {
        result.options.applicationName = val;
      } else {
        result.options[key] = val;
      }
    }
  }
  
  // Parse main: user:pass@host:port/db
  let rest = main;
  
  // Extract database (after last /)
  const slashIdx = rest.lastIndexOf('/');
  if (slashIdx >= 0) {
    result.database = decodeURIComponent(rest.substring(slashIdx + 1)) || result.database;
    rest = rest.substring(0, slashIdx);
  }
  
  // Extract user:pass (before @)
  const atIdx = rest.lastIndexOf('@');
  if (atIdx >= 0) {
    const userPart = rest.substring(0, atIdx);
    rest = rest.substring(atIdx + 1);
    
    const colonIdx = userPart.indexOf(':');
    if (colonIdx >= 0) {
      result.user = decodeURIComponent(userPart.substring(0, colonIdx));
      result.password = decodeURIComponent(userPart.substring(colonIdx + 1));
    } else {
      result.user = decodeURIComponent(userPart);
    }
  }
  
  // Extract host:port
  if (rest) {
    // Handle IPv6 [::1]:5432
    const v6Match = rest.match(/^\[([^\]]+)\]:?(\d+)?$/);
    if (v6Match) {
      result.host = v6Match[1];
      if (v6Match[2]) result.port = parseInt(v6Match[2]);
    } else {
      const colonIdx = rest.lastIndexOf(':');
      if (colonIdx >= 0) {
        result.host = rest.substring(0, colonIdx) || result.host;
        result.port = parseInt(rest.substring(colonIdx + 1)) || result.port;
      } else {
        result.host = rest || result.host;
      }
    }
  }
  
  return result;
}

function parseDSN(dsn) {
  const result = defaults();
  
  // Parse key=value pairs (space-separated, supports quoted values)
  const regex = /(\w+)\s*=\s*(?:'([^']*)'|"([^"]*)"|(\S+))/g;
  let match;
  
  while ((match = regex.exec(dsn)) !== null) {
    const key = match[1].toLowerCase();
    const val = match[2] || match[3] || match[4];
    
    switch (key) {
      case 'host': case 'hostaddr': result.host = val; break;
      case 'port': result.port = parseInt(val); break;
      case 'dbname': case 'database': result.database = val; break;
      case 'user': result.user = val; break;
      case 'password': result.password = val; break;
      case 'sslmode': result.ssl = val === 'require' || val === 'verify-full'; break;
      default: result.options[key] = val;
    }
  }
  
  return result;
}

/**
 * Build a connection string from components.
 * @param {Object} config - Connection config
 * @returns {string} postgres:// URL
 */
export function buildConnectionString(config) {
  const c = { ...defaults(), ...config };
  let url = 'postgres://';
  
  if (c.user) {
    url += encodeURIComponent(c.user);
    if (c.password) url += ':' + encodeURIComponent(c.password);
    url += '@';
  }
  
  url += c.host;
  if (c.port && c.port !== 5432) url += ':' + c.port;
  url += '/' + encodeURIComponent(c.database);
  
  const params = [];
  if (c.ssl) params.push('ssl=true');
  for (const [key, val] of Object.entries(c.options || {})) {
    params.push(`${key}=${encodeURIComponent(val)}`);
  }
  if (params.length) url += '?' + params.join('&');
  
  return url;
}
