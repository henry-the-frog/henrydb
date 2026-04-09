// er-diagram.js — Entity-Relationship diagram generator for HenryDB
// Generates SVG from database schema (tables, columns, relationships)

const COLORS = {
  bg: '#0d1117',
  surface: '#161b22',
  border: '#30363d',
  text: '#c9d1d9',
  textDim: '#8b949e',
  accent: '#58a6ff',
  green: '#3fb950',
  yellow: '#d29922',
  purple: '#bc8cff',
  red: '#f85149',
};

const TABLE_W = 180;
const COL_H = 22;
const HEADER_H = 30;
const PAD = 40;

/**
 * Generate an SVG ER diagram from database schema info.
 * @param {Array<{name: string, columns: Array<{name: string, type: string, pk?: boolean, fk?: string}>}>} tables
 * @returns {string} SVG string
 */
export function generateERDiagram(tables) {
  if (!tables || tables.length === 0) return '<svg><text>No tables</text></svg>';
  
  // Layout: arrange tables in a grid
  const cols = Math.ceil(Math.sqrt(tables.length));
  const rows = Math.ceil(tables.length / cols);
  
  const positions = [];
  for (let i = 0; i < tables.length; i++) {
    const r = Math.floor(i / cols);
    const c = i % cols;
    const tableH = HEADER_H + tables[i].columns.length * COL_H + 10;
    positions.push({
      x: PAD + c * (TABLE_W + PAD * 2),
      y: PAD + r * (200 + PAD),
      w: TABLE_W,
      h: tableH,
    });
  }
  
  const totalW = cols * (TABLE_W + PAD * 2) + PAD;
  const totalH = rows * (200 + PAD) + PAD;
  
  let svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${totalW} ${totalH}">`;
  svg += `<style>
    .table-header { font-family: -apple-system, sans-serif; font-size: 13px; font-weight: 600; fill: ${COLORS.accent}; }
    .col-name { font-family: 'SF Mono', Consolas, monospace; font-size: 11px; fill: ${COLORS.text}; }
    .col-type { font-family: 'SF Mono', Consolas, monospace; font-size: 10px; fill: ${COLORS.textDim}; }
    .pk-icon { fill: ${COLORS.yellow}; font-size: 10px; }
    .fk-icon { fill: ${COLORS.purple}; font-size: 10px; }
    .rel-line { stroke: ${COLORS.purple}; stroke-width: 1.5; fill: none; opacity: 0.6; }
  </style>`;
  
  // Draw relationship lines first (behind tables)
  const relationships = [];
  for (let i = 0; i < tables.length; i++) {
    for (const col of tables[i].columns) {
      if (col.fk) {
        const targetTable = tables.findIndex(t => t.name === col.fk);
        if (targetTable >= 0) {
          relationships.push({ from: i, to: targetTable, fromCol: col.name, toCol: 'id' });
        }
      }
    }
  }
  
  for (const rel of relationships) {
    const fromPos = positions[rel.from];
    const toPos = positions[rel.to];
    const fromX = fromPos.x + fromPos.w;
    const fromY = fromPos.y + HEADER_H + 15;
    const toX = toPos.x;
    const toY = toPos.y + HEADER_H + 5;
    
    // Draw curved line
    const midX = (fromX + toX) / 2;
    svg += `<path d="M${fromX} ${fromY} C${midX} ${fromY}, ${midX} ${toY}, ${toX} ${toY}" class="rel-line" marker-end="url(#arrowhead)"/>`;
  }
  
  // Arrowhead marker
  svg += `<defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
    <path d="M0,0 L8,3 L0,6" fill="${COLORS.purple}" opacity="0.6"/></marker></defs>`;
  
  // Draw tables
  for (let i = 0; i < tables.length; i++) {
    const table = tables[i];
    const pos = positions[i];
    const { x, y, w, h } = pos;
    
    // Table container
    svg += `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="6" fill="${COLORS.surface}" stroke="${COLORS.border}" stroke-width="1"/>`;
    
    // Header background
    svg += `<rect x="${x}" y="${y}" width="${w}" height="${HEADER_H}" rx="6" fill="${COLORS.accent}" fill-opacity="0.1"/>`;
    svg += `<rect x="${x}" y="${y + HEADER_H - 6}" width="${w}" height="6" fill="${COLORS.accent}" fill-opacity="0.1"/>`;
    
    // Table name
    svg += `<text x="${x + w/2}" y="${y + 20}" text-anchor="middle" class="table-header">📄 ${escSvg(table.name)}</text>`;
    
    // Separator
    svg += `<line x1="${x}" y1="${y + HEADER_H}" x2="${x + w}" y2="${y + HEADER_H}" stroke="${COLORS.border}"/>`;
    
    // Columns
    for (let j = 0; j < table.columns.length; j++) {
      const col = table.columns[j];
      const cy = y + HEADER_H + 6 + j * COL_H;
      
      // Icons
      let iconX = x + 8;
      if (col.pk) {
        svg += `<text x="${iconX}" y="${cy + 13}" class="pk-icon">🔑</text>`;
        iconX += 16;
      } else if (col.fk) {
        svg += `<text x="${iconX}" y="${cy + 13}" class="fk-icon">🔗</text>`;
        iconX += 16;
      }
      
      svg += `<text x="${iconX + 4}" y="${cy + 14}" class="col-name">${escSvg(col.name)}</text>`;
      svg += `<text x="${x + w - 8}" y="${cy + 14}" text-anchor="end" class="col-type">${escSvg(col.type || '')}</text>`;
    }
  }
  
  svg += '</svg>';
  return svg;
}

/**
 * Extract schema info from a Database instance.
 * @param {Database} db - HenryDB instance
 * @returns {Array} Array of table definitions
 */
export function extractSchema(db) {
  const tables = [];
  try {
    const result = db.execute('SHOW TABLES');
    for (const row of result.rows) {
      const name = row.table_name || row.name || Object.values(row)[0];
      const cols = [];
      try {
        const peek = db.execute(`SELECT * FROM ${name} LIMIT 1`);
        if (peek.rows && peek.rows.length > 0) {
          for (const [key, val] of Object.entries(peek.rows[0])) {
            const type = val === null ? 'NULL' : typeof val === 'number' ? 
              (Number.isInteger(val) ? 'INTEGER' : 'REAL') : 'TEXT';
            cols.push({
              name: key,
              type,
              pk: key === 'id',
              fk: key.endsWith('_id') ? key.replace('_id', 's') : null, // heuristic
            });
          }
        }
      } catch(e) {}
      tables.push({ name, columns: cols });
    }
  } catch(e) {}
  return tables;
}

function escSvg(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
