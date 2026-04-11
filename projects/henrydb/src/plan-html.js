// plan-html.js — HTML/SVG visualization for query plan trees
// Generates an interactive HTML page with SVG plan tree visualization.
// Inspired by pgAdmin's EXPLAIN visualizer and dalibo's explain.depesz.com.

import { PlanFormatter } from './query-plan.js';

const NODE_COLORS = {
  'Seq Scan':       { bg: '#FFE0B2', border: '#F57C00', icon: '📋' },
  'Index Scan':     { bg: '#C8E6C9', border: '#388E3C', icon: '🔍' },
  'BTree PK Lookup':{ bg: '#C8E6C9', border: '#2E7D32', icon: '🌳' },
  'Hash Join':      { bg: '#F8BBD0', border: '#C2185B', icon: '🔗' },
  'Nested Loop':    { bg: '#F8BBD0', border: '#AD1457', icon: '🔄' },
  'Merge Join':     { bg: '#F8BBD0', border: '#880E4F', icon: '↕️' },
  'Hash':           { bg: '#E1BEE7', border: '#7B1FA2', icon: '#️⃣' },
  'Aggregate':      { bg: '#B3E5FC', border: '#0288D1', icon: 'Σ' },
  'Sort':           { bg: '#D1C4E9', border: '#512DA8', icon: '⬆️' },
  'Unique':         { bg: '#DCEDC8', border: '#689F38', icon: '✨' },
  'Limit':          { bg: '#D7CCC8', border: '#5D4037', icon: '✂️' },
  'Filter':         { bg: '#BBDEFB', border: '#1976D2', icon: '🔬' },
  'WindowAgg':      { bg: '#FFF9C4', border: '#F9A825', icon: '📊' },
  'Result':         { bg: '#F5F5F5', border: '#757575', icon: '📤' },
};

function getNodeStyle(type) {
  for (const [key, style] of Object.entries(NODE_COLORS)) {
    if (type.includes(key)) return style;
  }
  return { bg: '#F5F5F5', border: '#9E9E9E', icon: '⚙️' };
}

/**
 * Layout a plan tree for SVG rendering.
 * Returns an array of positioned nodes with x, y, width, height.
 */
function layoutTree(root) {
  const NODE_W = 280;
  const NODE_H = 100;
  const H_GAP = 30;
  const V_GAP = 50;

  const nodes = [];
  const edges = [];

  // First pass: calculate subtree widths
  function subtreeWidth(node) {
    if (node.children.length === 0) return NODE_W;
    const childWidths = node.children.map(c => subtreeWidth(c));
    return Math.max(NODE_W, childWidths.reduce((s, w) => s + w, 0) + (node.children.length - 1) * H_GAP);
  }

  // Second pass: assign positions
  function positionNode(node, x, y, availWidth) {
    const nodeX = x + (availWidth - NODE_W) / 2;
    const id = nodes.length;
    nodes.push({ node, x: nodeX, y, id });

    if (node.children.length > 0) {
      const childWidths = node.children.map(c => subtreeWidth(c));
      const totalChildWidth = childWidths.reduce((s, w) => s + w, 0) + (node.children.length - 1) * H_GAP;
      let childX = x + (availWidth - totalChildWidth) / 2;

      for (let i = 0; i < node.children.length; i++) {
        const childId = positionNode(node.children[i], childX, y + NODE_H + V_GAP, childWidths[i]);
        edges.push({ from: id, to: childId });
        childX += childWidths[i] + H_GAP;
      }
    }

    return id;
  }

  const totalWidth = subtreeWidth(root);
  positionNode(root, 0, 20, totalWidth);

  return { nodes, edges, width: totalWidth, height: nodes.reduce((m, n) => Math.max(m, n.y + NODE_H), 0) + 40 };
}

/**
 * Generate SVG string for a plan tree.
 */
function planToSVG(root, options = {}) {
  const { nodes, edges, width, height } = layoutTree(root);
  const NODE_W = 280;
  const NODE_H = 100;
  const analyze = options.analyze || false;

  let svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${width + 40}" height="${height + 20}" viewBox="0 0 ${width + 40} ${height + 20}">\n`;
  svg += `  <defs>\n`;
  svg += `    <filter id="shadow" x="-5%" y="-5%" width="110%" height="120%">\n`;
  svg += `      <feDropShadow dx="2" dy="2" stdDeviation="3" flood-opacity="0.15"/>\n`;
  svg += `    </filter>\n`;
  svg += `  </defs>\n`;

  // Draw edges first (behind nodes)
  for (const edge of edges) {
    const from = nodes[edge.from];
    const to = nodes[edge.to];
    const fromX = from.x + NODE_W / 2;
    const fromY = from.y + NODE_H;
    const toX = to.x + NODE_W / 2;
    const toY = to.y;
    svg += `  <path d="M ${fromX} ${fromY} C ${fromX} ${fromY + 25}, ${toX} ${toY - 25}, ${toX} ${toY}" fill="none" stroke="#78909C" stroke-width="2" stroke-dasharray="5,3"/>\n`;
  }

  // Draw nodes
  for (const { node, x, y, id } of nodes) {
    const style = getNodeStyle(node.type);
    const label = PlanFormatter._nodeLabel ? PlanFormatter._nodeLabel(node) : node.type;

    // Node rectangle
    svg += `  <g transform="translate(${x},${y})" filter="url(#shadow)">\n`;
    svg += `    <rect width="${NODE_W}" height="${NODE_H}" rx="8" fill="${style.bg}" stroke="${style.border}" stroke-width="2"/>\n`;

    // Icon + type label
    svg += `    <text x="12" y="22" font-family="system-ui, sans-serif" font-size="13" font-weight="bold" fill="#333">${style.icon} ${escapeXml(label)}</text>\n`;

    // Cost + rows estimate
    let detailY = 40;
    if (node.estimatedRows != null) {
      svg += `    <text x="12" y="${detailY}" font-family="monospace" font-size="11" fill="#666">est rows: ${node.estimatedRows}</text>\n`;
      detailY += 16;
    }
    if (node.estimatedCost != null) {
      svg += `    <text x="12" y="${detailY}" font-family="monospace" font-size="11" fill="#666">cost: ${node.estimatedCost.toFixed(2)}</text>\n`;
      detailY += 16;
    }

    // Actuals (EXPLAIN ANALYZE)
    if (analyze && node.actualRows != null) {
      svg += `    <text x="12" y="${detailY}" font-family="monospace" font-size="11" fill="#1B5E20" font-weight="bold">actual rows: ${node.actualRows}</text>\n`;
      detailY += 16;

      // Row estimate accuracy bar
      if (node.estimatedRows != null && node.estimatedRows > 0) {
        const ratio = node.actualRows / node.estimatedRows;
        const barWidth = Math.min(250, Math.max(5, 125 * ratio));
        const barColor = ratio > 2 || ratio < 0.5 ? '#F44336' : ratio > 1.5 || ratio < 0.67 ? '#FF9800' : '#4CAF50';
        svg += `    <rect x="12" y="${detailY}" width="${barWidth}" height="6" rx="3" fill="${barColor}" opacity="0.7"/>\n`;
        svg += `    <text x="${barWidth + 18}" y="${detailY + 6}" font-family="monospace" font-size="9" fill="${barColor}">${(ratio * 100).toFixed(0)}%</text>\n`;
      }
    }

    // Filter info
    if (node.filter) {
      svg += `    <text x="12" y="${Math.min(detailY + 2, NODE_H - 8)}" font-family="monospace" font-size="10" fill="#E65100">⚡ ${escapeXml(truncate(node.filter, 35))}</text>\n`;
    }

    svg += `  </g>\n`;
  }

  svg += `</svg>`;
  return svg;
}

function escapeXml(str) {
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function truncate(str, len) {
  return str.length > len ? str.substring(0, len - 1) + '…' : str;
}

/**
 * Generate a full HTML page with the plan visualization.
 */
export function planToHTML(root, options = {}) {
  const svg = planToSVG(root, options);
  const textPlan = PlanFormatter.format(root, options).join('\n');

  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>HenryDB Query Plan</title>
  <style>
    body { font-family: system-ui, -apple-system, sans-serif; background: #FAFAFA; margin: 20px; color: #333; }
    h1 { color: #1565C0; font-size: 20px; }
    .plan-container { overflow-x: auto; padding: 20px; background: white; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin-bottom: 20px; }
    .text-plan { background: #263238; color: #B0BEC5; padding: 16px; border-radius: 8px; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 13px; line-height: 1.6; white-space: pre; overflow-x: auto; }
    .stats { display: flex; gap: 20px; margin: 16px 0; }
    .stat { background: white; padding: 12px 20px; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }
    .stat-value { font-size: 24px; font-weight: bold; color: #1565C0; }
    .stat-label { font-size: 12px; color: #666; text-transform: uppercase; }
    .legend { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; }
    .legend-item { display: flex; align-items: center; gap: 4px; font-size: 12px; }
    .legend-color { width: 16px; height: 16px; border-radius: 3px; border: 1px solid #ccc; }
  </style>
</head>
<body>
  <h1>🔍 HenryDB Query Plan${options.analyze ? ' (ANALYZE)' : ''}</h1>
  
  <div class="legend">
    ${Object.entries(NODE_COLORS).map(([name, style]) => 
      `<div class="legend-item"><div class="legend-color" style="background:${style.bg};border-color:${style.border}"></div>${name}</div>`
    ).join('\n    ')}
  </div>

  <div class="plan-container">
    ${svg}
  </div>

  <h2>Text Plan</h2>
  <div class="text-plan">${escapeXml(textPlan)}</div>
</body>
</html>`;
}
