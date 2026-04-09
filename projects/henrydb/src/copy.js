// copy.js — COPY TO/FROM for bulk data import/export
// COPY table FROM 'data' WITH (FORMAT csv|tsv|text, HEADER true, DELIMITER ',', NULL '')

/**
 * CopyManager — handles bulk COPY operations.
 */
export class CopyManager {
  constructor(db) {
    this.db = db;
  }

  /**
   * COPY TO — export table data.
   * @param {string} tableName
   * @param {object} options — { format, header, delimiter, null, columns, where }
   * @returns {string} Exported data
   */
  copyTo(tableName, rows, options = {}) {
    const format = (options.format || 'csv').toLowerCase();
    const header = options.header !== false;
    const delimiter = options.delimiter || (format === 'tsv' ? '\t' : ',');
    const nullStr = options.null || '';
    const columns = options.columns || (rows.length > 0 ? Object.keys(rows[0]) : []);
    const quote = options.quote || '"';

    const lines = [];

    if (header) {
      lines.push(columns.join(delimiter));
    }

    for (const row of rows) {
      const values = columns.map(col => {
        const val = row[col];
        if (val === null || val === undefined) return nullStr;
        const str = String(val);
        if (format === 'csv' && (str.includes(delimiter) || str.includes(quote) || str.includes('\n'))) {
          return `${quote}${str.replace(new RegExp(quote, 'g'), quote + quote)}${quote}`;
        }
        return str;
      });
      lines.push(values.join(delimiter));
    }

    return lines.join('\n') + '\n';
  }

  /**
   * COPY FROM — import data into a table.
   * @param {string} data — Raw CSV/TSV data
   * @param {object} options — { format, header, delimiter, null, columns }
   * @returns {{ rows: object[], count: number }}
   */
  copyFrom(data, columns, options = {}) {
    const format = (options.format || 'csv').toLowerCase();
    const header = options.header !== false;
    const delimiter = options.delimiter || (format === 'tsv' ? '\t' : ',');
    const nullStr = options.null || '';
    const quote = options.quote || '"';

    const lines = data.trim().split('\n');
    let startIdx = 0;

    let cols = columns;
    if (header && lines.length > 0) {
      cols = lines[0].split(delimiter).map(c => c.trim().replace(/^"|"$/g, ''));
      startIdx = 1;
    }

    const rows = [];
    for (let i = startIdx; i < lines.length; i++) {
      const line = lines[i];
      if (!line.trim()) continue;

      const values = this._parseLine(line, delimiter, quote);
      const row = {};
      for (let j = 0; j < cols.length; j++) {
        let val = j < values.length ? values[j] : null;
        if (val === nullStr) val = null;
        else if (val !== null) {
          // Auto-detect types
          if (/^-?\d+$/.test(val)) val = parseInt(val);
          else if (/^-?\d+\.\d+$/.test(val)) val = parseFloat(val);
          else if (val.toLowerCase() === 'true') val = true;
          else if (val.toLowerCase() === 'false') val = false;
        }
        row[cols[j]] = val;
      }
      rows.push(row);
    }

    return { rows, count: rows.length };
  }

  /**
   * Parse a CSV/TSV line handling quoted fields.
   */
  _parseLine(line, delimiter, quote) {
    const values = [];
    let current = '';
    let inQuote = false;
    let i = 0;

    while (i < line.length) {
      if (inQuote) {
        if (line[i] === quote) {
          if (i + 1 < line.length && line[i + 1] === quote) {
            current += quote;
            i += 2;
          } else {
            inQuote = false;
            i++;
          }
        } else {
          current += line[i++];
        }
      } else {
        if (line[i] === quote) {
          inQuote = true;
          i++;
        } else if (line[i] === delimiter || (delimiter.length > 1 && line.substring(i, i + delimiter.length) === delimiter)) {
          values.push(current);
          current = '';
          i += delimiter.length;
        } else {
          current += line[i++];
        }
      }
    }
    values.push(current);

    return values;
  }
}
