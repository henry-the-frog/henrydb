// matrix.js — Dense matrix for database computations
// Used for covariance matrices, linear regression, correlation analysis.

export class Matrix {
  constructor(rows, cols, data) {
    this.rows = rows;
    this.cols = cols;
    this._data = data || new Float64Array(rows * cols);
  }

  static zeros(rows, cols) { return new Matrix(rows, cols); }
  static identity(n) {
    const m = new Matrix(n, n);
    for (let i = 0; i < n; i++) m.set(i, i, 1);
    return m;
  }
  static fromArray(arr) {
    const rows = arr.length, cols = arr[0].length;
    const m = new Matrix(rows, cols);
    for (let i = 0; i < rows; i++) for (let j = 0; j < cols; j++) m.set(i, j, arr[i][j]);
    return m;
  }

  get(i, j) { return this._data[i * this.cols + j]; }
  set(i, j, v) { this._data[i * this.cols + j] = v; }

  add(other) {
    const result = new Matrix(this.rows, this.cols);
    for (let i = 0; i < this._data.length; i++) result._data[i] = this._data[i] + other._data[i];
    return result;
  }

  multiply(other) {
    const result = new Matrix(this.rows, other.cols);
    for (let i = 0; i < this.rows; i++) {
      for (let j = 0; j < other.cols; j++) {
        let sum = 0;
        for (let k = 0; k < this.cols; k++) sum += this.get(i, k) * other.get(k, j);
        result.set(i, j, sum);
      }
    }
    return result;
  }

  transpose() {
    const result = new Matrix(this.cols, this.rows);
    for (let i = 0; i < this.rows; i++)
      for (let j = 0; j < this.cols; j++) result.set(j, i, this.get(i, j));
    return result;
  }

  scale(s) {
    const result = new Matrix(this.rows, this.cols);
    for (let i = 0; i < this._data.length; i++) result._data[i] = this._data[i] * s;
    return result;
  }

  toArray() {
    const arr = [];
    for (let i = 0; i < this.rows; i++) {
      arr.push([]);
      for (let j = 0; j < this.cols; j++) arr[i].push(this.get(i, j));
    }
    return arr;
  }

  trace() {
    let sum = 0;
    for (let i = 0; i < Math.min(this.rows, this.cols); i++) sum += this.get(i, i);
    return sum;
  }

  frobenius() {
    let sum = 0;
    for (const v of this._data) sum += v * v;
    return Math.sqrt(sum);
  }
}
