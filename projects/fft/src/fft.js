// fft.js — FFT and signal processing

// ===== Complex number =====
export class Complex {
  constructor(re = 0, im = 0) { this.re = re; this.im = im; }
  add(c) { return new Complex(this.re + c.re, this.im + c.im); }
  sub(c) { return new Complex(this.re - c.re, this.im - c.im); }
  mul(c) { return new Complex(this.re * c.re - this.im * c.im, this.re * c.im + this.im * c.re); }
  magnitude() { return Math.sqrt(this.re * this.re + this.im * this.im); }
  phase() { return Math.atan2(this.im, this.re); }
  conjugate() { return new Complex(this.re, -this.im); }
  scale(s) { return new Complex(this.re * s, this.im * s); }
  static polar(r, theta) { return new Complex(r * Math.cos(theta), r * Math.sin(theta)); }
}

// ===== DFT (naive O(n²)) =====
export function dft(signal) {
  const N = signal.length;
  const result = [];
  for (let k = 0; k < N; k++) {
    let sum = new Complex();
    for (let n = 0; n < N; n++) {
      const angle = -2 * Math.PI * k * n / N;
      const x = signal[n] instanceof Complex ? signal[n] : new Complex(signal[n]);
      sum = sum.add(x.mul(Complex.polar(1, angle)));
    }
    result.push(sum);
  }
  return result;
}

// ===== FFT (Cooley-Tukey radix-2) =====
export function fft(signal) {
  const N = signal.length;
  if (N <= 1) return signal.map(x => x instanceof Complex ? x : new Complex(x));

  if (N & (N - 1)) throw new Error('FFT requires power-of-2 length');

  const data = signal.map(x => x instanceof Complex ? x : new Complex(x));
  return _fft(data, false);
}

export function ifft(spectrum) {
  const N = spectrum.length;
  const result = _fft(spectrum, true);
  return result.map(c => c.scale(1 / N));
}

function _fft(data, inverse) {
  const N = data.length;
  if (N === 1) return [data[0]];

  const even = _fft(data.filter((_, i) => i % 2 === 0), inverse);
  const odd = _fft(data.filter((_, i) => i % 2 === 1), inverse);

  const result = new Array(N);
  const sign = inverse ? 1 : -1;

  for (let k = 0; k < N / 2; k++) {
    const angle = sign * 2 * Math.PI * k / N;
    const twiddle = Complex.polar(1, angle).mul(odd[k]);
    result[k] = even[k].add(twiddle);
    result[k + N / 2] = even[k].sub(twiddle);
  }

  return result;
}

// ===== Convolution =====
export function convolve(a, b) {
  const N = nextPow2(a.length + b.length - 1);
  const fa = fft(pad(a, N));
  const fb = fft(pad(b, N));
  const product = fa.map((v, i) => v.mul(fb[i]));
  const result = ifft(product);
  return result.slice(0, a.length + b.length - 1).map(c => c.re);
}

function pad(signal, length) {
  const padded = new Array(length).fill(0);
  for (let i = 0; i < signal.length; i++) padded[i] = signal[i];
  return padded;
}

function nextPow2(n) {
  let p = 1;
  while (p < n) p <<= 1;
  return p;
}

// ===== Windowing Functions =====
export function hamming(N) {
  return Array.from({ length: N }, (_, n) => 0.54 - 0.46 * Math.cos(2 * Math.PI * n / (N - 1)));
}

export function hanning(N) {
  return Array.from({ length: N }, (_, n) => 0.5 * (1 - Math.cos(2 * Math.PI * n / (N - 1))));
}

export function blackman(N) {
  return Array.from({ length: N }, (_, n) =>
    0.42 - 0.5 * Math.cos(2 * Math.PI * n / (N - 1)) + 0.08 * Math.cos(4 * Math.PI * n / (N - 1))
  );
}

export function applyWindow(signal, window) {
  return signal.map((x, i) => x * window[i]);
}

// ===== Power Spectrum =====
export function powerSpectrum(signal) {
  const spectrum = fft(signal);
  return spectrum.map(c => c.magnitude() * c.magnitude() / signal.length);
}

export function magnitudeSpectrum(signal) {
  return fft(signal).map(c => c.magnitude());
}

// ===== Frequency Analysis =====
export function dominantFrequency(signal, sampleRate) {
  const spectrum = magnitudeSpectrum(signal);
  const halfN = Math.floor(spectrum.length / 2);
  let maxMag = 0, maxIdx = 0;
  for (let i = 1; i < halfN; i++) {
    if (spectrum[i] > maxMag) { maxMag = spectrum[i]; maxIdx = i; }
  }
  return maxIdx * sampleRate / spectrum.length;
}

// ===== Signal Generation =====
export function sine(freq, sampleRate, duration) {
  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, (_, n) => Math.sin(2 * Math.PI * freq * n / sampleRate));
}

export function cosine(freq, sampleRate, duration) {
  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, (_, n) => Math.cos(2 * Math.PI * freq * n / sampleRate));
}

export function squareWave(freq, sampleRate, duration) {
  return sine(freq, sampleRate, duration).map(x => x >= 0 ? 1 : -1);
}

// ===== Spectrogram =====
export function spectrogram(signal, windowSize = 256, hopSize = 128) {
  const win = hanning(windowSize);
  const frames = [];
  for (let start = 0; start + windowSize <= signal.length; start += hopSize) {
    const frame = signal.slice(start, start + windowSize);
    const windowed = applyWindow(frame, win);
    const spectrum = magnitudeSpectrum(windowed);
    frames.push(spectrum.slice(0, windowSize / 2));
  }
  return frames;
}
