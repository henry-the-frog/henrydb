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

// ===== Cross-correlation =====
export function crossCorrelation(a, b) {
  // Cross-correlation via FFT: corr(a,b) = IFFT(FFT(a) * conj(FFT(b)))
  const N = nextPow2(a.length + b.length - 1);
  const fa = fft(pad(a, N));
  const fb = fft(pad(b, N));
  const product = fa.map((v, i) => v.mul(fb[i].conjugate()));
  const result = ifft(product);
  return result.slice(0, a.length + b.length - 1).map(c => c.re);
}

export function autoCorrelation(signal) {
  return crossCorrelation(signal, signal);
}

// ===== Zero-Padding Interpolation =====
export function zeroPadInterpolate(signal, factor) {
  // Interpolate by zero-padding in frequency domain
  const N = signal.length;
  const M = N * factor;
  const spectrum = fft(signal);
  // Insert zeros in the middle of the spectrum
  const padded = new Array(M).fill(null).map(() => new Complex(0, 0));
  const half = N / 2;
  for (let i = 0; i < half; i++) padded[i] = spectrum[i].scale(factor);
  for (let i = 0; i < half; i++) padded[M - half + i] = spectrum[N - half + i].scale(factor);
  // Handle Nyquist bin for even-length signals
  if (N % 2 === 0) {
    padded[half] = spectrum[half].scale(factor / 2);
    padded[M - half] = spectrum[half].scale(factor / 2);
  }
  return ifft(padded).map(c => c.re);
}

// ===== Digital Filters =====

// FIR filter design using windowed sinc method
export function firLowpass(cutoffFreq, sampleRate, numTaps) {
  const fc = cutoffFreq / sampleRate;
  const M = numTaps - 1;
  const h = new Array(numTaps);
  const win = hamming(numTaps);
  for (let n = 0; n <= M; n++) {
    if (n === M / 2) {
      h[n] = 2 * fc;
    } else {
      const x = n - M / 2;
      h[n] = Math.sin(2 * Math.PI * fc * x) / (Math.PI * x);
    }
    h[n] *= win[n];
  }
  // Normalize to unity gain at DC
  const sum = h.reduce((a, b) => a + b, 0);
  return h.map(v => v / sum);
}

export function firHighpass(cutoffFreq, sampleRate, numTaps) {
  const lp = firLowpass(cutoffFreq, sampleRate, numTaps);
  // Spectral inversion: negate all, add 1 to center tap
  const M = numTaps - 1;
  return lp.map((v, i) => i === M / 2 ? 1 - v : -v);
}

export function firBandpass(lowFreq, highFreq, sampleRate, numTaps) {
  const lp1 = firLowpass(highFreq, sampleRate, numTaps);
  const lp2 = firLowpass(lowFreq, sampleRate, numTaps);
  return lp1.map((v, i) => v - lp2[i]);
}

// Apply FIR filter via convolution
export function applyFIR(signal, coefficients) {
  return convolve(signal, coefficients).slice(0, signal.length);
}

// IIR filter — second-order biquad (Direct Form I)
// Coefficients: [b0, b1, b2, a1, a2] where a0 is normalized to 1
export function biquadLowpass(cutoffFreq, sampleRate, Q = 0.7071) {
  const w0 = 2 * Math.PI * cutoffFreq / sampleRate;
  const alpha = Math.sin(w0) / (2 * Q);
  const cosw0 = Math.cos(w0);
  const b0 = (1 - cosw0) / 2;
  const b1 = 1 - cosw0;
  const b2 = (1 - cosw0) / 2;
  const a0 = 1 + alpha;
  const a1 = -2 * cosw0;
  const a2 = 1 - alpha;
  return { b: [b0 / a0, b1 / a0, b2 / a0], a: [1, a1 / a0, a2 / a0] };
}

export function biquadHighpass(cutoffFreq, sampleRate, Q = 0.7071) {
  const w0 = 2 * Math.PI * cutoffFreq / sampleRate;
  const alpha = Math.sin(w0) / (2 * Q);
  const cosw0 = Math.cos(w0);
  const b0 = (1 + cosw0) / 2;
  const b1 = -(1 + cosw0);
  const b2 = (1 + cosw0) / 2;
  const a0 = 1 + alpha;
  const a1 = -2 * cosw0;
  const a2 = 1 - alpha;
  return { b: [b0 / a0, b1 / a0, b2 / a0], a: [1, a1 / a0, a2 / a0] };
}

export function biquadBandpass(centerFreq, sampleRate, Q = 1) {
  const w0 = 2 * Math.PI * centerFreq / sampleRate;
  const alpha = Math.sin(w0) / (2 * Q);
  const cosw0 = Math.cos(w0);
  const b0 = alpha;
  const b1 = 0;
  const b2 = -alpha;
  const a0 = 1 + alpha;
  const a1 = -2 * cosw0;
  const a2 = 1 - alpha;
  return { b: [b0 / a0, b1 / a0, b2 / a0], a: [1, a1 / a0, a2 / a0] };
}

// Apply biquad IIR filter (Direct Form I)
export function applyBiquad(signal, { b, a }) {
  const output = new Array(signal.length);
  let x1 = 0, x2 = 0, y1 = 0, y2 = 0;
  for (let n = 0; n < signal.length; n++) {
    const x0 = signal[n];
    output[n] = b[0] * x0 + b[1] * x1 + b[2] * x2 - a[1] * y1 - a[2] * y2;
    x2 = x1; x1 = x0;
    y2 = y1; y1 = output[n];
  }
  return output;
}

// Chain multiple biquad sections (cascaded second-order sections)
export function applyBiquadCascade(signal, filters) {
  let result = signal;
  for (const f of filters) result = applyBiquad(result, f);
  return result;
}

// ===== STFT with overlap-add synthesis =====
export function stft(signal, windowSize = 256, hopSize = 128) {
  const win = hanning(windowSize);
  const frames = [];
  for (let start = 0; start + windowSize <= signal.length; start += hopSize) {
    const frame = signal.slice(start, start + windowSize);
    const windowed = applyWindow(frame, win);
    frames.push(fft(windowed));
  }
  return { frames, windowSize, hopSize };
}

export function istft(stftData, outputLength) {
  const { frames, windowSize, hopSize } = stftData;
  const win = hanning(windowSize);
  const output = new Array(outputLength).fill(0);
  const windowSum = new Array(outputLength).fill(0);

  for (let i = 0; i < frames.length; i++) {
    const start = i * hopSize;
    const recovered = ifft(frames[i]);
    for (let j = 0; j < windowSize && start + j < outputLength; j++) {
      output[start + j] += recovered[j].re * win[j];
      windowSum[start + j] += win[j] * win[j];
    }
  }

  // Normalize by window sum to avoid amplitude modulation
  for (let i = 0; i < outputLength; i++) {
    if (windowSum[i] > 1e-8) output[i] /= windowSum[i];
  }

  return output;
}

// ===== Frequency Response =====
export function frequencyResponse(coefficients, numPoints = 512) {
  // Compute frequency response of FIR filter
  const N = numPoints;
  const magnitude = new Array(N / 2);
  const phase = new Array(N / 2);
  for (let k = 0; k < N / 2; k++) {
    const w = 2 * Math.PI * k / N;
    let re = 0, im = 0;
    for (let n = 0; n < coefficients.length; n++) {
      re += coefficients[n] * Math.cos(-w * n);
      im += coefficients[n] * Math.sin(-w * n);
    }
    magnitude[k] = Math.sqrt(re * re + im * im);
    phase[k] = Math.atan2(im, re);
  }
  return { magnitude, phase };
}

// ===== Utility: dB conversion =====
export function toDB(value) { return 20 * Math.log10(Math.max(value, 1e-10)); }
export function fromDB(db) { return Math.pow(10, db / 20); }

// ===== Goertzel Algorithm (single-frequency DFT, O(N)) =====
export function goertzel(signal, targetFreq, sampleRate) {
  const N = signal.length;
  const k = Math.round(targetFreq * N / sampleRate);
  const w = 2 * Math.PI * k / N;
  const coeff = 2 * Math.cos(w);
  let s0 = 0, s1 = 0, s2 = 0;
  for (let n = 0; n < N; n++) {
    s0 = signal[n] + coeff * s1 - s2;
    s2 = s1; s1 = s0;
  }
  const re = s1 - s2 * Math.cos(w);
  const im = s2 * Math.sin(w);
  return new Complex(re, im);
}

// ===== Cepstrum =====
export function cepstrum(signal) {
  const spectrum = fft(signal);
  const logMag = spectrum.map(c => new Complex(Math.log(Math.max(c.magnitude(), 1e-10)), 0));
  return ifft(logMag).map(c => c.re);
}

// ===== Noise generation =====
export function whiteNoise(length, amplitude = 1) {
  return Array.from({ length }, () => (Math.random() * 2 - 1) * amplitude);
}

// ===== Pitch detection (autocorrelation method) =====
export function detectPitch(signal, sampleRate, minFreq = 50, maxFreq = 2000) {
  const ac = autoCorrelation(signal);
  const minLag = Math.floor(sampleRate / maxFreq);
  const maxLag = Math.min(Math.floor(sampleRate / minFreq), signal.length - 1);
  let bestLag = minLag, bestVal = -Infinity;
  for (let lag = minLag; lag <= maxLag; lag++) {
    if (ac[lag] > bestVal) { bestVal = ac[lag]; bestLag = lag; }
  }
  return sampleRate / bestLag;
}
