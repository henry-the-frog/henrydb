# Neural-net Architecture Reference — Compiled Apr 23, 2026 (Session B)

## Scale
- **168 source modules**, ~26K LOC
- **150 test files**, ~960 passing tests, 8 failing tests

## Gradient-Verified Modules (10/10 pass numerical checks)
| Module | Max Error | Paper/Standard |
|--------|-----------|----------------|
| Autograd (20 ops) | ~1e-10 | Chain rule composition |
| BatchNorm | 4.85e-10 | Ioffe & Szegedy 2015 |
| GroupNorm | 1.9e-5 | Wu & He 2018 |
| Conv2D (col2im) | 1.29e-12 | Standard CNN backward |
| LayerNorm | 2.49e-10 | Ba et al. 2016 |
| SelfAttention | 1.45e-13 | Vaswani 2017 |
| LoRA | 9.2e-13 | Hu et al. 2021 |
| RoPE | 1.77e-12 | Su et al. 2021 |
| SwiGLU | 1.66e-13 | Shazeer 2020 |
| Dense (ReLU) | 3.72e-13 | Standard MLP backward |

## End-to-End Models (2)
1. **char-lm.js** (831 lines): CharTokenizer → Embedding → Decoder Transformer → Text Gen. 13/13 tests.
2. **MicroGPT** (211 lines): Embedding → PE → TransformerEncoder → Dense(softmax). Working.

## Full LLM Pipeline
- Tokenizer: BPE (Sennrich 2016), byte-level, pipeline
- Embeddings: Token embedding with 1/√d init
- Position: Sinusoidal (Vaswani 2017), RoPE (Su 2021), T5 relative bias (Raffel 2020)
- Attention: Self, Cross, Multi-head, Flash (Dao 2022), GQA, Sparse (BigBird/Longformer)
- FFN: SwiGLU (Shazeer 2020), Dense, ReLU/GELU/SiLU
- Normalization: LayerNorm, BatchNorm, GroupNorm, RMSNorm (Zhang & Sennrich 2019)
- KV Cache: 2 impls (per-head, per-layer), no sliding window
- Sampling: Temperature, top-k, top-p (Holtzman 2020), repetition penalty (Keskar 2019), beam search
- Speculative Decoding: Correct (Leviathan/Chen 2023)

## Training Infrastructure
- AdamW (Loshchilov & Hutter 2019) — HAS BUG: step counter per-param not per-step
- SGD Momentum, AdaGrad, RMSprop, Lion (Chen 2023)
- LR Schedule: Cosine with warmup, WSD, StepDecay, OneCycle (Smith 2018)
- Gradient Accumulator: Micro-batch accumulation
- Mixed Precision: FP16/BF16 simulation + dynamic loss scaling
- LoRA (Hu 2021): Correct rank-r decomposition, merge for inference

## RLHF / Alignment
- PPO (Schulman 2017): Clipped surrogate, GAE, KL penalty. 8/8 tests.
- DPO (Rafailov 2023): Numerically stable, correct KL direction. 6/6 tests.
- Reward Model: Bradley-Terry — HAS BUG: bias gradients never computed

## Architecture Components
- Autograd: 18 differentiable ops, computation graph
- Residual connections (He 2015)
- Dropout (Srivastava 2014, inverted)
- Conv2D/Conv1D/MaxPool2D
- MoE (Shazeer 2017): Router + top-k + load balancing
- DARTS (Liu 2019): Differentiable NAS
- Weight Init: Xavier (Glorot 2010), Kaiming (He 2015), orthogonal

## Generative Models
- GAN (Goodfellow 2014): Non-saturating loss, label smoothing
- VAE (Kingma & Welling 2014): Reparameterization trick, β-VAE
- DDPM (Ho 2020): Forward + denoising, linear + cosine schedules
- Normalizing Flows: Planar (Rezende 2015) + Affine Coupling (Dinh 2017, RealNVP)

## Other
- Quantization: INT8 absmax + per-channel (5.3x compression)
- Pruning: Magnitude, structured, Lottery Ticket (Frankle 2019), gradual (Zhu 2017)
- Knowledge Distillation (Hinton 2015): Temperature-scaled soft labels
- Contrastive Learning / SimCLR (Chen 2020): NT-Xent loss
- Data Augmentation: Mixup (Zhang 2018), CutMix (Yun 2019)
- Loss Functions: MSE, cross-entropy, focal (Lin 2017), Dice, Hinge, Huber
- AdaLN (Peebles & Xie 2023): DiT adaptive layer norm

## Known Bugs (found in Session B, 5 total)
1. **8 broken test files** — 3 root causes:
   - rope.js: missing `applyRoPEToSequence` export (5 files affected)
   - MultiHeadFlashAttention: API mismatch (object vs positional args → NaN)
   - MoE: serialization uses wrong property names (W1/b1 vs up/down)
2. **AdamW step counter**: Increments per-param not per-optimizer-step
3. **Reward model bias**: db1/db2 gradients never computed
4. **MSE loss gradient**: Missing 1/n factor (forward divides by 2n but grad is just p-t)
5. **Pruning**: >= vs > threshold inconsistency between Matrix/Array paths
6. **GELU missing**: In activation.js (Dense can't use it), only in activation-functions.js

## Gaps
- No LSTM/RNN/GRU
- No Vision Transformer (ViT)
- No multi-task learning
- No data loader utility
- Cross-attention: no backward pass
- Flash Attention/GQA/Mamba: no backward passes (forward-only)
