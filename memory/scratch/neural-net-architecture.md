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

## Additional Modules (discovered late in audit, 80+ unaudited)
### Sequence Models
- **RNN/LSTM/GRU** (651 lines!) — full recurrent networks
- **RWKV** (174 lines, Peng 2023) — linear attention O(N) time
- **ESN** (Echo State Networks)

### Architecture Search / Meta-Learning
- **KAN** (300 lines, Liu 2024) — Kolmogorov-Arnold Networks with B-spline activations
- **MAML** (250 lines, Finn 2017) — meta-learning for fast adaptation
- **Neuroevolution** — evolutionary architecture search
- **AutoML** — automated model selection

### Neuroscience-Inspired
- **SNN** (321 lines) — Spiking Neural Networks with LIF neurons + STDP
- **Hopfield** (267 lines) — associative memory, energy minimization
- **NTM** (282 lines, Graves 2014) — Neural Turing Machine with external memory
- **SOM** — Self-Organizing Maps
- **Predictive Coding**

### Computer Vision
- **Capsule Networks** (267 lines, Sabour 2017) — dynamic routing
- **CLIP** (90 lines, Radford 2021) — contrastive language-image pretraining

### Continuous Models
- **Neural ODEs** (297 lines, Chen 2018) — continuous-depth networks
- **EBM** (216 lines) — energy-based models with contrastive divergence

### Reinforcement Learning
- **DQN** (370 lines) — Deep Q-Network
- **REINFORCE** — policy gradient

### LLM Engineering
- **RAG** (21 lines) — retrieval-augmented generation
- **Constitutional AI** (21 lines, Bai 2022) — self-critique
- **Think Tokens** (26 lines) — chain-of-thought (DeepSeek R1 style)
- **Scaling Laws** (24 lines, Hoffman 2022) — Chinchilla compute-optimal
- **Continuous Batching**, **Prefix Caching**, **Sliding Window**, **Rotary Cache**
- **Constrained Decoding**, **Token Healing**, **Multi-Token Prediction**
- **Position Interpolation** — context length extension
- **Prefix Tuning** — efficient fine-tuning

### Graph / Structured
- **GNN** (318 lines) — Graph Neural Networks
- **MDN** — Mixture Density Networks

### Utilities
- **Data Loader**, **Cross-Validation**, **Early Stopping**, **Callbacks**
- **Training Logger**, **Metrics**, **Label Smoothing**
- **Model Parallelism**, **Sequence Packing**, **Weight Tying**
- **EMA** — Exponential Moving Average

### Papers Verified Against (25+)
Vaswani 2017, Rafailov 2023, Schulman 2017, Ho 2020, Chen 2020 (SimCLR),
Hu 2021 (LoRA), Shazeer 2020 (SwiGLU), Su 2021 (RoPE), Dao 2022 (Flash Attention),
Gu 2023 (Mamba), Hinton 2015 (KD), Goodfellow 2014 (GAN), Kingma 2014 (VAE),
Lin 2017 (Focal), Holtzman 2020 (Top-p), Leviathan 2023 (Speculative),
Peebles 2023 (DiT/AdaLN), Rezende 2015 (Planar Flows), Dinh 2017 (RealNVP),
Liu 2019 (DARTS), Raffel 2020 (T5), Chen 2020 (MoCo), Zhang 2018 (Mixup),
Srivastava 2014 (Dropout), He 2015 (ResNet), Ioffe 2015 (BatchNorm),
Ba 2016 (LayerNorm), Wu 2018 (GroupNorm), Sennrich 2016 (BPE)

## Actual Gaps (post-comprehensive audit)
- Cross-attention: no backward pass
- Flash Attention/GQA/Mamba: no backward passes (forward-only)
- No Vision Transformer (ViT) specifically, though all components exist
