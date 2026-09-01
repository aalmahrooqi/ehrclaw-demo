#!/usr/bin/env bashWhat can you do?
H
CliniClaw

As a clinical data expert, I surface what's actually in patient records and medication data. I can:

    Query FHIR R4 servers for lab results, vitals, conditions, medications, encounters, and allergies
    Resolve free-text medication names, brands, and generics to RxNorm CUIs
    Fetch openFDA drug labels and summarize interactions, boxed warnings, and contraindications
    Produce multi-domain chart summaries (one patient, all domains)
    Prescreen patients in connected FHIR datasets against trial criteria
    Parse messy drug names and surface what sources actually say
set -euo pipefail

MODEL="nvidia/NVIDIA-Nemotron-3.5-Lightning-30B-A3B-NVFP4"
DRAFT_MODEL="nvidia/NVIDIA-Nemotron-3.5-Lightning-30B-A3B-NVFP4-DSpark"
VLLM_IMAGE="vllm/vllm-openai:v0.27.1"
HF_CACHE="${XDG_CACHE_HOME:-$PWD/.cache}/huggingface"

mkdir -p "$HF_CACHE"

docker run -d --rm \
  --name nemotron-vllm \
  --gpus all \
  --ipc=host \
  --network=host \
  -e TORCH_ALLOW_TF32_CUBLAS_OVERRIDE=1 \
  -v "$HF_CACHE:/root/.cache/huggingface" \
  --entrypoint vllm \
  "$VLLM_IMAGE" \
  serve "$MODEL" \
  --served-model-name nemotron-3.5-lightning \
  --max-model-len 131072 \
  --max-num-seqs 8 \
  --moe-backend marlin \
  --max-num-batched-tokens 4096 \
  --kv-cache-dtype fp8 \
  --enable-prefix-caching \
  --gpu-memory-utilization 0.85 \
  --mamba-backend flashinfer \
  --mamba-cache-mode align \
  --speculative_config.model "$DRAFT_MODEL" \
  --speculative_config.num_speculative_tokens 3 \
  --reasoning-parser nemotron_v3 \
  --tool-call-parser qwen3_coder \
  --enable-auto-tool-choice \
  --host 0.0.0.0 \
  --port 8000
