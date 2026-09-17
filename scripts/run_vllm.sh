mkdir -p ~/hf-cache

docker run -d --name qwen36-vllm \
  --gpus all \
  --ipc=host \
  -p 8000:8000 \
  -v ~/hf-cache:/root/.cache/huggingface \
  vllm/vllm-openai:nightly \
  nvidia/Qwen3.6-35B-A3B-NVFP4 \
    --host 0.0.0.0 \
    --port 8000 \
    --tensor-parallel-size 1 \
    --trust-remote-code \
    --kv-cache-dtype fp8 \
    --attention-backend flashinfer \
    --gpu-memory-utilization 0.5 \
    --max-model-len 114688 \
    --max-num-seqs 4 \
    --max-num-batched-tokens 8192 \
    --enable-chunked-prefill \
    --async-scheduling \
    --enable-prefix-caching \
    --speculative-config '{"method":"mtp","num_speculative_tokens":3,"moe_backend":"triton"}' \
    --load-format fastsafetensors \
    --reasoning-parser qwen3 \
    --tool-call-parser qwen3_coder \
    --enable-auto-tool-choice