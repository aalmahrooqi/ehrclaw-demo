export LOCAL_NIM_CACHE=~/.cache/nim
mkdir -p "$LOCAL_NIM_CACHE"
docker run -it --rm \
    --gpus all \
    --shm-size=16GB \
    -v "$LOCAL_NIM_CACHE:/opt/nim/.cache" \
    -p 8000:8000 \
    -e NIM_PASSTHROUGH_ARGS="--reasoning-parser nemotron_v3 --enable-auto-tool-choice --tool-call-parser qwen3_coder" \
    nvcr.io/nim/nvidia/nemotron-3.5-lightning-30b-a3b:latest
