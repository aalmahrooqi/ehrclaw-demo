cd "$(dirname "$0")/.."

docker compose down
docker compose -f docker-compose.dashboard.yaml down
docker stop qwen36-vllm 2>/dev/null || true
