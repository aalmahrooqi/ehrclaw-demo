# check if .env file exists AND NVIDIA_API_KEY is non-empty inside the file
if [ ! -f .env ] || [ -z "$(grep -E '^NVIDIA_API_KEY=' .env | cut -d '=' -f 2 | tr -d '\n')" ]; then
    echo "Error: .env file does not exist or NVIDIA_API_KEY is not set. Copy the .env.example file to .env and set the NVIDIA_API_KEY."
    exit 1
fi

set -a; source .env; set +a
echo "NVIDIA_API_KEY: $NVIDIA_API_KEY"

uv run nemoguardrails server --config ./configs/nemo_guardrails
