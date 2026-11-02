
set -euo pipefail
cd "$(dirname "$0")/.."
echo "Starting MarketPulse infrastructure..."

docker compose up -d

echo "Checking Docker services..."
docker compose ps

echo "Starting Spark market processing..."
.venv/bin/python scripts/spark_kafka.py
