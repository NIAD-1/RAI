#!/usr/bin/env bash
# simulate_telegram.sh — Send a mock Telegram webhook request to the local server.
#
# Usage:
#   ./scripts/simulate_telegram.sh "Impact of AI on Supply Chain 2025"

set -euo pipefail

TOPIC="${1:-Impact of Artificial Intelligence on Modern Agriculture}"
HOST="${RESEARCH_BOT_HOST:-http://localhost:3000}"

echo "🎓 Sending mock Telegram message to Professor AI..."
echo "   Topic: ${TOPIC}"
echo ""

curl -s -X POST "${HOST}/webhook/telegram" \
  -H "Content-Type: application/json" \
  -d "{
    \"update_id\": 1234567,
    \"message\": {
      \"message_id\": 1,
      \"from\": {
        \"id\": 999999,
        \"first_name\": \"Local Simulator\"
      },
      \"chat\": {
        \"id\": 999999
      },
      \"text\": \"${TOPIC}\"
    }
  }"

echo ""
echo "✅ Done. Check server logs."
