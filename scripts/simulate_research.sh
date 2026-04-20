#!/usr/bin/env bash
# simulate_research.sh — Send a test research request to the local server.
#
# Usage:
#   ./scripts/simulate_research.sh "Impact of AI on Supply Chain 2025"
#   ./scripts/simulate_research.sh "Renewable Energy in Lagos" premium

set -euo pipefail

TOPIC="${1:-Impact of Artificial Intelligence on Modern Agriculture}"
TIER="${2:-lite}"
HOST="${RESEARCH_BOT_HOST:-http://localhost:3000}"

echo "🎓 Sending research request to Professor AI..."
echo "   Topic: ${TOPIC}"
echo "   Tier:  ${TIER}"
echo ""

curl -s -X POST "${HOST}/simulate" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"${TOPIC}\", \"tier\": \"${TIER}\"}" | python3 -m json.tool 2>/dev/null || \
curl -s -X POST "${HOST}/simulate" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"${TOPIC}\", \"tier\": \"${TIER}\"}"

echo ""
echo "✅ Done."
