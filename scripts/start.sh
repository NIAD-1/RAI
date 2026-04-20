#!/bin/bash
# start.sh — Orchestration script for Professor AI

# Ensure the bridge knows where the backend is
export BACKEND_URL="http://localhost:${PORT:-3000}"

echo "🎓 Starting Professor AI Stack..."

# 1. Start the Rust Backend in the background
echo "🦀 Starting Rust Backend on port ${PORT:-3000}..."
research-bot &
BACKEND_PID=$!

# 2. Start the Node.js Bridge
# We run this in the foreground so the container stays alive as long as the bridge is up
echo "🌉 Starting WhatsApp Bridge..."
cd bridge && node index.js &
BRIDGE_PID=$!

# Handle shutdown signals
trap "kill $BACKEND_PID $BRIDGE_PID; exit" SIGINT SIGTERM

# Wait for both processes
wait $BACKEND_PID $BRIDGE_PID
