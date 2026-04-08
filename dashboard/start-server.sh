#!/bin/bash
# start-server.sh — Launch dashboard server with proper environment
# Used by LaunchAgent com.henry.dashboard-server

# Source environment variables
if [ -f "$HOME/.openclaw/.env" ]; then
  export $(grep -v '^#' "$HOME/.openclaw/.env" | xargs)
fi

# Use system Node.js
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

cd "$(dirname "$0")"
exec node server.js
