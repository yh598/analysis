#!/bin/bash
# Invisible background Slack Active script (no UI)
# Run: double-click KeepSlackActive.command or run from Terminal
while true; do
  osascript <<EOT
  tell application "System Events"
      key code 63
  end tell
EOT
  sleep 60
done
