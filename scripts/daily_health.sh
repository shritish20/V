#!/usr/bin/bash
API="http://localhost:8000"
METRICS=$(curl -s $API/api/metrics/live | jq .data)
ROLLBACKS=$(echo $METRICS | jq .counters.rollback_attempts)
if [[ $ROLLBACKS -gt 0 ]]; then
  curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
       -d chat_id=${TELEGRAM_CHAT_ID} \
       -d text="📊 EOD: $ROLLBACKS rollbacks today – verify logs"
fi
