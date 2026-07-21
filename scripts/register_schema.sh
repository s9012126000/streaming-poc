#!/usr/bin/env bash
# 明確註冊 market_trade schema —— producers 已關閉 auto.register.schemas，
# 送資料前必須先跑這支。docker compose 的 kafka-init 會自動執行它，
# 在 host 上也能手動跑（改了 avsc 之後）。
#
# 冪等：schema 內容沒變就回既有 id、不產生新版本；
# 不相容的變更會被 Registry 以 409 擋下並讓這支以非零 exit code 結束。
#
# 只用 curl + python3（cp-kafka image 沒有 jq）。
#
# Run:  bash scripts/register_schema.sh
set -euo pipefail
SR=${SCHEMA_REGISTRY:-http://localhost:8081}
SUBJECT=${SUBJECT:-market-trades-value}
AVSC=${AVSC:-schemas/market_trade.avsc}

py() { python3 -c "$@"; }

py 'import json,sys; print(json.dumps({"schema": open(sys.argv[1]).read()}))' "$AVSC" > /tmp/schema_payload.json

RESP=$(curl -s -w '\n%{http_code}' -X POST "$SR/subjects/$SUBJECT/versions" \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d @/tmp/schema_payload.json)
CODE=$(tail -1 <<<"$RESP"); BODY=$(sed '$d' <<<"$RESP")

if [ "$CODE" != "200" ]; then
  echo "註冊失敗 (HTTP $CODE): $BODY" >&2
  exit 1
fi
ID=$(py 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$BODY")
echo "$SUBJECT → schema id $ID  (versions: $(curl -s "$SR/subjects/$SUBJECT/versions"))"
