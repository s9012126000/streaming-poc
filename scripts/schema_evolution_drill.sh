#!/usr/bin/env bash
# Schema 演進演練 (Week 4). 對 market-trades-value subject 演練治理機制。
# 全程唯讀 — 只用 Schema Registry 的 /compatibility/ 端點做 dry-run 檢查，
# 不註冊任何版本、不改任何設定。記錄輸出到 week4 doc。
set -uo pipefail
SR=${SCHEMA_REGISTRY:-http://localhost:8081}
SUBJECT=market-trades-value

echo "== 1. 目前相容模式 =="
# subject 層沒設就繼承 global（預設 BACKWARD），演練不需要寫入設定
SUB_CFG=$(curl -s "$SR/config/$SUBJECT" | jq -r '.compatibilityLevel // empty')
if [ -n "$SUB_CFG" ]; then
  echo "subject 層: $SUB_CFG"
else
  echo "subject 層未設定 → 繼承 global: $(curl -s "$SR/config" | jq -r '.compatibilityLevel')"
fi

# helper: 拿 avsc 對 latest 版本做相容性 dry-run，印 true/false
# 注意 jq 的 `//` 會把 false 當空值吃掉，所以用 has() 判斷而非 `.is_compatible // .message`
check() { curl -s -X POST "$SR/compatibility/subjects/$SUBJECT/versions/latest" \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data-binary @<(jq -Rs '{schema: .}' "$1") \
  | jq -r 'if has("is_compatible") then .is_compatible else .message end'; }

echo "== 2. 相容演進：v2 新增 nullable 欄位 ingest_source (default null) — 預期 true =="
V2=$(mktemp); jq '.fields += [{"name":"ingest_source","type":["null","string"],"default":null,"doc":"upstream source tag"}]' \
  schemas/market_trade.avsc > "$V2"
echo "is_compatible=$(check "$V2")  (舊 Bronze consumer 應繼續正常消費，不需改動)"

echo "== 3a. 刪除必填欄位 price — 預期 true（BACKWARD 允許！）=="
# 反直覺但正確：BACKWARD = 新 schema 能讀舊資料，刪欄位時新 reader 只是忽略它。
# 想擋住刪欄位要用 FORWARD 或 FULL。Registry 不會替你攔這種變更。
DEL=$(mktemp); jq '.fields |= map(select(.name != "price"))' schemas/market_trade.avsc > "$DEL"
echo "is_compatible=$(check "$DEL")"

echo "== 3b. 不相容演進：新增必填欄位 venue_id（無 default）— 預期 false =="
BAD=$(mktemp); jq '.fields += [{"name":"venue_id","type":"string","doc":"required, no default"}]' \
  schemas/market_trade.avsc > "$BAD"
echo "is_compatible=$(check "$BAD")  (Registry 應擋下；真的 POST 註冊會收到 409)"

echo "== 4. source_type string->enum 相容性檢查 — 預期 false =="
ENUM=$(mktemp); jq '(.fields[] | select(.name=="source_type") | .type) |=
  {"type":"enum","name":"SourceType","symbols":["ALPACA_STOCK","BINANCE_CRYPTO"]}' \
  schemas/market_trade.avsc > "$ENUM"
echo "is_compatible=$(check "$ENUM")"

rm -f "$V2" "$DEL" "$BAD" "$ENUM"
