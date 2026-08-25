#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Week 6 / Step 1b — 預算警報。
#
# 計畫文件把這件事排在所有事情之前，順序是刻意的：
# 試用額度用完或到期後 GCP 不會停機，帳單直接開始算。
# 警報是唯一會主動告訴你「該收手了」的機制。
#
# 門檻：$300 的 25% / 50% / 80%，另加 100% forecasted（預測會超支就先叫）。
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

# 1) 找出帳單帳號 ----------------------------------------------------------
if [ -z "$BILLING_ACCOUNT" ]; then
  BILLING_ACCOUNT=$(gcloud billing projects describe "$PROJECT_ID" \
    --format="value(billingAccountName)" | sed 's|billingAccounts/||')
fi
[ -n "$BILLING_ACCOUNT" ] || _die "抓不到帳單帳號，手動 export BILLING_ACCOUNT=XXXXXX-XXXXXX-XXXXXX"
_ok "帳單帳號：$BILLING_ACCOUNT"

# 2) 檢查有沒有已存在的同名預算（避免重複建立）------------------------------
EXISTING=$(gcloud billing budgets list --billing-account="$BILLING_ACCOUNT" \
  --filter="displayName=$BUDGET_NAME" --format="value(name)" 2>/dev/null || true)

if [ -n "$EXISTING" ]; then
  echo "ℹ️  已存在同名預算：$BUDGET_NAME"
  echo "    如要重建：gcloud billing budgets delete $EXISTING --billing-account=$BILLING_ACCOUNT"
  gcloud billing budgets describe "$EXISTING" --billing-account="$BILLING_ACCOUNT" \
    --format="yaml(displayName,amount,thresholdRules)"
  exit 0
fi

# 3) 建立預算 --------------------------------------------------------------
# --filter-projects 讓預算只算這個專案，不會被同帳單下其他專案污染。
#
# ⚠️ 格式一定是 projects/{PROJECT_ID}，用 project NUMBER 會被 API 打回
#    INVALID_ARGUMENT，而且錯誤訊息不會指出是哪個參數錯。
#    其他 GCP API 大多兩種都吃，budgets 這支只吃 ID——是個真的會踩到的坑。
#
# 金額幣別同樣是坑：帶了後綴就必須與帳單帳號幣別相符，否則一樣是
# INVALID_ARGUMENT。這個帳號是 TWD，所以是 9000TWD 不是 300USD。
_info "建立預算 ${BUDGET_NAME}（${BUDGET_AMOUNT} ${BUDGET_CURRENCY:-帳戶預設幣別}，門檻 25/50/80% + 100% 預測）..."

set -x
gcloud billing budgets create \
  --billing-account="$BILLING_ACCOUNT" \
  --display-name="$BUDGET_NAME" \
  --budget-amount="${BUDGET_AMOUNT}${BUDGET_CURRENCY}" \
  --filter-projects="projects/${PROJECT_ID}" \
  --threshold-rule=percent=0.25 \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.8 \
  --threshold-rule=percent=1.0 \
  --threshold-rule=percent=1.0,basis=forecasted-spend
{ set +x; } 2>/dev/null

_ok "預算建立完成"

cat <<EOT

────────────────────────────────────────────────────────────
⚠️  還沒完成——預設只會寄給「帳單帳號管理員」的 email。
    驗收清單要求「測試過通知確實送達」，所以請到 Console 確認：

    https://console.cloud.google.com/billing/$BILLING_ACCOUNT/budgets

    在預算的 "Manage notifications" 檢查：
      □ Email recipients 有你會看的信箱
      □ （選配）連到 Pub/Sub topic，之後可以接到 Discord ——
        這跟 Week 4 的告警管線是同一套思路，是個不錯的延伸

💡 為什麼有 forecasted-spend 這一條：
    實際花費到 80% 才叫，往往已經來不及縮容。
    forecasted 是「照目前速率會超支」就先叫，才有反應時間。
────────────────────────────────────────────────────────────

下一步：bash k8s/deploy/gcp/03-check-quotas.sh
EOT
