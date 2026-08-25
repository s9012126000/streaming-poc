#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 02-budget-alert.sh 拿到 INVALID_ARGUMENT 時用這支。
#
# GCP 的 INVALID_ARGUMENT 不會告訴你是哪個參數錯，所以做兩件事：
#   A. --log-http 把 API 真正的回應挖出來（裡面通常有 fieldViolations）
#   B. 從最小可行指令開始，一次加一個參數，看哪一個加下去就爆
#
# 每次測試都用不同的 display name，成功的話會留下垃圾預算，最後統一清掉。
# ---------------------------------------------------------------------------
set -uo pipefail          # 刻意不用 -e：這支就是要讓指令失敗然後繼續跑
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

if [ -z "$BILLING_ACCOUNT" ]; then
  BILLING_ACCOUNT=$(gcloud billing projects describe "$PROJECT_ID" \
    --format="value(billingAccountName)" | sed 's|billingAccounts/||')
fi

SUFFIX=$(date +%s)
CREATED=()

# 跑一次測試：$1=標籤，其餘=額外參數
try() {
  local label="$1"; shift
  local name="dbg-${SUFFIX}-${label}"
  printf '\n───── [%s] ─────\n' "$label"
  printf '    ' ; printf '%q ' "$@" ; printf '\n'
  if gcloud billing budgets create \
       --billing-account="$BILLING_ACCOUNT" \
       --display-name="$name" \
       "$@" >/dev/null 2>/tmp/budget-err.txt; then
    echo "    ✅ 成功"
    CREATED+=("$name")
    return 0
  else
    echo "    ❌ 失敗：$(grep -m1 ERROR /tmp/budget-err.txt || cat /tmp/budget-err.txt)"
    return 1
  fi
}

echo "═══════════════════════════════════════════════════════"
echo " A. 先看 API 真正在抱怨什麼"
echo "═══════════════════════════════════════════════════════"
echo "（--log-http 會印出完整請求/回應，只抓 error 區塊）"

gcloud billing budgets create \
  --billing-account="$BILLING_ACCOUNT" \
  --display-name="dbg-${SUFFIX}-loghttp" \
  --budget-amount=300USD \
  --filter-projects="projects/${PROJECT_ID}" \
  --threshold-rule=percent=0.5 \
  --log-http 2>&1 \
  | grep -iE '"(message|description|field|reason|status)"|fieldViolations|badRequest' \
  | head -20
echo "（上面若有 field / description，就是確切原因）"

echo
echo "═══════════════════════════════════════════════════════"
echo " B. 逐步加參數，找出是哪一個造成 INVALID_ARGUMENT"
echo "═══════════════════════════════════════════════════════"

# 1) 最小：金額不帶幣別（用帳單帳號自己的幣別）
try "01-bare"        --budget-amount=300

# 2) 金額帶 USD —— 帳單帳號若是 TWD，這裡會爆
try "02-usd"         --budget-amount=300USD

# 3) 金額帶 TWD
try "03-twd"         --budget-amount=9000TWD

# 4) 最小 + 一條門檻
try "04-threshold"   --budget-amount=300 --threshold-rule=percent=0.5

# 5) 最小 + 專案過濾 —— filter-projects 格式錯的話這裡會爆
try "05-filter"      --budget-amount=300 --filter-projects="projects/${PROJECT_ID}"

# 6) 兩條同 percent、不同 basis —— 重複門檻若不被接受，這裡會爆
try "06-dup-percent" --budget-amount=300 \
                     --threshold-rule=percent=1.0 \
                     --threshold-rule=percent=1.0,basis=forecasted-spend

echo
echo "═══════════════════════════════════════════════════════"
echo " 判讀"
echo "═══════════════════════════════════════════════════════"
cat <<'EOT'
  01 就失敗            → 不是參數問題，是權限或帳單帳號狀態
                         （需要 Billing Account Administrator）
  01 成功、02 失敗     → 幣別不符。帳單帳號不是 USD，
                         把 env.sh 的金額改成不帶幣別即可
  01 成功、05 失敗     → filter-projects 的問題
  01 成功、06 失敗     → 同一個 percent 不能有兩條規則，拿掉其中一條
EOT

# --- 清掉測試產生的垃圾預算 ------------------------------------------------
if [ ${#CREATED[@]} -gt 0 ]; then
  echo
  echo "清除測試預算 ${#CREATED[@]} 筆..."
  for n in "${CREATED[@]}"; do
    id=$(gcloud billing budgets list --billing-account="$BILLING_ACCOUNT" \
         --filter="displayName=$n" --format="value(name)" 2>/dev/null)
    [ -n "$id" ] && gcloud billing budgets delete "$id" \
      --billing-account="$BILLING_ACCOUNT" --quiet 2>/dev/null \
      && echo "  已刪 $n"
  done
fi

echo
echo "把 A 段的輸出和 B 段的成功/失敗結果貼回來。"
