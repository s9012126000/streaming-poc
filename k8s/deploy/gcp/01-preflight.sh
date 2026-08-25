  #!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Week 6 / Step 1a — 開任何資源之前的檢查。
# 這支不建立任何會計費的東西，只讀狀態 + 啟用 API（啟用 API 本身免費）。
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

echo "════════════════════════════════════════════"
echo " Preflight — project: $PROJECT_ID"
echo "════════════════════════════════════════════"

# 1) 身分 ------------------------------------------------------------------
ACCOUNT=$(gcloud config get-value account 2>/dev/null)
[ -n "$ACCOUNT" ] || _die "還沒登入，跑 gcloud auth login"
_ok "登入身分：$ACCOUNT"

# 2) 專案存在且可存取 -------------------------------------------------------
gcloud projects describe "$PROJECT_ID" --format="value(projectId)" >/dev/null \
  || _die "看不到專案 ${PROJECT_ID}（不存在或沒權限）"
_ok "專案可存取：$PROJECT_ID"

# 3) 帳單有連結 ------------------------------------------------------------
BILLING_INFO=$(gcloud billing projects describe "$PROJECT_ID" \
  --format="value(billingEnabled,billingAccountName)" 2>/dev/null || echo "")
case "$BILLING_INFO" in
  True*) _ok "帳單已啟用：${BILLING_INFO#True	}" ;;
  *)     _die "專案沒連結帳單帳號。到 Console → Billing 綁定後再跑一次" ;;
esac

# 4) 試用額度狀態 ----------------------------------------------------------
cat <<'EOT'

⚠️  gcloud 沒有查「剩餘試用額度」的指令，這一項要用眼睛看：
    https://console.cloud.google.com/billing  →  左側 "Credits"
    記下：剩餘金額、到期日（自帳號建立起 90 天）
    到期後不會自動停機，帳單會直接開始算——所以下一步就是設預算警報。

EOT

# 4.5) kubectl 與 GKE 認證外掛 ---------------------------------------------
# kubectl 1.26 起，連 GKE 一定要透過 gke-gcloud-auth-plugin 這個外部執行檔，
# 它不會隨 gcloud 一起裝。少了它 gcloud 只會在建叢集「之後」才警告，
# 那時已經在計費了——所以挪到前面檢查。
if ! command -v kubectl >/dev/null 2>&1; then
  echo "⚠️  找不到 kubectl：gcloud components install kubectl"
elif ! command -v gke-gcloud-auth-plugin >/dev/null 2>&1; then
  cat <<'EOT'
⚠️  找不到 gke-gcloud-auth-plugin —— kubectl 將無法連上 GKE。

    先試：
      gcloud components install gke-gcloud-auth-plugin

    若回報 component manager 被停用（Homebrew 裝的 gcloud 會這樣）：
      brew install --cask gcloud-cli
    或直接裝外掛：
      brew install gke-gcloud-auth-plugin

    裝完驗證：
      gke-gcloud-auth-plugin --version
EOT
else
  _ok "gke-gcloud-auth-plugin 已安裝"
fi

# 5) 啟用必要 API ----------------------------------------------------------
APIS=(
  container.googleapis.com            # GKE
  compute.googleapis.com              # VM / PD / 網路
  artifactregistry.googleapis.com     # 之後推自己的 image（Week 8）
  cloudbilling.googleapis.com         # 讀帳單資訊
  billingbudgets.googleapis.com       # 預算警報
  monitoring.googleapis.com           # GKE 內建監控
  logging.googleapis.com
  iam.googleapis.com
  cloudresourcemanager.googleapis.com
)
_info "啟用 API（已啟用的會跳過，可能要等 1-2 分鐘）..."
gcloud services enable "${APIS[@]}" --project="$PROJECT_ID"
_ok "API 啟用完成"

echo
_info "已啟用清單："
gcloud services list --enabled --project="$PROJECT_ID" \
  --format="value(config.name)" | grep -E "container|compute|artifact|billing|monitoring" || true

echo
echo "════════════════════════════════════════════"
echo " 下一步：bash k8s/deploy/gcp/02-budget-alert.sh"
echo "════════════════════════════════════════════"
