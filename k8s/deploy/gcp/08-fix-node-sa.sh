#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 修正：節點服務帳戶缺少 roles/container.defaultNodeServiceAccount
#
# 症狀（Console 會跳這個警告）：
#   「叢集內的節點服務帳戶缺少 roles/container.defaultNodeServiceAccount，
#     因此作業成效降低，例如記錄、監控或效能 HPA 作業受阻。」
#
# 為什麼會發生：
#   建叢集時沒指定 --service-account，node 就用 Compute Engine 預設服務帳戶
#   （PROJECT_NUMBER-compute@developer.gserviceaccount.com）。
#   以前這個帳戶會自動拿到 Editor 權限，但 **2024-05-03 之後建立的組織**
#   預設啟用 iam.automaticIamGrantsForDefaultServiceAccounts 限制，
#   不再自動授權——所以新專案一定會踩到這個。
#
# 影響到什麼（都不是致命，但會讓 Week 9 很難做）：
#   - Cloud Logging：node 寫不進系統日誌
#   - Cloud Monitoring：收不到 node 指標
#   - metrics-server / HPA：kubectl top 可能沒數字
#   - Artifact Registry：Week 8 拉自己的 image 可能被拒
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
NODE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo "專案編號    : $PROJECT_NUMBER"
echo "節點服務帳戶: $NODE_SA"
echo

# --- 先確認 node 真的在用這個帳戶 ------------------------------------------
ACTUAL_SA=$(gcloud container node-pools describe "$NODE_POOL" \
  --cluster="$CLUSTER_NAME" --zone="$ZONE" --project="$PROJECT_ID" \
  --format="value(config.serviceAccount)" 2>/dev/null)
echo "node pool 實際設定的服務帳戶：${ACTUAL_SA:-（未顯示）}"
if [ "$ACTUAL_SA" = "default" ] || [ -z "$ACTUAL_SA" ]; then
  echo "  → 'default' 就是指上面那個 Compute Engine 預設帳戶"
fi
echo

# --- 檢查目前有沒有這個角色 ------------------------------------------------
_info "檢查現有授權..."
HAS=$(gcloud projects get-iam-policy "$PROJECT_ID" \
  --flatten="bindings[].members" \
  --filter="bindings.role=roles/container.defaultNodeServiceAccount AND bindings.members:${NODE_SA}" \
  --format="value(bindings.role)" 2>/dev/null)

if [ -n "$HAS" ]; then
  _ok "已經有 roles/container.defaultNodeServiceAccount，不用處理"
  exit 0
fi

echo "  → 沒有，準備授權"
echo

# --- 授權 ------------------------------------------------------------------
_info "授予 roles/container.defaultNodeServiceAccount..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${NODE_SA}" \
  --role="roles/container.defaultNodeServiceAccount" \
  --condition=None \
  --format="value(etag)" >/dev/null

_ok "授權完成"

cat <<'EOT'

────────────────────────────────────────────────────────────
 這個角色包含什麼（等於以前 Editor 的一小塊，最小權限版）：
   roles/logging.logWriter              寫 Cloud Logging
   roles/monitoring.metricWriter        寫 Cloud Monitoring 指標
   roles/monitoring.viewer              讀指標
   roles/autoscaling.metricsWriter      HPA 用的指標
   roles/artifactregistry.reader        拉 image（Week 8 會用到）

 ⚠️ 不用重建叢集，也不用重啟 node——IAM 變更幾分鐘內生效。
    Console 的警告可能要等到 24 小時後才消失（recommender 更新頻率）。

 驗證（等幾分鐘後）：
   kubectl top nodes          # 有數字就代表 metrics 通了
   gcloud logging read 'resource.type="k8s_node"' --limit=5 --freshness=10m

 📝 Week 9 retrospective 素材：
    這是「預設服務帳戶不再自動給 Editor」的 GCP 政策變更造成的。
    正式環境的最佳實務是建**專屬的 node 服務帳戶**而不是用預設的，
    建叢集時加 --service-account=<自訂SA>。這個 PoC 沒做，理由是
    專案裡只有這一個叢集，沒有跨服務的權限隔離需求。
────────────────────────────────────────────────────────────
EOT
