#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 清除舊 region 的所有資源。
#
# 什麼時候用：
#   - 換 region（本次：us-central1 → asia-east1）
#   - 專案結束、要停止一切計費
#
# ⚠️ 這會刪掉資料。Kafka 的 topic 內容、MinIO 的 bucket 全部消失。
#    Week 7 的部署可以用 manifest 重建，但**資料不會回來**。
#
# 為什麼換 region 一定要先刪：
#   CPUS_ALL_REGIONS 配額是 12，而現有叢集 3 × e2-standard-4 已經用滿。
#   不先釋放，新 region 一台都開不起來。
#
# 用法：
#   bash k8s/deploy/gcp/00-teardown.sh us-central1 us-central1-c
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../../.."

# 刻意要求明確傳入舊的 region/zone，不要從 env.sh 讀——
# env.sh 這時候已經改成新的了，讀進來會刪錯東西（或什麼都刪不到）。
OLD_REGION="${1:-}"
OLD_ZONE="${2:-}"
if [ -z "$OLD_REGION" ] || [ -z "$OLD_ZONE" ]; then
  echo "用法：bash k8s/deploy/gcp/00-teardown.sh <舊REGION> <舊ZONE>"
  echo "例如：bash k8s/deploy/gcp/00-teardown.sh us-central1 us-central1-c"
  exit 1
fi

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
CLUSTER_NAME="${CLUSTER_NAME:-streaming-poc}"
AR_REPO="${AR_REPO:-streaming-poc}"

echo "════════════════════════════════════════════════════"
echo " 即將刪除以下資源（含資料）"
echo "────────────────────────────────────────────────────"
echo "  專案    : $PROJECT_ID"
echo "  舊 zone : $OLD_ZONE"
echo "════════════════════════════════════════════════════"

echo
echo "── 目前存在的東西 ──────────────────────────────"
gcloud container clusters list --project="$PROJECT_ID" \
  --format="table(name,location,currentNodeCount,status)" 2>/dev/null
echo
gcloud compute disks list --project="$PROJECT_ID" --filter="zone:($OLD_ZONE)" \
  --format="table(name,sizeGb,type.basename(),users.len():label=ATTACHED)" 2>/dev/null
echo
gcloud artifacts repositories list --project="$PROJECT_ID" \
  --format="table(name,format,location)" 2>/dev/null

echo
read -r -p "確認全部刪除？輸入 DELETE 繼續： " CONFIRM
[ "$CONFIRM" = "DELETE" ] || { echo "已取消"; exit 0; }

# --- 1. 叢集 --------------------------------------------------------------
# 刪叢集會連帶刪掉 node 和 node 的開機碟，但 **PVC 對應的磁碟不一定會刪**
# ——取決於 StorageClass 的 reclaimPolicy（我們用的 standard 是 Delete，
# 理論上會刪，但 GKE 有時會留下孤兒磁碟，所以下面再掃一次）。
if gcloud container clusters describe "$CLUSTER_NAME" --zone="$OLD_ZONE" \
     --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo
  echo "→ 刪除叢集 ${CLUSTER_NAME}（約 3-5 分鐘）..."
  gcloud container clusters delete "$CLUSTER_NAME" \
    --zone="$OLD_ZONE" --project="$PROJECT_ID" --quiet
  echo "✅ 叢集已刪除"
else
  echo "ℹ️  叢集 $CLUSTER_NAME 不存在於 ${OLD_ZONE}，跳過"
fi

# --- 2. 孤兒磁碟 ----------------------------------------------------------
# ★ 這一步最容易被忘記，而且是「刪了叢集卻繼續收費」的主因。
echo
echo "→ 檢查孤兒 Persistent Disk..."
ORPHANS=$(gcloud compute disks list --project="$PROJECT_ID" \
  --filter="zone:($OLD_ZONE) AND -users:*" --format="value(name)" 2>/dev/null)

if [ -n "$ORPHANS" ]; then
  echo "   找到未掛載的磁碟："
  echo "$ORPHANS" | sed 's/^/     /'
  for d in $ORPHANS; do
    gcloud compute disks delete "$d" --zone="$OLD_ZONE" \
      --project="$PROJECT_ID" --quiet && echo "   已刪 $d"
  done
else
  echo "   ✅ 沒有孤兒磁碟"
fi

# --- 3. kubeconfig 殘留 ---------------------------------------------------
# Week 6 踩過的坑：刪叢集不會清 kubeconfig，
# 留著的 context 會讓下次 kubectl 連到不存在的 IP。
echo
echo "→ 清理 kubeconfig..."
OLD_CTX="gke_${PROJECT_ID}_${OLD_ZONE}_${CLUSTER_NAME}"
for ctx in "$OLD_CTX" "streaming-poc"; do
  if kubectl config get-contexts -o name 2>/dev/null | grep -qx "$ctx"; then
    kubectl config delete-context "$ctx" >/dev/null 2>&1 && echo "   已刪 context $ctx"
  fi
done
kubectl config delete-cluster "$OLD_CTX" >/dev/null 2>&1 || true
kubectl config delete-user    "$OLD_CTX" >/dev/null 2>&1 || true

# --- 4. Artifact Registry（選用）------------------------------------------
# 換 region 時，舊 region 的 repo 留著也能用（跨區拉取），
# 但會產生跨區流量費，而且 Spot 頻繁換機時每次都要重拉。
echo
if gcloud artifacts repositories describe "$AR_REPO" \
     --location="$OLD_REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
  read -r -p "也刪除 $OLD_REGION 的 Artifact Registry repo？(yes/no) " DELREPO
  if [ "$DELREPO" = "yes" ]; then
    gcloud artifacts repositories delete "$AR_REPO" \
      --location="$OLD_REGION" --project="$PROJECT_ID" --quiet
    echo "✅ repo 已刪除（新 region 要重新 push image）"
  else
    echo "ℹ️  保留舊 repo。注意跨區拉取會產生流量費。"
  fi
fi

# --- 5. 確認 ---------------------------------------------------------------
echo
echo "════════════════════════════════════════════════════"
echo " 清理後的狀態"
echo "════════════════════════════════════════════════════"
gcloud compute instances list --project="$PROJECT_ID" \
  --format="table(name,zone.basename(),status)" 2>/dev/null || echo "  （無 VM）"
echo
gcloud compute disks list --project="$PROJECT_ID" \
  --format="table(name,zone.basename(),sizeGb)" 2>/dev/null || echo "  （無磁碟）"

cat <<'EOT'

────────────────────────────────────────────────────────────
 全域 vCPU 配額已釋放，可以在新 region 建叢集了。

 接下來：
   bash k8s/deploy/gcp/03-check-quotas.sh      # 確認新 region 的配額
   bash k8s/deploy/gcp/04-create-cluster.sh
   bash k8s/deploy/gcp/12-setup-registry.sh    # 在新 region 建 repo
   bash k8s/deploy/gcp/13-build-push.sh        # 重新推 image
   kubectl apply -f k8s/base/namespace.yaml
   bash k8s/deploy/gcp/07-create-secrets.sh
   bash k8s/deploy/deploy.sh
   bash k8s/deploy/gcp/16-verify-week8.sh
────────────────────────────────────────────────────────────
EOT
