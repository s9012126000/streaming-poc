#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 收工：node pool 縮到 0。
#
# 計畫文件的降級順序第 1 項，零功能損失、效果最大。
# 縮到 0 之後 pod 全部 Pending，這是預期行為——05-up.sh 擴回來就會自動排回去。
#
# ⚠️  縮到 0 停不掉的東西（這是 PoC 閒置期的主要成本來源）：
#     - PersistentDisk（PVC 只要存在就一直算錢，約 $0.10/GB/月）
#     - Load Balancer 轉發規則（每條約 $18/月，閒置也算）
#     - 叢集管理費（單一 zonal 叢集通常被免費額度覆蓋）
#     所以 Week 7 決定 MinIO-on-PVC vs GCS 時，這一條是關鍵差異。
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

_info "縮到 0 台..."
gcloud container clusters resize "$CLUSTER_NAME" \
  --node-pool="$NODE_POOL" \
  --num-nodes=0 \
  --zone="$ZONE" --project="$PROJECT_ID" --quiet

_ok "node pool 已縮到 0"

echo
echo "── 縮到 0 之後還在計費的資源 ──────────────────"
echo "PersistentVolumeClaim（每個都對應一顆 PD）："
kubectl get pvc -A --no-headers 2>/dev/null \
  | awk '{printf "  %-16s %-28s %s\n", $1, $2, $5}' || echo "  （無）"

echo
echo "LoadBalancer Service（每個一條轉發規則）："
kubectl get svc -A --field-selector spec.type=LoadBalancer --no-headers 2>/dev/null \
  | awk '{printf "  %-16s %s\n", $1, $2}' || echo "  （無）"

echo
echo "GCP 上的 Persistent Disk："
gcloud compute disks list --project="$PROJECT_ID" --filter="zone:($ZONE)" \
  --format="table(name,sizeGb,type)" 2>/dev/null || true

echo
echo "💤 收工完成。明天：bash k8s/deploy/gcp/05-up.sh"
