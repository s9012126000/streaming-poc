#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 開工：把 node pool 擴回 ${NODE_COUNT}。
#
# 為什麼是手動 resize 而不是 cluster autoscaler min-nodes=0：
#   autoscaler 看到「有 pod 排不進去」就會擴。而 kube-dns / metrics-server
#   這些系統 pod 永遠存在，所以 min=0 的 pool 會被立刻拉回 1 台——縮不到 0。
#   真正要省錢就得明確 resize，代價是要自己記得跑。
#   （Week 9 可以把這兩支包成 Cloud Scheduler + Cloud Function 當延伸練習）
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

_info "擴回 $NODE_COUNT 台 Spot node（約 2-3 分鐘）..."
gcloud container clusters resize "$CLUSTER_NAME" \
  --node-pool="$NODE_POOL" \
  --num-nodes="$NODE_COUNT" \
  --zone="$ZONE" --project="$PROJECT_ID" --quiet

_info "等待 node Ready..."
kubectl wait --for=condition=Ready node --all --timeout=300s || true
kubectl get nodes

echo
_ok "可以開工了。收工記得：bash k8s/deploy/gcp/06-down.sh"
