#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Week 6 / Step 3 — 建立 GKE Standard 叢集 + Spot node pool。
#
# ⚠️  這是第一支會產生費用的腳本。跑之前確認 02-budget-alert.sh 已完成。
#
# 幾個刻意的選擇：
#   --zone 而非 --region  : zonal 叢集只在一個 zone 開 node。
#                           regional 會在 3 個 zone 各開一組，成本 x3。
#   --spot                : 最大的單一省錢槓桿（約 6-9 折 → 2-3 折）。
#                           代價是 node 隨時可能被回收——正好拿來驗證 K8s 自我修復。
#   --num-nodes 2         : 固定大小，不開 autoscaling。
#                           原因見 05-nodepool-up.sh 裡的說明（autoscaler 會跟縮到 0 打架）。
#   預設 node pool 直接不要: 用 --num-nodes 建預設 pool 再刪很浪費，
#                           所以建叢集時就只開一個最小的 pool 然後換掉。
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

echo "════════════════════════════════════════════════════"
echo " 即將建立會計費的資源"
echo "────────────────────────────────────────────────────"
echo "  專案      : $PROJECT_ID"
echo "  叢集      : $CLUSTER_NAME"
echo "  Zone      : $ZONE  (zonal，非 regional)"
echo "  Node pool : $NODE_POOL"
echo "  機型      : $MACHINE_TYPE × $NODE_COUNT  (Spot VM)"
echo "  開機碟    : $DISK_TYPE ${DISK_SIZE}GB / node"
echo "════════════════════════════════════════════════════"
read -r -p "確認建立？(yes/no) " CONFIRM
[ "$CONFIRM" = "yes" ] || { echo "已取消"; exit 0; }

if gcloud container clusters describe "$CLUSTER_NAME" --zone="$ZONE" \
     --project="$PROJECT_ID" >/dev/null 2>&1; then
  _ok "叢集 $CLUSTER_NAME 已存在，跳過建立"
else
  _info "建立叢集（約 8-12 分鐘）..."
  #
  # ⚠️ 為什麼直接把預設 pool 開成 Spot，而不是「先開小 pool 再加 Spot pool」：
  #    E2_CPUS 配額只有 8，剛好等於 2 × e2-standard-4。
  #    若先開一台 e2-small（2 vCPU）再加 8 vCPU 的 Spot pool，
  #    中間會瞬間需要 10 vCPU → 直接被配額擋掉。
  #    所以一次到位，全程剛好貼齊 8。代價是 pool 名字固定叫 default-pool。
  #
  # ⚠️ --addons 是「白名單」：沒列到的預設 addon 會被關掉。
  #    GcePersistentDiskCsiDriver 一定要列，否則 Week 7 的 PVC 完全建不起來
  #    （新版 K8s 的 StorageClass 都走 CSI，in-tree provisioner 已移除）。
  #    HttpLoadBalancing 刻意不列——不裝就不會有 LB 在背景計費。
  #
  # ⚠️ --max-surge-upgrade=0：node 自動升級預設會多開一台再換掉舊的，
  #    那需要額外 4 vCPU 配額，我們沒有。改成不 surge、一次換一台。
  #
  gcloud container clusters create "$CLUSTER_NAME" \
    --project="$PROJECT_ID" \
    --zone="$ZONE" \
    --release-channel=regular \
    --machine-type="$MACHINE_TYPE" \
    --num-nodes="$NODE_COUNT" \
    --disk-type="$DISK_TYPE" \
    --disk-size="$DISK_SIZE" \
    --spot \
    --max-pods-per-node=32 \
    --node-labels=workload=streaming \
    --max-surge-upgrade=0 \
    --max-unavailable-upgrade=1 \
    --enable-ip-alias \
    --no-enable-master-authorized-networks \
    --workload-pool="${PROJECT_ID}.svc.id.goog" \
    --logging=SYSTEM \
    --monitoring=SYSTEM \
    --addons=HorizontalPodAutoscaling,GcePersistentDiskCsiDriver \
    --no-enable-basic-auth \
    --labels=project=streaming-poc,phase=week6
  _ok "叢集建立完成"
fi

# --- kubectl 認證 ---------------------------------------------------------
#
# ⚠️ 這段以前有個會咬人的 bug：
#    get-credentials 寫入的是 gke_<專案>_<zone>_<叢集> 這種長名字，
#    我們再把它改名成 streaming-poc。但如果 streaming-poc 這個名字
#    已經存在（上一個被刪掉的叢集留下的），rename 會失敗；
#    當時用 `|| true` 吞掉錯誤，接著 use-context streaming-poc 卻會成功——
#    指向**已經不存在的舊叢集**，症狀就是 kubectl 連到舊 IP 然後 i/o timeout。
#
#    所以現在改成：先把同名的舊 context 清掉，再改名。
#
_info "取得 kubectl 認證..."
gcloud container clusters get-credentials "$CLUSTER_NAME" \
  --zone="$ZONE" --project="$PROJECT_ID"

CTX="gke_${PROJECT_ID}_${ZONE}_${CLUSTER_NAME}"
ALIAS="streaming-poc"

# 若 ALIAS 已存在且不是指向這次的叢集，就是殘留，清掉
if kubectl config get-contexts -o name 2>/dev/null | grep -qx "$ALIAS"; then
  OLD_CLUSTER=$(kubectl config view -o jsonpath="{.contexts[?(@.name=='${ALIAS}')].context.cluster}")
  if [ "$OLD_CLUSTER" != "$CTX" ]; then
    _info "清除殘留的 context '${ALIAS}'（原本指向 ${OLD_CLUSTER}）"
    kubectl config delete-context "$ALIAS"  >/dev/null 2>&1 || true
    kubectl config delete-cluster "$OLD_CLUSTER" >/dev/null 2>&1 || true
    kubectl config delete-user    "$OLD_CLUSTER" >/dev/null 2>&1 || true
  fi
fi

kubectl config rename-context "$CTX" "$ALIAS" >/dev/null 2>&1 || true
kubectl config use-context "$ALIAS" >/dev/null 2>&1 || kubectl config use-context "$CTX" >/dev/null

_ok "kubectl context: $(kubectl config current-context)"
_info "指向的 API 端點：$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"

echo
_info "驗收 —— node 應該都是 Ready，且 SPOT 欄位為 true："
kubectl get nodes -o custom-columns=\
'NAME:.metadata.name,STATUS:.status.conditions[-1].type,MACHINE:.metadata.labels.node\.kubernetes\.io/instance-type,SPOT:.metadata.labels.cloud\.google\.com/gke-spot'

echo
cat <<EOT
────────────────────────────────────────────────────────────
💸 從現在起開始計費。收工時務必跑：
     bash k8s/deploy/gcp/06-down.sh

下一步：暖身部署
       kubectl apply -f k8s/base/namespace.yaml
       bash k8s/deploy/gcp/07-create-secrets.sh
       kubectl apply -f k8s/_archive/warmup/
────────────────────────────────────────────────────────────
EOT
