#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 每週固定跑一次：現在有哪些東西在花錢。
#
# 計畫文件的「定期看帳單」＋ Week 9 的「成本回顧」都靠這個。
# gcloud 查不到即時金額（帳單資料有延遲，而且要 BigQuery export 才查得細），
# 所以這支的定位是「盤點在跑的資源」，金額請對照 Console。
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

echo "═══════════════════════════════════════════════════"
echo " 成本盤點 — $PROJECT_ID  $(date '+%Y-%m-%d %H:%M')"
echo "═══════════════════════════════════════════════════"

echo
echo "── 1. Compute 執行個體（縮到 0 的話應該是空的）────"
gcloud compute instances list --project="$PROJECT_ID" \
  --format="table(name,machineType.basename(),scheduling.provisioningModel,status)" || true

echo
echo "── 2. Persistent Disk（縮容也停不掉，閒置期主要成本）──"
gcloud compute disks list --project="$PROJECT_ID" \
  --format="table(name,sizeGb,type.basename(),users.len():label=ATTACHED)" || true
TOTAL_GB=$(gcloud compute disks list --project="$PROJECT_ID" \
  --format="value(sizeGb)" | awk '{s+=$1} END{print s+0}')
echo "   磁碟總容量：${TOTAL_GB} GB  ≈ \$$(awk "BEGIN{printf \"%.2f\", $TOTAL_GB*0.10}")/月 (pd-balanced 概估)"

echo
echo "── 3. 轉發規則 / LB（每條約 \$18/月，閒置也算）────"
gcloud compute forwarding-rules list --project="$PROJECT_ID" \
  --format="table(name,region.basename(),IPAddress)" 2>/dev/null || echo "   （無）"

echo
echo "── 4. 保留的靜態 IP（沒綁東西的 IP 反而更貴）────"
gcloud compute addresses list --project="$PROJECT_ID" \
  --format="table(name,address,status)" 2>/dev/null || echo "   （無）"

echo
echo "── 5. GKE 叢集（只該有一個）──────────────────────"
gcloud container clusters list --project="$PROJECT_ID" \
  --format="table(name,location,currentNodeCount,status)" || true

echo
cat <<EOT
═══════════════════════════════════════════════════
 實際金額請看：
   https://console.cloud.google.com/billing  →  Reports
   分組選 "Service"，就知道哪一項最貴（Week 9 retrospective 要這份拆解）

 已花超過 25% 且進度落後？照計畫文件的降級順序，一次只砍一項：
   1. 收工縮到 0（零功能損失）
   2. 不裝 Ingress，改 port-forward
   3. Trino 改成需要時才起
   4. Kafka 3-broker → 1-broker（最後才動，會失去 RF=3 驗收項）
═══════════════════════════════════════════════════
EOT
