#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 清空 lakehouse 的資料層，讓 catalog 與物件儲存重新對齊。
#
# 什麼時候需要：
#   catalog 失憶之後（見 week8-decisions.md 坑 6）。
#   iceberg-rest 的 SQLite 沒了 → 它不知道有哪些表，
#   但 MinIO 裡還躺著上一輪的 Parquet 與 metadata.json。
#   直接重跑 flink-init.sql 會在同一個路徑上建一張「新的空表」，
#   舊檔案就永遠變成沒人指向、也沒人刪的孤兒——佔空間、佔錢，
#   而且日後 `mc ls` 看到會分不清哪些還有效。
#
# ⚠️ 這會刪資料，而且刪的是**已經救不回來的那份**。
#    如果你想留著舊資料，先別跑這支——
#    Trino 的 CALL iceberg.system.register_table() 可以把既有的
#    metadata.json 重新掛回 catalog。那條路要先去 MinIO 找出
#    每張表最新的 metadata.json 路徑，成功率不保證。
#
# 不會碰的東西：
#   - Kafka 的訊息（那是上游，重跑 job 就會重新讀）
#   - warehouse bucket 本身
#   - iceberg-rest 的 PVC（catalog DB 由 Flink 重建）
#
# 用法：
#   bash k8s/deploy/gcp/17-reset-lakehouse.sh
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

NS="$K8S_NAMESPACE"

# 要清掉的前綴。checkpoints / savepoints / flink-ha 也一起——
# 它們指向的 job 已經不存在，留著只會讓 Flink 復原時讀到半截狀態。
PREFIXES="bronze silver gold checkpoints savepoints flink-ha"

echo "════════════════════════════════════════════════════"
echo " 即將清空 s3://warehouse/ 底下的資料層"
echo "────────────────────────────────────────────────────"
echo "  namespace : ${NS}"
echo "  會刪除    : ${PREFIXES}"
echo "════════════════════════════════════════════════════"

echo
echo "── 目前 warehouse 的內容 ──────────────────────────"
kubectl exec -n "$NS" minio-0 -- sh -c 'du -sh /data/warehouse/* 2>/dev/null' \
  || echo "  （讀不到，MinIO 可能沒起來）"

echo
read -r -p "確認刪除？輸入 RESET 繼續： " CONFIRM
[ "$CONFIRM" = "RESET" ] || { echo "已取消"; exit 0; }

# 用 mc 而不是直接 rm -rf /data/... ——
# 走 S3 API 才會正確處理 MinIO 的內部中繼資料。
# 直接動檔案系統會留下不一致的 bucket 狀態。
echo
echo "→ 清除中..."
kubectl run mc-reset-$RANDOM -n "$NS" --rm -i --restart=Never --quiet \
  --image=quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z \
  --env="MC_USER=$(kubectl get secret streaming-secrets -n "$NS" -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)" \
  --env="MC_PASS=$(kubectl get secret streaming-secrets -n "$NS" -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)" \
  --env="PREFIXES=${PREFIXES}" \
  --command -- sh -c '
    set -u
    mc alias set local http://minio:9000 "$MC_USER" "$MC_PASS" >/dev/null 2>&1 || {
      echo "❌ 連不上 MinIO"; exit 1; }
    for p in $PREFIXES; do
      if mc ls "local/warehouse/${p}/" >/dev/null 2>&1; then
        mc rm --recursive --force "local/warehouse/${p}/" >/dev/null 2>&1
        echo "   已清除 ${p}/"
      else
        echo "   ${p}/ 本來就不存在，跳過"
      fi
    done
    # checkpoints 目錄要留著（minio-init 建過，Flink 預期它存在）
    mc mb --ignore-existing local/warehouse/checkpoints >/dev/null 2>&1
    echo
    echo "剩下的內容："
    mc ls --recursive local/warehouse/ | head -20
  '

# Flink 的 HA 中繼資料存在 K8s 的 ConfigMap，不在 S3——
# S3 清了但 ConfigMap 留著的話，JM 會嘗試復原一個檔案已經不見的 job，
# 症狀是啟動後不斷 RESTARTING 然後 FAILED。必須一起清。
echo
echo "→ 清除 Flink HA 的 ConfigMap..."
HACM=$(kubectl get cm -n "$NS" -o name 2>/dev/null | grep "streaming-poc-flink" || true)
if [ -n "$HACM" ]; then
  echo "$HACM" | sed 's/^/   /'
  echo "$HACM" | xargs -r kubectl delete -n "$NS" >/dev/null 2>&1 \
    && echo "   已刪除"
else
  echo "   沒有殘留的 HA ConfigMap"
fi

cat <<EOT

════════════════════════════════════════════════════
 清除完成。接下來要讓 catalog 與表重新長出來：

   # 1. 確認 iceberg-rest 已經是持久化版本
   kubectl exec -n ${NS} deploy/iceberg-rest -- printenv CATALOG_URI
   #    應為 jdbc:sqlite:file:/data/catalog.db

   # 2. 重建表並提交 job（FORCE 不需要，現在是 0 個 job）
   bash k8s/deploy/gcp/14-submit-flink.sh

   # 3. 等幾分鐘讓 checkpoint 跑一輪，再驗收
   bash k8s/deploy/gcp/16-verify-week8.sh
════════════════════════════════════════════════════
EOT
