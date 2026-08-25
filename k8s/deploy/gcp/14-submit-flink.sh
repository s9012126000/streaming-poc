#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 把 Flink 的 SQL 與 PyFlink 檔案打包成 ConfigMap，然後提交 job。
#
# 為什麼要這一層轉換：
#   compose 用 bind mount 把 docker/flink/sql 和 jobs 掛進容器，
#   K8s 沒有 bind mount，改用 ConfigMap。
#
#   而檔案裡的 Kafka 位址是 compose 時代的 broker-1:29092,...，
#   K8s 上要換成 kafka-bootstrap:9092。
#
#   **原始檔案不動** —— 轉換只發生在打包成 ConfigMap 的過程，
#   這樣 docker compose up 仍然可用，兩邊維持單一份原始碼。
#
# 用法：
#   bash k8s/deploy/gcp/14-submit-flink.sh          # 冪等，已有 job 就跳過
#   FORCE=1 bash k8s/deploy/gcp/14-submit-flink.sh  # 改了 SQL 要重新提交
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

NS="$K8S_NAMESPACE"
FORCE="${FORCE:-0}"

# compose 時代的 Kafka 位址 → K8s Service
OLD_BOOTSTRAP="broker-1:29092,broker-2:29092,broker-3:29092"
NEW_BOOTSTRAP="kafka-bootstrap:9092"

# --- 1. 準備轉換後的檔案（放暫存目錄，不動原始檔）-------------------------
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/sql" "$TMP/jobs"

_info "複製並轉換 Kafka 位址"
echo "     ${OLD_BOOTSTRAP}"
echo "  →  ${NEW_BOOTSTRAP}"

for f in docker/flink/sql/*.sql; do
  sed "s|${OLD_BOOTSTRAP}|${NEW_BOOTSTRAP}|g" "$f" > "$TMP/sql/$(basename "$f")"
done
# __pycache__ 不要進 ConfigMap——那是二進位，而且會讓 ConfigMap 變大
for f in docker/flink/jobs/*.py; do
  sed "s|${OLD_BOOTSTRAP}|${NEW_BOOTSTRAP}|g" "$f" > "$TMP/jobs/$(basename "$f")"
done

# 確認轉換真的發生了——沒換到的話 job 會連不上 Kafka，
# 而錯誤訊息是「連線逾時」，很難聯想到是位址沒換
REMAIN=$(grep -rl "broker-1:29092" "$TMP" 2>/dev/null || true)
if [ -n "$REMAIN" ]; then
  _die "還有檔案含舊位址：$REMAIN"
fi
_ok "轉換完成，無殘留舊位址"

# --- 2. 建立 ConfigMap ----------------------------------------------------
_info "建立 ConfigMap flink-sql / flink-jobs"
kubectl create configmap flink-sql -n "$NS" \
  --from-file="$TMP/sql/" --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap flink-jobs -n "$NS" \
  --from-file="$TMP/jobs/" --dry-run=client -o yaml | kubectl apply -f -

echo
kubectl get configmap flink-sql flink-jobs -n "$NS" \
  -o custom-columns='NAME:.metadata.name,KEYS:.data' 2>/dev/null \
  | cut -c1-120

# --- 3. 前置檢查：JobManager 在嗎 -----------------------------------------
JM_READY=$(kubectl get deploy flink-jobmanager -n "$NS" \
             -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "")
if [ "${JM_READY:-0}" != "1" ]; then
  echo
  echo "❌ JobManager 尚未就緒（readyReplicas=${JM_READY:-無}）"
  kubectl get pods -n "$NS" -l app=flink 2>/dev/null
  echo
  echo "先處理它："
  echo "  kubectl apply -f k8s/30-compute/flink-jobmanager.yaml"
  echo "  kubectl logs -n $NS deploy/flink-jobmanager --tail=40"
  exit 1
fi
_ok "JobManager 已就緒"

TM_READY=$(kubectl get deploy flink-taskmanager -n "$NS" \
             -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
_info "TaskManager ready: ${TM_READY:-0} / 2"

# --- 4. 跑提交 Job --------------------------------------------------------
echo
_info "重建 flink-submit Job（FORCE=${FORCE}）"
kubectl delete job flink-submit -n "$NS" --ignore-not-found

# Job 的 spec 大多不可變更，所以用 delete + apply。
# FORCE 透過 patch 傳進去，不改 manifest 檔案。
kubectl apply -f k8s/30-compute/flink-submit-job.yaml
if [ "$FORCE" = "1" ]; then
  kubectl set env job/flink-submit -n "$NS" FORCE=1 2>/dev/null || true
fi

_info "等待完成（最多 10 分鐘）..."
if kubectl wait --for=condition=complete job/flink-submit \
     -n "$NS" --timeout=600s 2>/dev/null; then
  _ok "提交完成"
else
  echo "❌ Job 未在時限內完成"
  kubectl logs -n "$NS" job/flink-submit --all-containers --tail=60 || true
  exit 1
fi

echo
kubectl logs -n "$NS" job/flink-submit -c submit --tail=40

cat <<EOT

────────────────────────────────────────────────────────────
 看 Flink Dashboard：
   kubectl port-forward -n ${NS} svc/flink-jobmanager 8081:8081
   → http://localhost:8081

 確認 checkpoint 有寫進 MinIO：
   kubectl exec -n ${NS} minio-0 -- ls /data/warehouse/checkpoints

 確認 Iceberg 表有 Parquet：
   kubectl exec -n ${NS} minio-0 -- find /data/warehouse -name '*.parquet' | head
────────────────────────────────────────────────────────────
EOT
