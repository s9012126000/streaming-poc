#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 註冊 Avro schema 到 Schema Registry。
#
# 兩步：
#   1. 把 schemas/*.avsc 打包成 ConfigMap（保持本機檔案是唯一真相來源）
#   2. 跑 Job 去註冊
#
# 改了 .avsc 之後重跑這支即可——ConfigMap 會更新，Job 會重新註冊。
# 內容沒變的話 Schema Registry 會回既有 id，不產生新版本。
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

[ -d schemas ] || _die "找不到 schemas/ 目錄"

# 先確認 Schema Registry 真的 ready——不然 Job 只會卡在 initContainer，
# 錯誤訊息還會指向錯的地方
SR_READY=$(kubectl get deploy schema-registry -n "$K8S_NAMESPACE" \
             -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "")
if [ "${SR_READY:-0}" != "1" ]; then
  echo "❌ Schema Registry 尚未就緒（readyReplicas=${SR_READY:-無})"
  echo
  kubectl get pods -n "$K8S_NAMESPACE" -l app=schema-registry 2>/dev/null
  echo
  echo "先處理它，再回來跑這支："
  echo "  kubectl apply -f k8s/20-platform/schema-registry.yaml"
  echo "  kubectl logs -n $K8S_NAMESPACE deploy/schema-registry --tail=40"
  exit 1
fi
_ok "Schema Registry 已就緒"

_info "把 schemas/ 打包成 ConfigMap avro-schemas"
kubectl create configmap avro-schemas \
  --namespace="$K8S_NAMESPACE" \
  --from-file=schemas/ \
  --dry-run=client -o yaml | kubectl apply -f -

echo
kubectl describe configmap avro-schemas -n "$K8S_NAMESPACE" | sed -n '/^Data/,/^Events/p' | head -20

echo
_info "重建 schema-register Job"
# 先刪再建：Job 的 spec 大多不可變更，直接 apply 已存在的 Job 會被拒絕
kubectl delete job schema-register -n "$K8S_NAMESPACE" --ignore-not-found
kubectl apply -f k8s/20-platform/schema-register-job.yaml

_info "等待完成（最多 5 分鐘）..."
if kubectl wait --for=condition=complete job/schema-register \
     -n "$K8S_NAMESPACE" --timeout=300s 2>/dev/null; then
  _ok "註冊完成"
else
  echo "❌ Job 未在時限內完成，看日誌："
  kubectl logs -n "$K8S_NAMESPACE" job/schema-register --all-containers --tail=40 || true
  exit 1
fi

echo
kubectl logs -n "$K8S_NAMESPACE" job/schema-register -c register
