#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 把本機 .env 的內容變成 K8s Secret。
#
# 為什麼用腳本而不是寫一份 secret.yaml：
#   Secret 的 YAML 裡是 base64，不是加密——commit 進 git 等於明文外洩。
#   驗收清單那條「敏感資訊不出現在 manifest 或 env 明文」，
#   靠的就是「manifest 只引用 secretKeyRef，值從本機 .env 現場生成」。
#
# ⚠️ K8s Secret 預設在 etcd 裡也只是 base64。真正的作法是
#    Secret Manager + Workload Identity（Week 7 的選型題）。
#    這裡先用 K8s Secret 跑通，把「為什麼要換」的理由留給 Week 7 記錄。
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

[ -f .env ] || _die "找不到 .env"

_info "從 .env 建立 Secret streaming-secrets（namespace: ${K8S_NAMESPACE}）"

kubectl create secret generic streaming-secrets \
  --namespace="$K8S_NAMESPACE" \
  --from-env-file=.env \
  --dry-run=client -o yaml | kubectl apply -f -

_ok "Secret 已套用"

echo
echo "── 確認：只看得到 key，看不到值 ───────────────"
kubectl describe secret streaming-secrets -n "$K8S_NAMESPACE" | sed -n '/^Data/,$p'

cat <<'EOT'

驗證 Secret 真的注入了（暖身 pod 部署後跑）：
  kubectl exec -n streaming deploy/warmup -- printenv MINIO_ROOT_USER

反過來確認「manifest 裡沒有明文」：
  grep -rn "ALPACA\|DISCORD_TOKEN\|MINIO_ROOT_PASSWORD" k8s/ || echo "✅ k8s/ 底下沒有任何憑證明文"
EOT
