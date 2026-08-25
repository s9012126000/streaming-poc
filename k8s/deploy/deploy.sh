#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 依層次部署整套系統到 GKE。
#
# 為什麼要分層而不是 `kubectl apply -f k8s/ -R`：
#   K8s 是宣告式的，理論上你把所有 manifest 丟進去，controller 會自己收斂。
#   實務上不行，因為**有些依賴不是 K8s 能表達的**：
#     - Kafka 要先有 topic，Flink 的 job 才提交得起來
#     - Iceberg 的表要先建立，Trino 才查得到
#     - Schema Registry 要先註冊 schema，producer 送出的 Avro 才解得開
#   這些是「一次性動作」，K8s 用 Job 表達，而 Job 之間沒有依賴機制。
#
#   所以分層的本質是：**把 K8s 表達不了的順序，寫在腳本裡。**
#   同一層內的東西可以任意順序（靠 initContainer 互相等待），
#   層與層之間才需要 barrier。
#
# 用法：
#   bash k8s/deploy/deploy.sh              全部（從 base 開始）
#   bash k8s/deploy/deploy.sh --from 30    從 30-compute 開始（前面已部署過）
#   bash k8s/deploy/deploy.sh --only 50    只部署 50-monitoring
#   bash k8s/deploy/deploy.sh --dry-run    只印會做什麼，不實際執行
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../.."
# shellcheck source=/dev/null
[ -f k8s/deploy/gcp/env.sh ] && source k8s/deploy/gcp/env.sh
source k8s/deploy/lib.sh

FROM=0; ONLY=""; DRY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --from) FROM="${2:-0}"; shift 2 ;;
    --only) ONLY="${2:-}";  shift 2 ;;
    --dry-run) DRY=1; shift ;;
    -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    *) _die "未知參數：$1" ;;
  esac
done

_should() {   # 這一層要不要跑
  local n="$1"
  [ -n "$ONLY" ] && { [ "$ONLY" = "$n" ] && return 0 || return 1; }
  [ "$n" -ge "$FROM" ]
}

if [ "$DRY" = "1" ]; then
  echo "（dry-run）會依序部署："
  for l in base 10-storage 20-platform 30-compute 40-app 50-monitoring; do
    echo "  ${l}/"; ls -1 "k8s/${l}" 2>/dev/null | sed 's/^/     /'
  done
  exit 0
fi

echo "════════════════════════════════════════════════════════"
echo " 部署 streaming-poc → namespace: ${NS}"
echo "════════════════════════════════════════════════════════"
kubectl cluster-info >/dev/null 2>&1 || _die "連不上叢集。先跑 bash k8s/deploy/gcp/05-up.sh"
_info "叢集：$(kubectl config current-context)"

# ═══════════════════════════════════════════════════════════
# base — Namespace / ResourceQuota / LimitRange
# ═══════════════════════════════════════════════════════════
if _should 0; then
  _hdr "base — Namespace"
  kubectl apply -f k8s/base/namespace.yaml >/dev/null || _die "namespace 失敗"
  _ok "namespace ${NS}"

  # 憑證要在任何 pod 起來之前就位。
  # 少一個 key 的症狀是 pod 卡在 CreateContainerConfigError，
  # 而那個狀態不會出現在 rollout status 裡，只會一直等到逾時。
  require_secret_keys streaming-secrets \
    MINIO_ROOT_USER MINIO_ROOT_PASSWORD CATALOG_DB_PASSWORD \
    DISCORD_TOKEN DISCORD_CHANNEL_ID DISCORD_WEBHOOK \
    ALPACA_API_KEY ALPACA_SECRET_KEY
fi

# ═══════════════════════════════════════════════════════════
# 10-storage — MinIO（物件儲存）+ Postgres（catalog 後端）
#
# 最底層：這兩個是「別人的狀態放在哪裡」。
# 先起它們，後面所有東西的 initContainer 才等得到。
# ═══════════════════════════════════════════════════════════
if _should 10; then
  _hdr "10-storage — MinIO / catalog-db"
  apply_layer 10-storage
  wait_ready statefulset minio      300 || exit 1
  wait_ready statefulset catalog-db 300 || exit 1
  # bucket 要先存在，Flink 的 checkpoint 才寫得進去
  run_job k8s/10-storage/minio-init-job.yaml 180 || exit 1
fi

# ═══════════════════════════════════════════════════════════
# 20-platform — Kafka / Schema Registry / iceberg-rest
#
# 資料平面的骨幹。三個 Job 的順序有依賴：
#   kafka-init      建 topic（Flink 的 source 要用）
#   schema-register 註冊 Avro schema（producer 要用）
# ═══════════════════════════════════════════════════════════
if _should 20; then
  _hdr "20-platform — Kafka / Schema Registry / iceberg-rest"
  apply_layer 20-platform
  # Kafka 是 podManagementPolicy: Parallel，三個 broker 同時起，
  # 但 quorum 要三個都在才算就緒——所以這裡的等待比單一 pod 久。
  wait_ready statefulset broker          420 || exit 1
  wait_ready deployment  schema-registry 300 || exit 1
  wait_ready deployment  iceberg-rest    300 || exit 1
  run_job k8s/20-platform/kafka-init-job.yaml      240 || exit 1
  run_job k8s/20-platform/schema-register-job.yaml 180 || exit 1
fi

# ═══════════════════════════════════════════════════════════
# 30-compute — Flink（+ HA 的 RBAC）/ Trino
#
# ⚠️ flink-rbac 必須先於 JobManager：pod 起來時 ServiceAccount 要已存在，
#    否則 pod 會建立失敗（不是稍後重試，是直接拒絕）。
#    apply_layer 依檔名字母序，flink-rbac 剛好排在 flink-jobmanager 之後，
#    所以這裡明確先套用它，不依賴排序這種脆弱的東西。
#
# flink-submit Job 放在最後（40-app 之後），因為它要等 TaskManager 的
# slot 都註冊上來。見下方。
# ═══════════════════════════════════════════════════════════
if _should 30; then
  _hdr "30-compute — Flink / Trino"
  _info "先套用 RBAC（ServiceAccount 必須早於使用它的 pod）"
  kubectl apply -f k8s/30-compute/flink-rbac.yaml >/dev/null || _die "flink-rbac 失敗"

  # 部署前先問，比部署後翻日誌找 Forbidden 快得多。
  # 權限不足的症狀是 JM 起得來但選不出 leader，REST 一直 503——
  # 完全不像權限問題。
  for chk in "create:configmaps" "create:leases"; do
    v="${chk%%:*}"; r="${chk##*:}"
    a=$(kubectl auth can-i "$v" "$r" --as="system:serviceaccount:${NS}:flink" -n "$NS" 2>/dev/null)
    [ "$a" = "yes" ] && _ok "flink 可以 ${v} ${r}" || _die "flink 不能 ${v} ${r} —— HA 會失敗"
  done

  apply_layer 30-compute
  wait_ready deployment flink-jobmanager  420 || exit 1
  wait_ready deployment flink-taskmanager 420 || exit 1
  wait_ready deployment trino             420 || exit 1
fi

# ═══════════════════════════════════════════════════════════
# 40-app — Producer / Gateway / Web / Bot
# ═══════════════════════════════════════════════════════════
if _should 40; then
  _hdr "40-app — Producer / Gateway / Web / Bot"
  apply_layer 40-app
  wait_ready deployment producer 300 || exit 1
  wait_ready deployment gateway  300 || exit 1
  wait_ready deployment web      300 || exit 1
  wait_ready deployment bot      300 || exit 1
fi

# ═══════════════════════════════════════════════════════════
# Flink job 提交 —— 刻意排在這裡
#
# 它需要兩個前提，分屬不同層：
#   1. TaskManager 的 slot 已註冊（30-compute）
#   2. iceberg-rest 與 catalog 可用（10/20）
# 而它建立的 Iceberg 表是 Trino 與 gateway 查詢的對象，
# 所以放在 app 起來之後、監控之前。
#
# ⚠️ 這支腳本會用 docker/flink/ 底下的檔案重新產生 ConfigMap
#    （過程中把 compose 的 broker 位址換成 K8s 的 Service 名），
#    所以它不是單純的 kubectl apply。
# ═══════════════════════════════════════════════════════════
if _should 30 || _should 40; then
  _hdr "提交 Flink job"
  if [ -f k8s/deploy/gcp/14-submit-flink.sh ]; then
    bash k8s/deploy/gcp/14-submit-flink.sh || {
      _err "job 提交失敗"
      echo "     部分 job 已存在時腳本會刻意停下來要你決定——那不是 bug。"
      echo "     見 k8s/30-compute/flink-submit-job.yaml 的說明。"
      exit 1
    }
  else
    _warn "找不到 k8s/deploy/gcp/14-submit-flink.sh，跳過"
  fi
fi

# ═══════════════════════════════════════════════════════════
# 50-monitoring — kafka-exporter / Prometheus / Alertmanager / Grafana
#
# 最後才部署，因為 Prometheus 靠 pod annotation 自動發現目標——
# 被監控的東西要先存在。（其實晚點也會發現，watch 是持續的，
# 但放最後可以讓「8 個 target 全 UP」直接當成部署成功的判準。）
# ═══════════════════════════════════════════════════════════
if _should 50; then
  _hdr "50-monitoring — Prometheus / Alertmanager / Grafana"
  _info "先套用 RBAC"
  kubectl apply -f k8s/50-monitoring/prometheus-rbac.yaml >/dev/null || _die "prometheus-rbac 失敗"
  a=$(kubectl auth can-i list pods --as="system:serviceaccount:${NS}:prometheus" -n "$NS" 2>/dev/null)
  [ "$a" = "yes" ] && _ok "prometheus 可以 list pods" \
                   || _die "prometheus 不能 list pods —— /targets 會是空的"

  apply_layer 50-monitoring
  wait_ready deployment kafka-exporter 300 || exit 1
  wait_ready deployment prometheus     300 || exit 1
  wait_ready deployment alertmanager   300 || exit 1
  wait_ready deployment grafana        300 || exit 1
fi

# ═══════════════════════════════════════════════════════════
_hdr "完成"
kubectl get pods -n "$NS" -o wide
cat <<EOT

────────────────────────────────────────────────────────────
 驗收（等 2-3 分鐘讓 checkpoint 跑一輪再跑）：
   bash k8s/deploy/gcp/16-verify-week8.sh     全鏈路 27 項
   bash k8s/deploy/gcp/18-security-check.sh   安全檢查
   bash k8s/deploy/gcp/19-alert-drill.sh      告警演練（約 10 分鐘）

 看畫面：
   kubectl port-forward -n ${NS} svc/web        8080:80
   kubectl port-forward -n ${NS} svc/grafana    3000:3000
   kubectl port-forward -n ${NS} svc/prometheus 9090:9090

 ⚠️ 部署成功只證明「現在能動」。真正的驗收是隔天——
    06-down.sh 收工，隔天 05-up.sh 之後**不重送 job** 而能 27/27。
    見 doc/updated_plan/week9-retrospective.md。

 收工：bash k8s/deploy/gcp/06-down.sh
────────────────────────────────────────────────────────────
EOT
