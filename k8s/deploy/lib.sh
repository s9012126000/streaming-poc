#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 部署腳本的共用函式。由 deploy.sh source，不單獨執行。
# ---------------------------------------------------------------------------

NS="${K8S_NAMESPACE:-streaming}"
K8S_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

_hdr()  { echo; echo "════ $* ════════════════════════════════"; }
_info() { echo "  → $*"; }
_ok()   { echo "  ✅ $*"; }
_warn() { echo "  ⚠️  $*"; }
_err()  { echo "  ❌ $*" >&2; }
_die()  { _err "$*"; exit 1; }

# ---------------------------------------------------------------------------
# 套用一層的普通資源（Deployment / StatefulSet / Service / ConfigMap / RBAC）
#
# 刻意排除 *-job.yaml——Job 不能用 kubectl apply 更新，見 _run_job。
# ---------------------------------------------------------------------------
apply_layer() {
  local dir="$1" f found=0
  for f in "${K8S_DIR}/${dir}"/*.yaml; do
    [ -e "$f" ] || continue
    case "$(basename "$f")" in
      *-job.yaml) continue ;;    # 交給 _run_job
    esac
    _info "apply $(basename "$f")"
    kubectl apply -f "$f" >/dev/null || _die "apply 失敗：$f"
    found=1
  done
  [ "$found" = "1" ] || _warn "${dir}/ 沒有非 Job 的 manifest"
}

# ---------------------------------------------------------------------------
# 執行一次性 Job
#
# ⚠️ Job 的 spec.template 是不可變的（immutable）。
#    對一個已經存在的 Job 跑 kubectl apply，只要 template 有任何差異就會回
#      field is immutable
#    而且訊息不會說「你應該先刪掉它」。這是重跑部署腳本時最常見的失敗。
#
#    所以固定「先刪再建」。--ignore-not-found 讓第一次執行也能通過。
#    這也順便解決另一個問題：Job 完成後不會自己消失
#    （除非設了 ttlSecondsAfterFinished），重跑時會被誤認為「已經在跑」。
# ---------------------------------------------------------------------------
run_job() {
  local file="$1" timeout="${2:-300}"
  local name
  name=$(awk '/^kind: Job/{f=1} f && /^  name:/{print $2; exit}' "$file")
  [ -n "$name" ] || _die "在 $file 裡找不到 Job 名稱"

  _info "Job ${name}（先刪除舊的）"
  kubectl delete job "$name" -n "$NS" --ignore-not-found --wait=true >/dev/null 2>&1
  kubectl apply -f "$file" >/dev/null || _die "apply 失敗：$file"

  _info "等待 ${name} 完成（最多 ${timeout} 秒）..."
  if kubectl wait --for=condition=complete "job/${name}" -n "$NS" \
       --timeout="${timeout}s" >/dev/null 2>&1; then
    _ok "${name} 完成"
    return 0
  fi

  # 逾時或失敗——把日誌印出來，不要只說「失敗」。
  # 這是 Week 8 學到的：Job 失敗時最需要的就是它的日誌，
  # 而 restartPolicy: Never 讓失敗的 pod 保留下來正是為了這一刻。
  _err "${name} 未在時限內完成"
  echo
  echo "  ── Job 狀態 ──"
  kubectl get job "$name" -n "$NS" -o wide 2>/dev/null | sed 's/^/     /'
  echo "  ── Pod ──"
  kubectl get pods -n "$NS" -l "job-name=${name}" 2>/dev/null | sed 's/^/     /'
  echo "  ── 日誌（最後 40 行）──"
  kubectl logs -n "$NS" -l "job-name=${name}" --tail=40 --prefix 2>/dev/null | sed 's/^/     /'
  return 1
}

# ---------------------------------------------------------------------------
# 等待工作負載就緒
#
# 用 rollout status 而不是 kubectl wait --for=condition=available：
# 前者對 StatefulSet 和 Deployment 都適用，而且會即時印進度。
# ---------------------------------------------------------------------------
wait_ready() {
  local kind="$1" name="$2" timeout="${3:-300}"
  _info "等待 ${kind}/${name} 就緒..."
  if kubectl rollout status "${kind}/${name}" -n "$NS" \
       --timeout="${timeout}s" >/dev/null 2>&1; then
    _ok "${kind}/${name} 就緒"
    return 0
  fi
  _err "${kind}/${name} 未就緒"
  kubectl get pods -n "$NS" -l "app=${name}" -o wide 2>/dev/null | sed 's/^/     /'
  echo "  ── Events ──"
  kubectl describe "${kind}/${name}" -n "$NS" 2>/dev/null \
    | sed -n '/Events:/,$p' | head -20 | sed 's/^/     /'
  return 1
}

# ---------------------------------------------------------------------------
# 前置檢查：Secret 必須存在且含指定的 key
#
# 為什麼要驗 key 而不只是驗 Secret 存在：
#   Secret 是從 .env 整份轉過來的，漏了一個 key 不會有任何錯誤，
#   直到某個 pod 因為 secretKeyRef 找不到而卡在 CreateContainerConfigError。
#   那個狀態不會出現在 rollout status 的輸出裡，只會一直等。
# ---------------------------------------------------------------------------
require_secret_keys() {
  local secret="$1"; shift
  kubectl get secret "$secret" -n "$NS" >/dev/null 2>&1 \
    || _die "找不到 Secret ${secret}——先跑 bash k8s/deploy/gcp/07-create-secrets.sh"

  local missing="" k
  for k in "$@"; do
    kubectl get secret "$secret" -n "$NS" -o jsonpath="{.data.${k}}" 2>/dev/null \
      | grep -q . || missing="${missing} ${k}"
  done
  if [ -n "$missing" ]; then
    _err "Secret ${secret} 缺少：${missing}"
    echo "     把它們加進 .env 之後重跑："
    echo "       bash k8s/deploy/gcp/07-create-secrets.sh"
    exit 1
  fi
  _ok "Secret ${secret} 含所有必要的 key"
}
