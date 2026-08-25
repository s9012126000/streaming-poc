#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Trino 的查詢工具與起停控制。
#
# ⚠️ Trino 現在是**常駐**（replicas: 1）。
#    Week 6 原本定案「按需啟動」以省 1 vCPU，後來推翻——
#    因為 Web 的 /api/klines 和 /api/symbols 都依賴它，
#    而且 node 數量是為 Kafka quorum 決定的，Trino 常駐的邊際成本是零。
#    完整理由見 doc/updated_plan/week8-decisions.md。
#
#    所以 down 的用途變成「臨時讓出資源」（例如壓力測試），
#    不是日常流程。用完記得 up 回來，否則圖表會沒有歷史資料。
#
# 用法：
#   bash k8s/deploy/gcp/15-trino.sh q "SELECT 1"          # 執行 SQL
#   bash k8s/deploy/gcp/15-trino.sh demo                  # 跑 Week 3 的驗證查詢
#   bash k8s/deploy/gcp/15-trino.sh shell                 # 進互動式 CLI
#   bash k8s/deploy/gcp/15-trino.sh up / down             # 起停（臨時用）
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

NS="$K8S_NAMESPACE"
CMD="${1:-}"

_up() {
  local ready
  ready=$(kubectl get deploy trino -n "$NS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "")
  if [ "${ready:-0}" = "1" ]; then
    _ok "Trino 已在執行"
    return 0
  fi
  _info "啟動 Trino（JVM 啟動約 60-90 秒）..."
  kubectl scale deploy/trino -n "$NS" --replicas=1
  if ! kubectl wait --for=condition=available deploy/trino -n "$NS" --timeout=300s; then
    echo "❌ 啟動逾時"
    kubectl get pods -n "$NS" -l app=trino
    kubectl describe pod -n "$NS" -l app=trino | grep -A8 "Last State" || true
    kubectl logs -n "$NS" deploy/trino --tail=40 2>/dev/null || true
    return 1
  fi

  # ⚠️ deployment available 還不夠。
  #    實測踩到：probe 過了、kubectl wait 通過，但查詢仍收到
  #    "Trino server is still initializing"。
  #    現在 probe 已改成檢查 "starting":false，這裡再確認一次——
  #    多一層保險，而且失敗時訊息比 SQL 錯誤清楚。
  _info "確認 Trino 完成初始化..."
  for i in $(seq 1 40); do
    if kubectl exec -n "$NS" deploy/trino -- \
         sh -c 'curl -sf localhost:8080/v1/info | grep -q "\"starting\":false"' 2>/dev/null; then
      _ok "Trino 就緒（第 ${i} 次確認）"
      return 0
    fi
    [ $((i % 5)) -eq 0 ] && echo "   仍在初始化（第 ${i} 次）..."
    sleep 5
  done

  echo "❌ 初始化逾時"
  kubectl logs -n "$NS" deploy/trino --tail=40 2>/dev/null || true
  return 1
}

_down() {
  _info "縮回 0"
  kubectl scale deploy/trino -n "$NS" --replicas=0
  _ok "已停止（Service 仍在，Gateway 會得到連線被拒而不是 DNS 失敗）"
}

# 在 Trino 的 pod 裡用內建 CLI 執行 SQL。
# 不另外開 client pod——trino CLI 就在 image 裡，直接 exec 最省事。
#
# ⚠️ 這裡**不**呼叫 _up。
#    原本每個查詢都先 _up，結果 Trino 中途 OOM 重啟時，
#    後續查詢會各自印一次「已在執行」然後失敗，
#    畫面上看起來像六個獨立的錯誤，其實是同一次崩潰。
#    改成由呼叫端負責啟動，失敗就整批停下。
_query() {
  local sql="$1"
  echo
  echo "── SQL ─────────────────────────────────────────────"
  echo "$sql"
  echo "────────────────────────────────────────────────────"
  if ! kubectl exec -n "$NS" deploy/trino -- \
         trino --server localhost:8080 --catalog iceberg --user poc \
               --output-format=ALIGNED --execute "$sql"; then
    echo
    echo "❌ 查詢失敗。常見原因："
    echo "   exit 137        → OOMKilled，提高 limits.memory"
    echo "   still initializing → 啟動未完成（probe 應該要擋掉，若出現代表 probe 有問題）"
    echo "   Schema not found → 表還沒建立，Flink 的 job 可能還沒 commit"
    echo
    echo "   kubectl describe pod -n ${NS} -l app=trino | grep -A8 'Last State'"
    return 1
  fi
}

case "$CMD" in
  up)   _up ;;
  down) _down ;;

  q)
    shift
    [ $# -gt 0 ] || _die '用法：bash k8s/deploy/gcp/15-trino.sh q "SELECT ..."'
    _up || exit 1
    _query "$*"
    ;;

  shell)
    _up || exit 1
    echo "（輸入 quit 離開；離開後記得跑 down）"
    kubectl exec -it -n "$NS" deploy/trino -- \
      trino --server localhost:8080 --catalog iceberg --user poc
    ;;

  demo)
    _up || exit 1
    echo
    echo "════════════════════════════════════════════════════"
    echo " Week 3 查詢驗證"
    echo "════════════════════════════════════════════════════"

    # 失敗就停——繼續跑下去只會產生一連串誤導的錯誤
    _query "SHOW SCHEMAS FROM iceberg" || exit 1
    _query "SHOW TABLES FROM iceberg.bronze" || exit 1
    _query "SHOW TABLES FROM iceberg.silver" || exit 1

    echo
    echo "── 資料量 ──────────────────────────────────────────"
    _query "SELECT COUNT(*) AS rows FROM iceberg.bronze.trades" || exit 1

    echo
    echo "── 最新 5 筆成交 ───────────────────────────────────"
    _query "SELECT symbol, price, size, trade_time
            FROM iceberg.bronze.trades
            ORDER BY trade_time DESC
            LIMIT 5"

    echo
    echo "── VWAP（silver 層，需要視窗關閉才有資料）──────────"
    _query "SELECT symbol, window_start, vwap, volume, trade_count
            FROM iceberg.silver.vwap_1min
            ORDER BY window_start DESC
            LIMIT 5"

    cat <<'EOT'

────────────────────────────────────────────────────────────
 silver.vwap_1min 若是空的不代表故障——它是 1 分鐘視窗聚合，
 要等視窗關閉才輸出，而視窗關閉依賴 watermark 前進。
 剛部署完或資料稀疏的幣種可能要好幾分鐘才出現。

 Trino 是常駐的，查完不用停——Web 的 /api/klines 需要它。
────────────────────────────────────────────────────────────
EOT
    ;;

  *)
    cat <<'EOT'
用法：
  bash k8s/deploy/gcp/15-trino.sh up                 啟動
  bash k8s/deploy/gcp/15-trino.sh q "SELECT 1"       執行 SQL
  bash k8s/deploy/gcp/15-trino.sh demo               跑 Week 3 驗證查詢
  bash k8s/deploy/gcp/15-trino.sh shell              互動式 CLI
  bash k8s/deploy/gcp/15-trino.sh down               停止（省資源）
EOT
    exit 1
    ;;
esac
