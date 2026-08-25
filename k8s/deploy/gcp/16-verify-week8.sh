#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Week 8 驗收 —— 全鏈路。
#
# 逐段驗證，順序就是資料流的方向：
#   Producer → Kafka → Flink → Iceberg → Trino → Gateway → Web / Bot
#
# 純讀取，可以反覆跑。
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

NS="$K8S_NAMESPACE"
PASS=0; FAIL=0
_p() { echo "  ✅ $*"; PASS=$((PASS+1)); }
_f() { echo "  ❌ $*"; FAIL=$((FAIL+1)); }

# grep -c 找不到時會印 0 並回 exit 1，配 set -e 會靜默中止——
# 這個 helper 是 Week 7/8 踩過三次的坑的統一解法。
_cnt() { grep -c . 2>/dev/null || true; }

K="kubectl exec -n $NS broker-0 --"
BS="--bootstrap-server localhost:9092"

echo "════════════════════════════════════════════════════════"
echo " Week 8 驗收 — 全鏈路"
echo "════════════════════════════════════════════════════════"

# --- 1. 所有服務就緒 ------------------------------------------------------
echo
echo "── 1. 服務狀態 ──────────────────────────────────────"
kubectl get pods -n "$NS" -o wide
echo
for w in "statefulset/broker:3" "statefulset/minio:1" \
         "deployment/schema-registry:1" "deployment/iceberg-rest:1" \
         "deployment/producer:1" \
         "deployment/flink-jobmanager:1" "deployment/flink-taskmanager:2" \
         "deployment/trino:1" \
         "deployment/gateway:1" "deployment/web:2" "deployment/bot:1"; do
  obj="${w%%:*}"; want="${w##*:}"
  ready=$(kubectl get "$obj" -n "$NS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
  ready="${ready:-0}"
  [ "$ready" = "$want" ] && _p "$obj  $ready/$want" || _f "$obj  $ready/$want"
done

# --- 2. Producer → Kafka --------------------------------------------------
echo
echo "── 2. Producer → Kafka ─────────────────────────────"
# ⚠️ 不要用 kafka-run-class kafka.tools.GetOffsetShell ——
#    那個類別路徑在 Kafka 3.x 之後搬到 org.apache.kafka.tools，
#    舊路徑會 ClassNotFound。配上 2>/dev/null 就變成靜默回 0，
#    把「有資料」誤報成「是空的」。
#    cp-kafka 7.9 有 kafka-get-offsets 包裝腳本，用它最穩。
OFFSETS=$($K kafka-get-offsets --bootstrap-server localhost:9092 \
            --topic market-trades 2>/dev/null \
          | awk -F: '{s+=$3} END {print s+0}')

# 包裝腳本不存在時的後備：用新的類別路徑
if [ "${OFFSETS:-0}" -eq 0 ]; then
  OFFSETS=$($K kafka-run-class org.apache.kafka.tools.GetOffsetShell \
              --bootstrap-server localhost:9092 --topic market-trades 2>/dev/null \
            | awk -F: '{s+=$3} END {print s+0}')
fi

echo "     market-trades 總訊息數：${OFFSETS:-0}"
if [ "${OFFSETS:-0}" -gt 0 ]; then
  _p "Kafka 有資料流入"
else
  _f "market-trades 是空的（或 offset 查詢失敗）"
  echo "     手動確認："
  echo "       kubectl exec -n ${NS} broker-0 -- kafka-get-offsets \\"
  echo "         --bootstrap-server localhost:9092 --topic market-trades"
fi

# --- 3. Flink job ---------------------------------------------------------
echo
echo "── 3. Flink job ────────────────────────────────────"
# ⚠️ 2026-08-25 修正。原本是從外面另開一個 curl pod 打
#    flink-jobmanager:8081。那樣「JM 掛了」和「JM 活著但 0 個 job」
#    會得到完全一樣的答案（空字串 → grep -o → wc -l → 0），
#    害我第一輪把根因猜成 Spot 回收，白花一輪。
#    改成從 JM 自己的 pod 裡打，並且**先印出原始回應**。
JOBS=$(kubectl exec -n "$NS" deploy/flink-jobmanager -c jobmanager -- \
         curl -s --max-time 10 localhost:8081/jobs 2>/dev/null)

if [ -z "$JOBS" ]; then
  _f "JobManager 的 REST 沒有回應——這不是「0 個 job」，是 JM 本身有問題"
  echo "     kubectl get pods -n ${NS} -l component=jobmanager"
  echo "     kubectl logs -n ${NS} deploy/flink-jobmanager --tail=50"
else
  echo "     /jobs 原始回應：${JOBS}"
  RUNNING=$(echo "$JOBS" | grep -o '"status":"RUNNING"' | wc -l | tr -d ' ')
  echo "     RUNNING job 數：${RUNNING}"
  if [ "${RUNNING:-0}" -ge 4 ]; then
    _p "4 個 job 都在跑"
  else
    _f "只有 ${RUNNING} 個 job RUNNING（應為 4）"
    echo "     JM 活著但 job 不夠。開了 Kubernetes HA 之後這應該會自動復原，"
    echo "     若沒有，先看 HA 的 ConfigMap 還在不在："
    echo "       kubectl get cm -n ${NS} | grep streaming-poc-flink"
    echo "     真的沒了就重送：bash k8s/deploy/gcp/14-submit-flink.sh"
  fi
fi

# --- 4. Checkpoint --------------------------------------------------------
echo
echo "── 4. Checkpoint 寫入 MinIO ────────────────────────"
CHK=$(kubectl exec -n "$NS" minio-0 -- ls /data/warehouse/checkpoints 2>/dev/null | _cnt)
echo "     checkpoint 目錄數：${CHK}"
[ "${CHK:-0}" -ge 4 ] && _p "每個 job 都有 checkpoint" || _f "checkpoint 目錄只有 ${CHK} 個"

# --- 5. Iceberg 資料 ------------------------------------------------------
echo
echo "── 5. Iceberg 表（經 Trino 查詢）───────────────────"
TRINO="kubectl exec -n $NS deploy/trino -- trino --server localhost:8080 --catalog iceberg --user verify --output-format=CSV_UNQUOTED --execute"

# ⚠️ 2026-08-25 修正。原本是 `2>/dev/null | tail -1 | tr -dc '0-9'`——
#    查詢失敗時 stderr 被吞、stdout 是空的、tr 給不出數字，
#    於是「查詢炸了」和「表真的是 0 筆」印出一模一樣的 0。
#    8/25 那次真正的錯誤訊息是
#        Schema 'bronze' does not exist
#    （iceberg-rest 的 catalog 沒持久化，重啟後表全消失），
#    但驗收畫面上只看得到「bronze.trades 是空的」。
#    現在先確認 schema 存在，再查筆數，而且錯誤照印。
SCHEMAS=$($TRINO "SHOW SCHEMAS FROM iceberg" 2>&1)
if ! echo "$SCHEMAS" | grep -q '^bronze$'; then
  _f "iceberg catalog 裡沒有 bronze schema——這不是「沒資料」，是 catalog 不見了"
  echo "     Trino 回應："
  echo "$SCHEMAS" | sed 's/^/       /' | head -12
  echo "     檢查 catalog 有沒有持久化："
  echo "       kubectl exec -n ${NS} deploy/iceberg-rest -- printenv | grep CATALOG_URI"
  echo "       （應為 jdbc:sqlite:file:/data/catalog.db，不是 /tmp 或 memory）"
  echo "     重建表：bash k8s/deploy/gcp/14-submit-flink.sh"
else
  BRONZE_RAW=$($TRINO "SELECT COUNT(*) FROM iceberg.bronze.trades" 2>&1)
  BRONZE=$(echo "$BRONZE_RAW" | tail -1 | tr -dc '0-9')
  echo "     bronze.trades 筆數：${BRONZE:-0}"
  if [ "${BRONZE:-0}" -gt 0 ]; then
    _p "bronze 層有資料"
  else
    _f "bronze.trades 是空的"
    echo "$BRONZE_RAW" | sed 's/^/       /' | head -6
  fi
fi

SILVER=$($TRINO "SELECT COUNT(*) FROM iceberg.silver.vwap_1min" 2>/dev/null | tail -1 | tr -dc '0-9')
echo "     silver.vwap_1min 筆數：${SILVER:-0}"
if [ "${SILVER:-0}" -gt 0 ]; then
  _p "silver 層有 VWAP 聚合"
else
  echo "     ⚠️ silver 是空的——1 分鐘視窗需要 watermark 前進才輸出，"
  echo "        剛部署完可能要等幾分鐘。不計入失敗。"
fi

# --- 6. Gateway API -------------------------------------------------------
echo
echo "── 6. Gateway ──────────────────────────────────────"
# ⚠️ 2026-08-25：加了重試。
#    `kubectl run --rm -i` 有競態——pod 啟動比 kubectl attach 慢時，
#    容器已經跑完退出，輸出就接不到，回傳空字串。
#    症狀是「GET /api/health → 無回應」，但直接 exec 進 pod 打是 200。
#    node 正在拉 image 或排程繁忙時特別容易發生。
#    這是驗收腳本第 7 次騙人，這次是誤報失敗（前幾次都是誤報成功）。
_http() {
  local url="$1" code=""
  for _ in 1 2; do
    code=$(kubectl run gchk-$RANDOM -n "$NS" --rm -i --restart=Never --quiet \
             --image=curlimages/curl:8.11.1 -- \
             curl -s -o /dev/null -w '%{http_code}' --max-time 15 "$url" 2>/dev/null \
           | tr -dc '0-9')
    [ -n "$code" ] && break
    sleep 3
  done
  echo "$code"
}
for ep in "/api/health" "/api/symbols" "/api/klines"; do
  CODE=$(_http "http://gateway:8000${ep}")
  [ "$CODE" = "200" ] && _p "GET ${ep} → 200" || _f "GET ${ep} → ${CODE:-無回應}"
done

# --- 7. Web ---------------------------------------------------------------
echo
echo "── 7. Web（含反向代理）─────────────────────────────"
CODE=$(_http "http://web/healthz")
[ "$CODE" = "200" ] && _p "web /healthz → 200" || _f "web /healthz → ${CODE:-無回應}"
# 這一項驗證的是 nginx 的 proxy_pass 設定——
# 前端打的是同源的 /api/*，經 nginx 轉到 gateway。
CODE=$(_http "http://web/api/health")
[ "$CODE" = "200" ] && _p "web → gateway 反向代理正常" || _f "反向代理 → ${CODE:-無回應}"

# --- 8. Bot ---------------------------------------------------------------
echo
echo "── 8. Discord Bot ──────────────────────────────────"
# ⚠️ 2026-08-25 修正。原本只 grep 日誌找 "ready|logged in|connected"，
#    結果 bot 明明是 CrashLoopBackOff（重啟 24 次），這一項照樣印 ✅——
#    因為它**曾經**連上過，那行日誌還留在 --tail=50 的範圍內。
#    「曾經成功」被當成「現在健康」。
#
#    這是驗收腳本第 6 次騙人，而且是新的一種模式：
#      第 1-5 次 → 把失敗印成 0（吞掉 stderr、grep exit 1）
#      第 6 次   → 拿歷史當現況
#
#    通則：**先看 K8s 眼中的狀態，再看應用程式日誌。**
#    pod 沒 Ready，日誌講什麼都不算數。
BOT_READY=$(kubectl get deploy bot -n "$NS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
BOT_RESTARTS=$(kubectl get pods -n "$NS" -l app=bot \
                -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null)

if [ "${BOT_READY:-0}" != "1" ]; then
  _f "bot 沒有 Ready（重啟 ${BOT_RESTARTS:-?} 次）——日誌裡的連線成功訊息是歷史，不是現況"
  echo "     看死因（--previous 才是死掉那次的日誌）："
  echo "       kubectl logs -n ${NS} deploy/bot --previous --tail=60"
  echo "       kubectl describe pod -n ${NS} -l app=bot | grep -A12 'Last State'"
  echo "     退出碼 137 = OOMKilled，1 = 程式拋例外"
elif kubectl logs -n "$NS" deploy/bot --tail=50 2>/dev/null | grep -qiE "ready|logged in|connected"; then
  _p "bot Ready 且日誌顯示已連上 Discord（重啟 ${BOT_RESTARTS:-0} 次）"
else
  _f "bot Ready 但日誌沒有連線成功的跡象"
  echo "     kubectl logs -n $NS deploy/bot --tail=30"
fi

# --- 9. 全域要求 ----------------------------------------------------------
echo
echo "── 9. 全域要求 ─────────────────────────────────────"
# enableServiceLinks: false（Week 7 坑 4）
MISSING=$(kubectl get deploy -n "$NS" \
  -o jsonpath='{range .items[?(@.spec.template.spec.enableServiceLinks!=false)]}{.metadata.name}{" "}{end}' 2>/dev/null)
[ -z "$MISSING" ] && _p "全部 Deployment 都設了 enableServiceLinks: false" \
                  || _f "未設定：$MISSING"

# 無 LoadBalancer
LB=$(kubectl get svc -n "$NS" --field-selector spec.type=LoadBalancer --no-headers 2>/dev/null | _cnt)
[ "${LB:-0}" -eq 0 ] && _p "無 LoadBalancer（省每月約 \$18）" || _f "有 ${LB} 個 LoadBalancer"

# --- 持久化回歸檢查（2026-08-25 新增）------------------------------------
# 這兩項不是「功能」，是「隔夜之後還在不在」。8/25 就是這兩處同時失守，
# 而原本的驗收 25 項全過卻完全沒看出來——因為它們只驗當下的狀態。
# ⚠️ 2026-08-26 修正。這條檢查昨天寫死成
#      jdbc:sqlite:file:/data/*) → 通過
#    今天把 catalog 後端換成 Postgres 之後，它自己變成誤報失敗。
#
#    問題在於**判準檢查的是特定實作，而不是要保證的性質**。
#    要保證的是「catalog 重啟後不會失憶」，
#    而不是「它必須是放在 /data 的 SQLite 檔」。
#
#    這是驗證腳本第 8 次騙人——而且是新的一種：
#    「為了防止回歸而寫的守衛，自己過期了」。
#    寫得越具體的斷言，越容易在正常演進時變成雜訊，
#    然後被當成「已知可忽略」——那是告警疲勞的另一個入口。
#
#    現在改成用「排除法」：只要不是記憶體或容器暫存層就算通過。
CATURI=$(kubectl exec -n "$NS" deploy/iceberg-rest -- printenv CATALOG_URI 2>/dev/null)
case "$CATURI" in
  "")
    _f "iceberg-rest 沒有 CATALOG_URI → 退回 image 預設的 in-memory SQLite，重啟就失憶"
    ;;
  *memory*|jdbc:sqlite:*/tmp/*)
    _f "catalog 不會存活過重啟：${CATURI}"
    ;;
  jdbc:postgresql://*|jdbc:mysql://*)
    _p "catalog 存在外部資料庫（${CATURI}）"
    ;;
  jdbc:sqlite:*)
    # SQLite 只要不在 /tmp 就算持久化，但它撐不住並行寫入——
    # 8/26 就是這樣炸的（SQLITE_BUSY → 500 → CommitStateUnknownException）。
    # 通過但要提醒，因為「能存活重啟」和「能承受兩個 job 同時 commit」
    # 是兩件不同的事，而前者過關很容易讓人以為後者也沒問題。
    _p "catalog 有持久化（${CATURI}）"
    echo "     ⚠️ SQLite 是單寫者模型。多個 Flink job 同時 commit 會 SQLITE_BUSY，"
    echo "        見 doc/updated_plan/week9-decisions.md 坑 1。"
    ;;
  *)
    _f "無法判斷 catalog 是否持久化：${CATURI}"
    ;;
esac

HA=$(kubectl get deploy flink-jobmanager -n "$NS" \
      -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="FLINK_PROPERTIES")].value}' 2>/dev/null \
     | grep -c "high-availability.type: kubernetes" || true)
[ "${HA:-0}" -ge 1 ] && _p "Flink 已開啟 Kubernetes HA（JM 重啟後 job 會復原）" \
                     || _f "Flink 沒開 HA——JM 一重啟，4 個 job 會永久消失"

# 憑證不落地
if grep -rqE "ALPACA_API_KEY=|DISCORD_TOKEN=|MINIO_ROOT_PASSWORD=" k8s/ 2>/dev/null; then
  _f "k8s/ 底下有憑證明文"
else
  _p "k8s/ 底下無憑證明文"
fi

# --- 總結 -----------------------------------------------------------------
echo
echo "════════════════════════════════════════════════════════"
echo " 結果：通過 $PASS 項，失敗 $FAIL 項"
[ "$FAIL" -eq 0 ] && echo " ✅ Week 8 驗收通過 —— 全鏈路在 GKE 上跑通" \
                  || echo " ❌ 有項目未通過"
echo "════════════════════════════════════════════════════════"
cat <<EOT

 看畫面：
   kubectl port-forward -n ${NS} svc/web 8080:80
   → http://localhost:8080

 Flink Dashboard：
   kubectl port-forward -n ${NS} svc/flink-jobmanager 8081:8081

 收工：bash k8s/deploy/gcp/06-down.sh
EOT
exit "$FAIL"
