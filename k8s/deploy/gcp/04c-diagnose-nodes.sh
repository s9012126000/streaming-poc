#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 「叢集建好了但 kubectl get nodes 是空的」的診斷。
#
# 最可能的原因：Spot VM 被回收（preemption）。
# Spot 沒有任何保留容量保證，us-central1-a 的 e2-standard-4 一旦供不應求，
# 現有的 Spot node 會被收走，而且**不保證補得回來**——
# MIG 會一直重試，但沒容量就是開不出來。
#
# 這正是 Spot 的真實代價，不是設定錯誤。
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

echo "═══════════════════════════════════════════════════"
echo " node 消失診斷"
echo "═══════════════════════════════════════════════════"

echo
echo "── 1. 叢集認為自己有幾個 node ──────────────────"
gcloud container clusters describe "$CLUSTER_NAME" \
  --zone="$ZONE" --project="$PROJECT_ID" \
  --format="value[separator='  |  '](name,status,currentNodeCount)"

echo
echo "── 2. node pool 設定與狀態 ─────────────────────"
gcloud container node-pools describe "$NODE_POOL" \
  --cluster="$CLUSTER_NAME" --zone="$ZONE" --project="$PROJECT_ID" \
  --format="yaml(name,status,initialNodeCount,config.machineType,config.spot,config.preemptible,autoscaling)"

echo
echo "── 3. 實際存在的 VM ────────────────────────────"
INST=$(gcloud compute instances list --project="$PROJECT_ID" \
  --filter="name~gke-${CLUSTER_NAME}" \
  --format="table(name,status,scheduling.provisioningModel,zone.basename())" 2>/dev/null)
if [ -z "$INST" ]; then
  echo "   ❌ 一台都沒有"
else
  echo "$INST"
fi

echo
echo "── 4. 受管執行個體群組（MIG）的目標 vs 實際 ─────"
gcloud compute instance-groups managed list --project="$PROJECT_ID" \
  --filter="name~gke-${CLUSTER_NAME}" \
  --format="table(name,zone.basename(),targetSize,status.isStable)" 2>/dev/null \
  || echo "   （查不到）"

echo
echo "── 5. 最近的操作紀錄（找 preemption / repair）───"
gcloud compute operations list --project="$PROJECT_ID" \
  --filter="targetLink~gke-${CLUSTER_NAME} AND creationTimestamp>-PT3H" \
  --format="table(operationType,targetLink.basename(),status,creationTimestamp)" \
  --sort-by=~creationTimestamp --limit=15 2>/dev/null \
  || echo "   （查不到）"

echo
echo "── 6. MIG 建立失敗的真正原因 ⭐ 最關鍵 ──────────"
# list-errors 才會顯示 ZONE_RESOURCE_POOL_EXHAUSTED / QUOTA_EXCEEDED 這類實際錯誤，
# describe 只看得到 currentActions 的計數，看不出為什麼失敗。
for mig in $(gcloud compute instance-groups managed list --project="$PROJECT_ID" \
             --filter="name~gke-${CLUSTER_NAME}" --format="value(name)" 2>/dev/null); do
  echo "   [$mig]"
  # 用 value() 而不是 table(...slice())——slice 會把字串當成 list 切，
  # 印出一個一個字元的陣列，完全不能讀。
  gcloud compute instance-groups managed list-errors "$mig" \
    --zone="$ZONE" --project="$PROJECT_ID" \
    --format="value[separator='  '](error.code,timestamp)" 2>/dev/null \
    | sort | uniq -c | sort -rn | head -5 \
    || echo "   （list-errors 查不到）"
  echo "   完整訊息："
  gcloud compute instance-groups managed list-errors "$mig" \
    --zone="$ZONE" --project="$PROJECT_ID" \
    --format="value(error.message)" 2>/dev/null | head -1 | cut -c1-200
  echo
  echo "   ⚠️ list-errors 顯示的是**歷史**錯誤，不代表現在還在失敗。"
  echo "      要看現況請對照上面第 3、4 項：有 VM 且 IS_STABLE=True 就是已經好了。"
  echo
  echo "   目前動作："
  gcloud compute instance-groups managed describe "$mig" \
    --zone="$ZONE" --project="$PROJECT_ID" \
    --format="value(currentActions)" 2>/dev/null
done

echo
echo "── 7. 全專案的 VM（確認沒有別的東西在吃配額）───"
gcloud compute instances list --project="$PROJECT_ID" \
  --format="table(name,machineType.basename(),status,zone.basename())" 2>/dev/null \
  || echo "   （無）"

echo
echo "── 8. 目前 E2_CPUS 用量 ────────────────────────"
gcloud compute regions describe "$REGION" --project="$PROJECT_ID" --format=json \
  | python3 -c '
import json,sys
q={x["metric"]:x for x in json.load(sys.stdin)["quotas"]}
for m in ("CPUS","E2_CPUS","PREEMPTIBLE_CPUS","INSTANCES","SSD_TOTAL_GB"):
    if m in q:
        used = q[m].get("usage", 0)
        lim  = q[m]["limit"]
        print("   %-20s used=%6s / limit=%s" % (m, used, lim))
'

cat <<'EOT'

═══════════════════════════════════════════════════
 判讀（先看第 3、4 項，那才是現況）
═══════════════════════════════════════════════════

 【有 VM + IS_STABLE=True + currentActions 全是 none】
   → 已經正常了。第 6 項的錯誤是歷史紀錄，不用理會。
     直接跑 k8s/deploy/gcp/04b-verify-cluster.sh

 【沒有 VM + currentActions 有 creating=N】
   → 正在重試中，還沒成功。
     ZONE_RESOURCE_POOL_EXHAUSTED = 該 zone 目前沒有這個機型的 Spot 容量。
     Spot 沒有容量保證，MIG 會一直重試，通常幾分鐘到幾十分鐘會補上。

     ⏳ 先等 10-20 分鐘再看一次。真的補不回來才動下面的選項：
       a) 換 zone：us-central1-a → -b / -c / -f
          （改 env.sh 的 ZONE，重建叢集）
       b) 換機型：e2-standard-4 → n2-standard-4
          （N2 走獨立的 N2_CPUS 配額，且不同機型的 Spot 供給狀況不同）
       c) 放棄 Spot 改隨需（成本升 3-4 倍，最後手段）

 【有 VM 但 kubectl get nodes 是空的】
   → node 沒註冊進叢集，看 VM 的 serial console log：
     gcloud compute instances get-serial-port-output <VM名> --zone=us-central1-a

 【node pool 有 autoscaling 且 minNodeCount=0】
   → 被縮到 0 了，跑 k8s/deploy/gcp/05-up.sh 擴回來
EOT
