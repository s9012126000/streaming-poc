#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Week 6 / Step 2 — 配額檢查。
#
# 為什麼要在開叢集前做：試用帳號的配額明顯低於正式帳號，
# 而且 Free Trial 帳號**根本不能申請提高配額**（GCP 官方明文）。
# 也就是說配額不是「先試試看不夠再說」，而是硬牆——現在不查，Week 7 才撞。
#
# ⚠️ gcloud 把 quotas 回成巢狀陣列，直接用 --format=table(quotas.metric)
#    會印出三個對不起來的平行 list，完全不能讀。所以這裡取 JSON 自己解析。
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

echo "════════════════════════════════════════════════════════════"
echo " 配額檢查 — $PROJECT_ID @ $REGION"
echo "════════════════════════════════════════════════════════════"

REGION_JSON=$(gcloud compute regions describe "$REGION" --project="$PROJECT_ID" --format=json)
PROJECT_JSON=$(gcloud compute project-info describe --project="$PROJECT_ID" --format=json)

export REGION_JSON PROJECT_JSON MACHINE_TYPE NODE_COUNT DISK_SIZE DISK_TYPE REGION

python3 <<'PY'
import json, os

region  = json.loads(os.environ["REGION_JSON"])
project = json.loads(os.environ["PROJECT_JSON"])

def quotas(obj):
    return {q["metric"]: (q["limit"], q.get("usage", 0)) for q in obj.get("quotas", [])}

R, P = quotas(region), quotas(project)

def show(label, table, metric, need=None, note=""):
    if metric not in table:
        print(f"  {label:<28} {'(此專案無此項)':>12}")
        return None
    limit, usage = table[metric]
    limit_i = int(limit)
    line = f"  {label:<28} limit={limit_i:<8} used={int(usage):<5}"
    if need is not None:
        line += "  ✅ 夠" if limit_i >= need else f"  ❌ 需要 ≥{need}"
    if note:
        line += f"  {note}"
    print(line)
    return limit_i

# --- 這次規劃要用的量 -----------------------------------------------------
mt         = os.environ["MACHINE_TYPE"]           # e2-standard-4
nodes      = int(os.environ["NODE_COUNT"])
vcpu_each  = int(mt.rsplit("-", 1)[-1])           # 4
need_vcpu  = vcpu_each * nodes
node_disk  = int(os.environ["DISK_SIZE"]) * nodes

print(f"\n規劃：{mt} × {nodes} = {need_vcpu} vCPU，node 開機碟共 {node_disk} GB\n")

print("── 運算（region: %s）──────────────────────────" % os.environ["REGION"])
# ⚠️ E2 和 N1 機型共用 `CPUS` 這個配額池，**不是** `E2_CPUS`。
#    實測佐證：跑 2 台 e2-standard-4 時 CPUS used=8、E2_CPUS used=0。
#    E2_CPUS 這個 metric 存在但不被 E2 機型消耗，拿它當上限會誤判。
cpus    = show("CPUS",             R, "CPUS",             need_vcpu, "← E2 實際吃這條")
show("E2_CPUS",          R, "E2_CPUS",          None,      "← 參考用，E2 不吃這條")
preempt = show("PREEMPTIBLE_CPUS", R, "PREEMPTIBLE_CPUS")
show("INSTANCES",        R, "INSTANCES",        nodes)

print("\n── 運算（project 全域）────────────────────────")
allreg = show("CPUS_ALL_REGIONS",  P, "CPUS_ALL_REGIONS",  need_vcpu)

print("\n── 儲存（region）──────────────────────────────")
# pd-balanced / pd-ssd 都算 SSD_TOTAL_GB，不是 DISKS_TOTAL_GB——很常搞錯
ssd  = show("SSD_TOTAL_GB",   R, "SSD_TOTAL_GB",   node_disk, "← pd-balanced / pd-ssd 走這條")
show("DISKS_TOTAL_GB", R, "DISKS_TOTAL_GB", None,      "← pd-standard 走這條")

print("\n── 網路 ───────────────────────────────────────")
show("IN_USE_ADDRESSES (region)",  R, "IN_USE_ADDRESSES", 1)
show("IN_USE_ADDRESSES (global)",  P, "IN_USE_ADDRESSES", 1)
show("FORWARDING_RULES",           P, "FORWARDING_RULES", 1, "← Week 8 Ingress/LB")
show("NETWORKS",                   P, "NETWORKS")
show("FIREWALLS",                  P, "FIREWALLS")

# --- 判讀 -----------------------------------------------------------------
print("\n" + "═" * 60)
print(" 判讀")
print("═" * 60)

# 實際能開的 vCPU = region CPUS 與全域 CPUS_ALL_REGIONS 取最小
# （E2_CPUS 不列入，E2 機型不消耗它）
caps = {"CPUS(region)": cpus, "CPUS_ALL_REGIONS": allreg}
caps = {k: v for k, v in caps.items() if v is not None}
binding = min(caps, key=caps.get)
max_vcpu = caps[binding]

print(f"\n 最緊的 vCPU 上限：{binding} = {max_vcpu}")
print(f" → 最多開 {max_vcpu // vcpu_each} 台 {mt}\n")

if preempt == 0:
    print(" Spot VM：PREEMPTIBLE_CPUS = 0，且從未申請過")
    print("   → 依 GCP 規則，此時 Spot VM 改吃「標準 CPUS 配額」，不是被禁用。")
    print("     所以 Spot 照開，省錢策略不受影響。")
    print("   ⚠️ 但反過來說：Spot 和一般 VM 共用同一份配額，")
    print("      不會因為是 Spot 就多給你額度。")
else:
    print(f" Spot VM：PREEMPTIBLE_CPUS = {preempt}（已配發，Spot 只能吃這份）")

if ssd is not None:
    left = ssd - node_disk
    print(f"\n SSD 配額：{ssd} GB，node 開機碟吃掉 {node_disk} GB，剩 {left} GB 給 PVC")
    print(f"   Week 7 預估 PVC：Kafka 3×20 + MinIO 50 ≈ 110 GB")
    if left < 110:
        print("   ❌ 不夠——縮小 node 開機碟，或 PVC 改用 pd-standard（走 DISKS_TOTAL_GB）")
    else:
        print("   ✅ 夠，但 Week 7 建 PVC 時要記得這條線")
PY

cat <<'EOT'

════════════════════════════════════════════════════════════
 Week 7-8 全開時的資源需求（requests，不是 limits）
════════════════════════════════════════════════════════════
  Kafka broker × 3                     1.5 vCPU /  6 Gi
  Schema Registry                      0.2      /  0.5
  MinIO                                0.3      /  1
  iceberg-rest                         0.2      /  0.5
  Flink JobManager                     0.5      /  1.5
  Flink TaskManager × 2                2.0      /  6
  Trino                                1.0      /  3
  Producer / Gateway / Bot             0.6      /  0.9
  Prometheus + Grafana                 0.8      /  2
  GKE 系統元件                          1.0      /  2
  ──────────────────────────────────────────────────
  總需求                              ~8.1 vCPU / ~23 Gi

  2 × e2-standard-4 實際可配置約 7.8 vCPU / 26 Gi
  （GKE 每台 node 會保留一部分給 kubelet / 系統）
  → vCPU 差 0.3，全開會有 pod 卡在 Pending

════════════════════════════════════════════════════════════
 ⚠️ Free Trial 帳號不能申請提高配額（GCP 官方限制）
    所以上面查到的數字就是硬上限，只能調整架構去配合。
════════════════════════════════════════════════════════════

 下一步：確認 env.sh 的 NODE_COUNT / MACHINE_TYPE 後
        bash k8s/deploy/gcp/04-create-cluster.sh
EOT
