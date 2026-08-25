#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 共用設定：所有 k8s/deploy/gcp/*.sh 都會 source 這一支。
# 想改設定，改這裡，或在外面 export 同名變數覆蓋。
#
#   source k8s/deploy/gcp/env.sh
# ---------------------------------------------------------------------------

# --- 專案 ---------------------------------------------------------------
# 留空 = 用 gcloud 目前的 default project
: "${PROJECT_ID:=$(gcloud config get-value project 2>/dev/null)}"

# --- 位置 ---------------------------------------------------------------
# 刻意用 zonal（單一 zone）叢集：regional 叢集會在 3 個 zone 各開一組 node，
# 成本 x3，$300 額度撐不住。
# 2026-08-23 從 us-central1 改到 asia-east1（台灣彰化）。
#
# 原因：Binance.com 依服務條款封鎖美國 IP，us-central1 的 pod 連過去
#       收到 HTTP 451「Service unavailable from a restricted location」。
#       這不是設定問題，是地理限制，繞不過去。
#
# 當初選 us-central1 純粹是為了成本（最便宜的區域），
# **完全沒考慮資料來源的地理限制**——這是規劃階段的盲點：
# 選 region 時只想到「機器多少錢」，沒想到「這裡連得到我要的資料嗎」。
#
# 附帶好處：使用者在台灣，kubectl / port-forward 的往返延遲從
# 約 150ms 降到約 10ms，整天的操作體感差很多。
# 附帶代價：asia-east1 的 Spot 價格略高於 us-central1，
#           且該區的 Spot 容量狀況需要重新觀察。
: "${REGION:=asia-east1}"
: "${ZONE:=asia-east1-b}"

# --- 叢集 ---------------------------------------------------------------
# 全程只開這一個叢集。第二個叢集開始就要付管理費。
: "${CLUSTER_NAME:=streaming-poc}"
# 叢集建立時就把預設 pool 開成 Spot，名字因此固定是 default-pool。
# 不另開 spot-pool 的原因見 04-create-cluster.sh：E2_CPUS 只有 8，
# 兩個 pool 並存的瞬間會超出配額。
: "${NODE_POOL:=default-pool}"

# --- node 規格 ----------------------------------------------------------
# 2026-08-09 實測配額（見 doc/updated_plan/week6-decisions.md）：
#   CPUS (us-central1)    = 32  ← E2 機型實際吃這條
#   E2_CPUS               = 8   ← 名字像但 E2 不吃它（實測 usage 恆為 0）
#   CPUS_ALL_REGIONS      = 12  ← 真正的上限，最多 3 台 e2-standard-4
#   SSD_TOTAL_GB          = 250 ← pd-balanced / pd-ssd 都吃這條
# Free Trial 帳號不能申請提高配額，所以這些是硬上限，不是起點。
#
# 2026-08-19 從 2 台改成 3 台。原本的理由是「6.0 vCPU 需求，2 台的 7.84 就夠」——
# 那個判斷只看資源，漏掉了容錯：
#   2 台 → Kafka 最佳分佈是 2+1，掉「2」那台就失去 quorum
#          等於有一半機率單節點故障是致命的
#   3 台 → 1+1+1，掉任何一台都只少 1 個 broker，必定存活
#          正好對上 quorum 的容錯能力（3 節點容忍 1 個故障）
# 成本約每月 +$12–30（Spot）。3 × 4 = 12 vCPU 剛好吃滿 CPUS_ALL_REGIONS，
# 之後不能再開任何 VM——以這個 PoC 的規模夠用。
: "${MACHINE_TYPE:=e2-standard-4}"
: "${NODE_COUNT:=3}"

# 開機碟壓到 32GB：GKE node 的映像檔約 10GB，32 已經很寬裕。
# 2 台只吃 64GB SSD 配額，留 186GB 給 Week 7 的 PVC。
: "${DISK_TYPE:=pd-balanced}"
: "${DISK_SIZE:=32}"          # 每個 node 的開機碟 GB（不是 PVC）

# Week 7 建 PVC 時用這個 StorageClass。
#   standard-rwo = pd-balanced → 吃 SSD_TOTAL_GB（250，緊）
#   standard     = pd-standard → 吃 DISKS_TOTAL_GB（2048，寬鬆）
# PoC 的資料量感覺不出效能差異，優先選不會撞配額的那個。
: "${PVC_STORAGE_CLASS:=standard}"

# --- 預算 ---------------------------------------------------------------
# ⚠️ 幣別必須和帳單帳號一致，不然 budgets API 回 INVALID_ARGUMENT，
#    而且錯誤訊息完全不提幣別（實測：300USD 失敗、9000TWD 成功）。
#    這個帳號是 TWD，所以計畫文件裡的「$300 額度」實際上是約 NT$9,000。
#
#    BUDGET_CURRENCY 留空 = 不帶幣別後綴，直接用帳單帳號的幣別（最安全）。
: "${BUDGET_CURRENCY:=TWD}"
: "${BUDGET_AMOUNT:=9000}"
: "${BUDGET_NAME:=streaming-poc-trial}"

# 之後所有成本估算用的換算率。文件裡的數字都是 USD，
# 但帳單和預算警報都是 TWD——看報表時要記得乘回來。
: "${USD_TO_TWD:=30}"

# 留空 = 由 02-budget-alert.sh 自動抓帳單帳號
: "${BILLING_ACCOUNT:=}"

# --- K8s ----------------------------------------------------------------
: "${K8S_NAMESPACE:=streaming}"

# ---------------------------------------------------------------------------
_die() { echo "❌ $*" >&2; exit 1; }
_ok()  { echo "✅ $*"; }
_info(){ echo "→  $*"; }

[ -n "$PROJECT_ID" ] || _die "PROJECT_ID 沒設定，也抓不到 gcloud default project。跑 gcloud config set project <id>"
