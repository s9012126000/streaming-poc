#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Week 8 / Step 1b — 建置並推送四個自建 image。
#
# ★ 本週最容易踩的坑：架構不符 ★
#
#   你的 Mac 是 Apple Silicon（arm64），GKE node 是 e2-standard-4（amd64）。
#   直接 `docker build` 出來的是 arm64 image，推上去 pod 會報：
#       exec /usr/local/bin/python: exec format error
#   而且這個錯誤要等到 pod 啟動才會出現，build 和 push 都是成功的。
#
#   解法是 buildx 明確指定 --platform linux/amd64。
#   代價是在 Mac 上要透過模擬層建置，比原生慢 2-5 倍——
#   Python image 大約多花 1-2 分鐘，可以接受。
#
# 用法：
#   bash k8s/deploy/gcp/13-build-push.sh              # 全部四個
#   bash k8s/deploy/gcp/13-build-push.sh producer     # 只建其中一個
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

: "${AR_REPO:=streaming-poc}"
: "${AR_LOCATION:=$REGION}"
: "${PLATFORM:=linux/amd64}"
: "${TAG:=$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d-%H%M)}"

REGISTRY="${AR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}"

# 名稱:Dockerfile 路徑
# build context 一律是專案根目錄，因為 Dockerfile 裡會 COPY utils/、schemas/
# 這些跨目錄的東西。
IMAGES=(
  "producer:docker/producer/Dockerfile"
  "gateway:docker/gateway/Dockerfile"
  "bot:docker/bot/Dockerfile"
  "flink:docker/flink/Dockerfile"
  "web:docker/web/Dockerfile"
)

echo "════════════════════════════════════════════════════"
echo "  Registry : $REGISTRY"
echo "  Tag      : $TAG"
echo "  Platform : $PLATFORM"
echo "════════════════════════════════════════════════════"

# --- 前置檢查 -------------------------------------------------------------
command -v docker >/dev/null || _die "找不到 docker"
docker buildx version >/dev/null 2>&1 || _die "找不到 docker buildx（Docker Desktop 內建）"

# buildx 需要一個 builder instance。預設的 "default" 不支援跨平台建置。
if ! docker buildx inspect poc-builder >/dev/null 2>&1; then
  _info "建立 buildx builder..."
  docker buildx create --name poc-builder --use --bootstrap
else
  docker buildx use poc-builder
fi

# --- 建置 -----------------------------------------------------------------
ONLY="${1:-}"
BUILT=()

for entry in "${IMAGES[@]}"; do
  name="${entry%%:*}"
  dockerfile="${entry#*:}"

  [ -n "$ONLY" ] && [ "$ONLY" != "$name" ] && continue
  [ -f "$dockerfile" ] || { echo "⚠️  跳過 ${name}：找不到 $dockerfile"; continue; }

  echo
  echo "──────────────────────────────────────────────"
  _info "建置 $name  ($dockerfile)"
  echo "──────────────────────────────────────────────"

  # --push 讓 buildx 直接推送，不經過本機 docker images。
  # 跨平台建置的產物本來就不能在本機跑，留在本機沒意義。
  docker buildx build \
    --platform "$PLATFORM" \
    --file "$dockerfile" \
    --tag "${REGISTRY}/${name}:${TAG}" \
    --tag "${REGISTRY}/${name}:latest" \
    --push \
    .

  BUILT+=("$name")
  _ok "$name 推送完成"
done

[ ${#BUILT[@]} -eq 0 ] && _die "沒有建置任何 image（名稱打錯？）"

# --- 驗證架構 -------------------------------------------------------------
echo
echo "════════════════════════════════════════════════════"
echo " 驗證：確認推上去的是 amd64"
echo "════════════════════════════════════════════════════"
ARCH_PARSER=$(cat <<'PYEOF'
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print("parse-error"); sys.exit()
if "manifests" in d:
    out = []
    for m in d["manifests"]:
        p = m.get("platform", {})
        if p.get("architecture") == "unknown":
            continue        # buildx 的 attestation 層，不是真的 image
        out.append(f"{p.get('os')}/{p.get('architecture')}")
    print(",".join(out) or "none")
else:
    # 單一架構的 manifest 沒有 manifests 陣列，架構在 config 裡
    print(d.get("architecture", "single-unknown"))
PYEOF
)

BAD=0
for name in "${BUILT[@]}"; do
  arch=$(docker manifest inspect "${REGISTRY}/${name}:${TAG}" 2>/dev/null \
         | python3 -c "$ARCH_PARSER" 2>/dev/null || echo "?")
  if echo "$arch" | grep -qE "linux/amd64|^amd64$"; then
    _ok "${name}: ${arch}"
  else
    echo "  ❌ ${name}: ${arch} —— 不是 amd64，pod 會 exec format error"
    BAD=$((BAD+1))
  fi
done

if [ "$BAD" -gt 0 ]; then
  echo
  echo "❌ 有 ${BAD} 個 image 架構不符。確認 PLATFORM=linux/amd64 後重建。"
  exit 1
fi

cat <<EOT

────────────────────────────────────────────────────────────
 在 manifest 裡引用（建議用具體 tag 而不是 latest，
 latest 不會觸發 K8s 重新拉取，改了也不會生效）：

   image: ${REGISTRY}/producer:${TAG}
   image: ${REGISTRY}/gateway:${TAG}
   image: ${REGISTRY}/bot:${TAG}
   image: ${REGISTRY}/flink:${TAG}
   image: ${REGISTRY}/web:${TAG}

 這次的 tag 是：${TAG}
 存進 env.sh 方便 manifest 引用：
   echo 'export IMAGE_TAG=${TAG}' >> k8s/deploy/gcp/env.local.sh

 列出已推送的 image：
   gcloud artifacts docker images list ${REGISTRY} --include-tags
────────────────────────────────────────────────────────────
EOT
