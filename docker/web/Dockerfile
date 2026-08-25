 # ---------------------------------------------------------------------------
# Web 前端 —— 這是「改寫」不是「搬遷」。
#
# compose 版本是開發模式：
#     image: node:22-alpine
#     volumes: ./web:/app  +  web_node_modules:/app/node_modules
#     command: npm install && npm run dev -- --host 0.0.0.0
#
# 那個模式在 K8s 上三件事都不成立：
#   1. 沒有 bind mount —— 原始碼要打進 image
#   2. 不該在容器啟動時 npm install —— 慢、不可重現、要對外網路
#   3. vite dev server 不是給生產用的 —— 沒有壓縮、單執行緒、會 watch 檔案
#
# 改成兩階段 build：build 階段產出靜態檔，執行階段只有 nginx。
# 最終 image 從約 1.2GB（node + node_modules）降到約 50MB。
# ---------------------------------------------------------------------------

# --- 階段 1：建置 ----------------------------------------------------------
FROM node:22-alpine AS build

WORKDIR /app

# 先只複製 package 檔案再 npm ci——這樣只要相依沒變，
# 這一層就能命中快取，改前端程式碼時不用重裝依賴。
COPY web/package.json web/package-lock.json ./
RUN npm ci

# 再複製原始碼。順序反過來的話每次改 code 都要重裝依賴。
COPY web/ ./
RUN npm run build          # 產出 /app/dist

# --- 階段 2：執行 ----------------------------------------------------------
FROM nginx:1.27-alpine

# 只把建置產物帶過來，node 和 node_modules 全部留在階段 1
COPY --from=build /app/dist /usr/share/nginx/html
COPY docker/web/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 8080

# nginx 官方 image 的預設 CMD 已經是 nginx -g "daemon off;"，不用覆寫
