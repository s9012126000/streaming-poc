> ⚠️ **已封存。** k8s/ 在 2026-08-26 改成依部署層次分組，
> 這份文件描述的 `k8s/week8/` 目錄已不存在，內容移到 `k8s/30-compute / 40-app/`。
> 對照表與現行部署順序見 [`k8s/README.md`](../README.md)。
> 保留這份是因為它記錄了當時的推理過程。

---

# Week 8 — 運算工作負載 + 對外存取

要先定案：

1. **Flink**：Kubernetes Operator（CRD、支援 savepoint 升級）vs. Standalone Deployment
   —— 現在 `docker/flink/sql/*.sql` 是「容器啟動時跑 SQL Client 送 Job」，
   這個模式在 K8s 上要重新設計，是與 Compose 之間最大的架構差異
2. **App 層**：GKE Deployment vs. Cloud Run
   —— Cloud Run 能縮到 0（額度友善），但 SSE 長連線逾時與 Bot 的常駐 Kafka
   consumer 是否適合，要先驗證
3. **對外**：Ingress（LB 計費）vs. port-forward（零成本先求有）

也是第一次需要推自己的 image → Artifact Registry
（`gateway` / `bot` / `producers` / `flink` 都有自己的 Dockerfile）。

預期檔案：`flink-jobmanager.yaml` / `flink-taskmanager.yaml` / `flink-job.yaml` /
`producer-deployment.yaml` / `gateway.yaml` / `web.yaml` / `bot.yaml` /
`trino.yaml` / `ingress.yaml`
