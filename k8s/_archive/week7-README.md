> ⚠️ **已封存。** k8s/ 在 2026-08-26 改成依部署層次分組，
> 這份文件描述的 `k8s/week7/` 目錄已不存在，內容移到 `k8s/10-storage / 20-platform/`。
> 對照表與現行部署順序見 [`k8s/README.md`](../README.md)。
> 保留這份是因為它記錄了當時的推理過程。

---

# Week 7 — 有狀態服務

四週裡最難的一段，Kafka 上 K8s 預留一半以上時間。

實作前要先定案的三件事（`doc/updated_plan/week6-9-gcp-direct.md` 有取捨表）：

1. **儲存**：MinIO on PVC（改動最小，但 PD 閒置也計費）vs. GCS（縮容期幾乎不花錢，但 S3 相容設定要調）
2. **Kafka**：Strimzi Operator vs. Bitnami chart vs. 手刻 StatefulSet
3. **Catalog**：自管 iceberg-rest vs. BigLake Managed Tables

預期檔案：`kafka-statefulset.yaml` / `kafka-headless-svc.yaml` / `kafka-init-job.yaml` /
`minio-statefulset.yaml`（或 GCS 版的 workload-identity 設定）/ `iceberg-rest.yaml` /
`schema-registry.yaml`

⚠️ 最容易卡的一題：broker 的 advertised listener。叢集內走 headless Service 的
pod DNS，叢集外要另外一組 listener——Compose 的 `29092` / `9092` 雙 listener
設計在 K8s 上會變成不同的解法。
