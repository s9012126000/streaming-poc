# 知識庫

這個專案在遷移過程中弄懂的東西，整理成獨立文件。

跟 `doc/updated_plan/` 的分工：

| | 內容 |
|---|---|
| `doc/updated_plan/` | **敘事**：那一週遇到什麼、因此改成怎麼做 |
| `doc/knowledge/`（這裡） | **概念**：這個東西是什麼、為什麼這樣設計 |

概念文件裡的每個結論都盡量附上這個專案的實際證據——
錯誤訊息、指令輸出、manifest 片段。沒有實測過的地方會明說。

---

## 目錄

| 文件 | 內容 | 起因 |
|---|---|---|
| [k8s-objects.md](k8s-objects.md) | Namespace / Pod / Deployment / StatefulSet / Service / ConfigMap / Secret / PVC，以及 etcd 在底下扮演什麼角色 | 「ConfigMap 是像 docker volume 的地方嗎」 |
| [k8s-scheduling-health.md](k8s-scheduling-health.md) | pod 怎麼被排到 node 上、`topologySpreadConstraints`、resources、三種 probe | 「哪裡設定分散在不同 node」、bot 被自己的 probe 殺掉 |
| [k8s-rbac.md](k8s-rbac.md) | ServiceAccount / Role / RoleBinding，以及為什麼權限不足的症狀不像權限問題 | Flink HA 與 Prometheus 都需要 |
| [distributed-coordination.md](distributed-coordination.md) | leader election、fencing token、共識演算法 vs 租約、Flink HA 的實作 | 「單副本為什麼要選 leader」 |
| [observability.md](observability.md) | Prometheus 的服務發現與 relabel、告警的四個狀態、靜默失效 | `static_configs` → `kubernetes_sd_configs` |
| [iceberg-catalog.md](iceberg-catalog.md) | catalog 與物件儲存的分工、`metadata_location`、commit 的樂觀併發 | catalog 失憶事件 |
| [debugging-playbook.md](debugging-playbook.md) | 這四週歸納出的除錯方法 | 8 次驗證工具騙人 |
| [kb-kafka.md](kb-kafka.md) | Kafka：quorum、ISR、advertised listener、Avro wire format | Week 7 |
| [project_schema.md](project_schema.md) | 服務之間怎麼串（含資料流圖） | 「可以簡單說明服務之間的關係嗎」 |
| [data-journey-kafka-to-minio.md](data-journey-kafka-to-minio.md) | 一筆成交從 Kafka 到 Parquet 的完整路徑 | Week 2–3 |

---

## 貫穿全部的三句話

**1. 「資料還在」和「系統知道資料在哪」是兩件事。**
Iceberg catalog 與 Flink HA 兩次故障的本質完全相同——本體好好的，指標沒了。
書還在書架上，被燒掉的是索引卡。

**2. 弱訊號的監控沒有上行空間，只有下行風險。**
一個「聊勝於無」的健康檢查，好的情況是提供不了資訊，
壞的情況是殺掉健康的服務。這不是取捨，是純粹的負面。

**3. 你沒辦法從「沒有告警」推論出「系統正常」。**
因為「告警壞了」也長這樣。唯一的辦法是製造一次真的故障。
