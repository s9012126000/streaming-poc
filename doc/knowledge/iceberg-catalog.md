# Iceberg 的 catalog 與物件儲存

> 起因：某天早上 Trino 回 `Schema 'bronze' does not exist`，
> 但 MinIO 裡 59,078 筆資料的 Parquet 檔好端端躺著

---

## 兩層，缺一不可

```
catalog（目錄）          物件儲存（書架）
─────────────────       ─────────────────────────
「有哪些 namespace」      metadata.json（表的結構、快照歷史）
「有哪些 table」          manifest list / manifest（哪些檔案屬於哪個快照）
「現在該讀哪個            data files（真正的 Parquet）
  metadata.json」
```

**書還在書架上，被燒掉的是索引卡。**

這個比喻精確到什麼程度——catalog 的核心只有一張表，兩個欄位：

```sql
iceberg_tables
  catalog_name | table_namespace | table_name | metadata_location
  ─────────────┼─────────────────┼────────────┼──────────────────────────
  rest         | bronze          | trades     | s3://warehouse/bronze/trades/
                                              |   metadata/00042-xxx.metadata.json
```

`metadata_location` 就是那張索引卡。**整個 catalog 的價值，
是一串指向物件儲存的路徑字串。**

---

## iceberg-rest 不是 catalog，是翻譯機

這一層繞很容易看暈：

```
Flink / Trino ──REST 協定──▶ iceberg-rest ──JDBC──▶ Postgres
                             (講 REST)      (內部真正的 catalog)
```

外面的 client 只會講 Iceberg REST 規格。iceberg-rest 把 REST 請求
轉成對真正 catalog 的操作——**所以它自己也需要一個後端**。

```yaml
CATALOG_CATALOG__IMPL: org.apache.iceberg.jdbc.JdbcCatalog
CATALOG_URI: jdbc:postgresql://catalog-db:5432/iceberg
```

Iceberg 的 catalog 實作是可抽換的：

```
org.apache.iceberg.jdbc.JdbcCatalog       ← 關聯式資料庫  ★ 我們用這個
org.apache.iceberg.hive.HiveCatalog       ← Hive Metastore
org.apache.iceberg.aws.glue.GlueCatalog   ← AWS Glue
org.apache.iceberg.hadoop.HadoopCatalog   ← 直接用檔案系統目錄
```

> `HadoopCatalog` 不建議用在生產環境，原因見下方「commit 的樂觀併發」。

### 環境變數的命名轉換

`tabulario/iceberg-rest` image 的慣例：去掉 `CATALOG_` 前綴 →
單底線變 `.` → **雙底線變 `-`** → 全部轉小寫。

```
CATALOG_CATALOG__IMPL            → catalog-impl
CATALOG_S3_PATH__STYLE__ACCESS   → s3.path-style-access
CATALOG_JDBC_USER                → jdbc.user
```

啟動日誌的第一行會印出實際生效的設定，可以直接驗證：

```
Creating catalog with properties: {jdbc.user=iceberg, ...,
  uri=jdbc:postgresql://catalog-db:5432/iceberg}
```

看到 `jdbc.user=user`（image 的預設值）就代表環境變數沒吃到。

---

## 坑 1：預設的 catalog 不會存活過重啟

`tabulario/iceberg-rest` 沒設 `CATALOG_URI` 時，預設是：

```
jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory
```

**記憶體裡的 SQLite。** 容器一重啟，「有哪些表」全部歸零。
Parquet 檔全都變成沒人指向的孤兒。

### 為什麼 compose 從沒暴露這個問題

`docker-compose.yml` 裡 iceberg-rest 的 volumes 數量是 **0**，
也同樣沒設 `CATALOG_URI`——**本機版一直是同一個 bug**。

沒炸的原因有兩個，而且都是巧合：

1. `restart: unless-stopped` 讓容器一路活著
2. 每次真的重來都會重跑 `flink-init.sql`，而它開頭是
   `CREATE DATABASE IF NOT EXISTS`——對著空 catalog 就變成「建立」

也就是說，**每次都在重建 catalog，只是沒察覺**。

K8s 每天 down/up 一次，但提交 Flink job 的腳本不是自動跑的。
第一次出現「重啟了但沒重跑 init」的組合，缺口就露出來了。

> **compose 的生命週期讓這個 bug 沒有機會發生。**
> 這句話適用於這次遷移一半以上的坑。

---

## 坑 2：SQLite 撐不住並行寫入

第一次修正是把 SQLite 放到 PVC 上。解決了持久化，但隔天：

```
org.sqlite.SQLiteException: [SQLITE_BUSY] The database file is locked
  → org.apache.iceberg.jdbc.UncheckedSQLException
  → HTTP 500
  → org.apache.iceberg.exceptions.CommitStateUnknownException
```

**SQLite 是單寫者模型**——同一時間只能有一個寫入交易，第二個不是排隊，
是直接失敗。兩個寫 Iceberg 的 Flink job checkpoint interval 都是 60 秒，
commit 撞在一起就是這樣。

症狀的分界乾淨得不像巧合：

```
Kafka → Iceberg   ❌ RESTARTING   要經過 iceberg-rest 的 commit
Kafka → Kafka     ✅ RUNNING      完全不碰 catalog
```

### 當初的判斷錯在哪

manifest 裡寫的理由是：

> 限制：只能 1 副本（SQLite 檔案鎖），但我們本來就是 `replicas: 1`

**只考慮了「伺服器有幾個副本」，忽略了「有幾個 client 同時寫」。**
單副本的伺服器一樣會收到並行請求。

最糟的是它讓一個真實的限制**看起來像是已經被評估過並排除了**——
比完全沒提到還糟。

### 修正：Postgres

| | SQLite | Postgres |
|---|---|---|
| 並行寫入 | 單寫者，第二個直接失敗 | MVCC，只在動到同一列時互等 |
| 對應到 Iceberg | — | 更新 `iceberg_tables` 的某一列，不同表不會碰到同一列 |

---

## `CommitStateUnknownException` 為什麼是死局

```
Cannot determine whether the commit was successful or not...
Retrying an already successful operation will result in duplicate records
```

它的語意是「Iceberg 不知道那次 commit 到底成功了沒」。所以它：

- **拒絕自動重試**
- **不刪任何檔案**（包括可能無用的 manifest list）
- 停下來要人介入

**這個設計是對的**——在不確定的情況下，寧可卡住也不要靜默產生重複資料。

而失敗發生在 `IcebergFilesCommitter.initializeState`（從 checkpoint 復原時），
所以每次重啟都從同一個壞掉的起點開始。**無法自癒的迴圈，等再久都不會好。**

> 判斷是不是這種情況：看堆疊有沒有 `initializeState`。
> 在 `processElement` 裡失敗的通常重試就好；在 `initializeState` 裡失敗的不會。

---

## commit 的樂觀併發（第三層 fencing）

Iceberg 的 commit 是一次 CAS：

> 「如果 `metadata_location` 還是 `00042-xxx.json`，就換成 `00043-yyy.json`」

兩個 writer 同時 commit，只有一個成功，另一個收到衝突後重試。

**這是為什麼 catalog 必須是強一致的儲存。** 如果 catalog 是 S3 上的一個檔案
（沒有 CAS），這層保護就不存在——這正是 `HadoopCatalog` 不建議用在生產環境的原因。

### 同一個原理在這個 stack 出現三次

| 層 | token 是什麼 | 誰拒絕 |
|---|---|---|
| Flink HA | ConfigMap 的 `resourceVersion` | etcd |
| Flink RPC | `leaderSessionID` | TaskManager |
| **Iceberg commit** | 當前的 `metadata_location` | **catalog** |

所以就算 leader election 完全失效，Iceberg 這層還會擋一次。
**任何允許併發寫入的系統，最終都得在某個地方做這件事。**
見 [distributed-coordination.md](distributed-coordination.md)。

---

## 「有沒有狀態」比想像中難判斷

同一份 manifest，三天內對同一個問題給了三個答案：

| 日期 | 判斷 | 結果 |
|---|---|---|
| 8/24 | 無狀態 | ❌ 隔天 catalog 失憶 |
| 8/25 | 有狀態 → PVC + SQLite | ⭕ 但選錯後端 |
| 8/26 | 無狀態 → 狀態外推到 Postgres | ✅ |

第一次和第三次結論一樣但理由完全不同：
第一次是「以為狀態不存在」，第三次是「把狀態移到別人那裡了」。

**要問的不是「它有沒有 volume」，而是「哪些東西重啟後必須還在」。**

---

## 兩個 Postgres 專屬的坑

**`PGDATA` 必須指向子目錄。**

```yaml
- name: PGDATA
  value: /var/lib/postgresql/data/pgdata    # ← 不能直接用 .../data
```

GKE 掛載 ext4 的 PVC 時根目錄會有 `lost+found`，而 `initdb` 堅持要一個
完全空的目錄：

```
initdb: error: directory "/var/lib/postgresql/data" exists but is not empty
```

**probe 用 `pg_isready` 而不是 `nc -z`。**
initdb 期間 5432 就已經在聽了，但那時還不能用——
`nc` 會誤判成就緒，然後 iceberg-rest 連上去失敗、直接退出。

---

## 查證用的指令

```bash
# catalog 的後端是什麼
kubectl exec -n streaming deploy/iceberg-rest -- printenv CATALOG_URI

# 索引卡長什麼樣
kubectl exec -n streaming catalog-db-0 -- psql -U iceberg -d iceberg \
  -c "SELECT table_namespace, table_name, metadata_location FROM iceberg_tables;"

# 書架上有什麼
kubectl exec -n streaming minio-0 -- du -sh /data/warehouse/*
```

> ⚠️ 查詢失敗時**不要吞掉 stderr**。`Schema 'bronze' does not exist`
> 和「表是空的」是完全不同的訊息，但 `2>/dev/null | tr -dc '0-9'`
> 會把兩者變成同一個 `0`。這個錯誤讓一次診斷往完全錯誤的方向走了一輪。
