# 農業開放資料平台資料庫 — 架構設計文件

## 專案目標

將農業開放資料平台的資料集下載並存入本地 PostgreSQL 資料庫，進行初步前處理（目前僅限地理圖資），並以 `main.py` 作為應用程式入口驅動整個流程。所有資料集的 metadata 集中儲存於 `metadata_index` 表格。

---

## 模組職責

| 模組 | 職責 |
|------|------|
| `main.py` | 主選單、流程協調、呼叫各模組入口函式 |
| `data_parser.py` | 網頁解析、JSON 下載、分布圖處理 |
| `database_manager.py` | PostgreSQL 連線、CRUD、表格管理、ID 管理 |
| `operations_of_postgresql.py` | 資料庫操作選單（查詢、刪除、列表等） |
| `data_preprocessor.py` | 資料清理與前處理（待實作） |
| `sort_data_by_date.py` | JSON 資料排序（互動式 + 歷史設定） |
| `json_file_operations.py` | 本地 JSON 檔案讀寫、metadata.json 管理 |
| `menu_utils.py` | 選單工具（yes/no、自動確認等） |
| `ui_interface.py` | 使用者輸入介面（popup） |
| `logs_handle.py` | 日誌設定與自訂 log 等級 |
| `utils.py` | 通用工具函式 |

---

## 資料庫表格架構

```
+------------------+       +----------------------+       +--------------------------+
|  metadata_index  |       |   {dataset}_raw      |       |  {dataset}_processed     |
+------------------+       +----------------------+       +--------------------------+
| category_table_id|──────>| category_table_data_id|      | category_table_data_id   |
| 標題             |  (FK) | metadata_id (FK)     |──────>| raw_record_id (FK)       |
| 資料更新日期      |       | ...業務欄位...        |  (FK) | ...處理後欄位...          |
| ...              |       +----------------------+       +--------------------------+
+------------------+
         |
         v
+------------------+       +------------------+
|     files        |       |  record_files    |
+------------------+       +------------------+
| file_id (PK)     |<──────| file_id (FK)     |
| file_path        |       | record_id (FK)   |
| file_name        |       +------------------+
| file_type        |
| file_size        |
+------------------+
```

### ID 命名規則

- `metadata_index`：`category_table_id` = 分類ID（3碼）+ 資料集序列（4碼）
- 原始資料表：`category_table_data_id` = `1` + 分類ID + 資料集序列 + 資料序列（7碼）
- 處理後資料表：`category_table_data_id` = `2` + 分類ID + 資料集序列 + 資料序列（7碼）
- `files`：`file_id` 為純數字遞增字串

### JSON 檔案角色（本地）

不作為即時同步備份，用途限定為：
- 下載中斷的暫存快取（`hand_download/`）
- 排序設定的歷史記錄（`minor_info.json`）
- 偵錯或版本快照的輸出

---

## 主要流程

### 下載流程（`_handle_data_download`）

```
輸入網頁 URL
  → parse_webpage_to_metadata()       # 解析取得 metadata
  → insert_or_update_metadata()       # 寫入 metadata_index
  → check_dataset_content_update_status()
  → fetch_and_process_json_data()     # 下載原始 JSON + 分布圖
  → process_map_json_data()           # 地理圖資：更新 URL 並下載新圖
  → save_dataframe_to_postgresql()    # 存入資料表
```

### 大型統計資料下載（≥ 499 筆）

```
detect_optimal_skip()     # 倍增探針 + 二分收斂，找出末頁 skip 值
  → _fetch_data_logic()   # 逐頁下載
  → _combine_and_filter_data()  # 合併去重
```

### 資料庫管理選單（`operations_of_postgresql`）

提供：列出所有資料集、查詢/顯示資料集內容、刪除資料集（含 metadata）、清理孤立 metadata 條目。

### 前處理流程（待實作）

```
read_data_from_postgresql()          # 讀取原始資料表
  → analyze_and_clean_dataframe()    # 清理空值、記錄清理規則
  → save_dataframe_to_postgresql()   # 存入前處理資料表
```

---

## 資料類型區分

| 類型 | 說明 | 處理入口 |
|------|------|----------|
| 地理圖資 | 含分布圖 URL，需下載並更新圖檔 | `handle_data_download_by_category()` |
| 統計資料（< 499 筆）| 單次請求可取得全部資料 | `handle_data_download_by_category()` 兼任 |
| 統計資料（≥ 499 筆）| 需多頁下載並組合 | `download_and_combine_json()` / `handle_large_data_download()` |

---

## 自動更新

`fully_auto_update(auto_input_list)` 依照傳入的清單批次執行下載，支援 `autodetect_skip` 模式，每筆間隔 1 秒，完成後將 `minor_info` 寫回 SQL 與本地 JSON。
