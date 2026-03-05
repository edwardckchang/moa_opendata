# 農業開放資料平台 — 重構待辦清單

> 目標結構：`config/` / `core/` / `service/` / `cli/` / `utils/`  
> 原則：由底層往上層、最小有效改動、每階段完成後驗證再進行下一階段  
> 資料前處理不在本專案範疇，相關項目已移除

---

## 重構 Phase 1：消除跨模組全域狀態

> 目標：穩固 `DB` 連線傳遞方式，消除隱式全域依賴，不改任何業務邏輯  
> 風險等級：低（向後相容修改）  
> 完成標準：所有主選單選項跑過一遍無 exception，日誌輸出與改動前一致

### 1-A　`database_manager.py` — `_execute_sql` 增加 `conn` 參數

- [ ] 為 `_execute_sql()`、`_table_exists()`、`_ensure_db_connection()` 增加可選的 `conn` 參數（預設 `None`，`None` 時 fallback 到模組全域 `DB`），不改任何現有呼叫

### 1-B　`main.py` — 修正重複連線問題

- [ ] 將 `main()` while 迴圈內的 `DB = connect_db(...)` 移除，連線只在 `__main__` 區塊執行一次
- [ ] 確認 `init(db_connection)` 是唯一設定 `DB` / `global_metadata_cache` / `minor_info` 的入口，移除其他散落的 `global DB` 賦值

### 1-C　`operations_of_postgresql.py` — 移除重複初始化副作用

- [ ] 將 `operations_of_postgresql()` 函式開頭的 `get_global_data()` 呼叫移除（信任 `init()` 已初始化）
- [ ] 將 `pd.set_option(...)` 從 `operations_of_postgresql()` 移除，確保只在 `main.py` 的 `__main__` 區塊執行一次

### 1-D　`auto_update_data_moa.py` — 對齊連線初始化方式

- [ ] 確認 `__main__` 區塊的 `DB` 連線與初始化流程與 `main.py` 一致（目前兩者各自連線，邏輯重複）

---

## 重構 Phase 2：標記層次邊界、建立目標目錄結構

> 目標：在不移動任何程式碼的前提下，以註解標記清楚各函式所屬層次，並建立新目錄骨架  
> 風險等級：極低（無執行邏輯變動）  
> 完成標準：新目錄與空檔案建立完成，各模組函式均有層次標記

### 2-A　在 `database_manager.py` 內標記三個層次

- [ ] 以區塊註解標記 **Layer 1 Infrastructure**（`_execute_sql`、`_table_exists`、`_get_all_tables`、`_ensure_db_connection`、`connect_db`）
- [ ] 以區塊註解標記 **Layer 2 Repository**（`save_dataframe_to_postgresql`、`insert_or_update_metadata`、`check_metadata_update_status`、`check_dataset_content_update_status`、`get_metadata`、`get_table_columns`、`get_dataset_content_for_list`、`delete_table_and_metadata_entry`、`delete_all_data_from_table`、`rename_data_tables`、`create_file_entry_with_upsert`、`insert_record_file_entry`、`get_files_for_record`、`create_empty_table_unexistent`、`create_files_table_if_not_exists`、`create_record_files_table_if_not_exists`、`_insert_records_to_postgresql`、`get_max_category_table_data_id`、`preserve_old_data_by_date`）
- [ ] 以區塊註解標記 **Layer 3 Cache / State**（`GLOBAL_METADATA_CACHE`、`get_global_data`、`_generate_global_data`、`get_minor_info_data`、`save_minor_info_to_sql`）

### 2-B　建立目標目錄骨架（空檔案）

- [ ] 建立 `config/__init__.py`、`config/settings.py`（.env 載入與 config 值集中管理）
- [ ] 建立 `core/__init__.py`、`core/db.py`（連線管理）、`core/repository.py`（Layer 1 目標位置）
- [ ] 建立 `service/__init__.py`、`service/download.py`、`service/dataset.py`、`service/maintenance.py`
- [ ] 建立 `cli/__init__.py`、`cli/main_menu.py`、`cli/db_menu.py`
- [ ] 確認 `utils/`、`logs_handle.py` 保持原位不動（無依賴，不需移動）

### 2-C　釐清各模組函式的目標歸屬，記錄於本文件

| 現有位置 | 函式 | 目標模組 |
|---|---|---|
| `database_manager.py` | `connect_db` | `core/db.py` |
| `database_manager.py` | `_execute_sql`, `_table_exists`, `_get_all_tables`, `_ensure_db_connection` | `core/repository.py` |
| `database_manager.py` | Layer 2 Repository 全部 | `core/repository.py` |
| `database_manager.py` | `get_global_data`, `get_minor_info_data`, `save_minor_info_to_sql`, `GLOBAL_METADATA_CACHE` | `service/dataset.py` |
| `main.py` | `_handle_data_download`, `handle_data_download_by_user_setting`, `fully_auto_update` | `service/download.py` |
| `main.py` | `update_by_metadata`, `metadata_selection`, `get_count`, `get_value_from_minorinfo`, `get_value_from_global_metadata_cache` | `service/dataset.py` |
| `main.py` | `main()` while 選單 | `cli/main_menu.py` |
| `auto_update_data_moa.py` | `fully_auto_update`（目前與 `main.py` 重複） | 合併至 `service/download.py` |
| `operations_of_postgresql.py` | `operations_of_postgresql()` 及所有 `_` 前綴函式 | `cli/db_menu.py` |
| `operations_of_postgresql.py` | `get_metadata`, `_listing_metadata`, `_match_table_indexes_and_names` | `service/dataset.py` |
| `db_maintenance.py` | `create_indexes_for_all_tables` | `service/maintenance.py` |
| `dotenv_values()` 呼叫散落各 `__main__` | 全部集中 | `config/settings.py` |

---

## 重構 Phase 3：逐步搬移程式碼

> 目標：將函式依 Phase 2 歸屬表逐步搬移至新目錄，每搬一批即在原位留轉接函式  
> 風險等級：中（需逐步驗證）  
> 完成標準：所有新模組可獨立 import，原有 import 路徑透過轉接函式保持相容，主流程測試通過

### 3-A　搬移 `core/` 層（依賴最少，優先搬）

- [ ] 將 `connect_db()` 搬移至 `core/db.py`，在 `database_manager.py` 原位留轉接：`from core.db import connect_db`
- [ ] 將 Layer 1 Infrastructure 函式（`_execute_sql`、`_table_exists`、`_get_all_tables`、`_ensure_db_connection`）搬移至 `core/repository.py`，`database_manager.py` 原位留轉接
- [ ] 驗證：`operations_of_postgresql.py` 與 `db_maintenance.py` 的現有呼叫正常

### 3-B　搬移 `config/settings.py`

- [ ] 將三個 `__main__` 區塊（`main.py`、`auto_update_data_moa.py`、`operations_of_postgresql.py`）中重複的 `.env` 載入與 `USERNAME/PASSWORD/DBNAME` 讀取，集中至 `config/settings.py`，各 `__main__` 改為 `from config.settings import USERNAME, PASSWORD, DBNAME`
- [ ] 驗證：三個入口點獨立執行均能正常連線

### 3-C　搬移 `service/dataset.py`

- [ ] 將 `get_global_data()`、`get_minor_info_data()`、`save_minor_info_to_sql()`、`GLOBAL_METADATA_CACHE` 從 `database_manager.py` 搬移至 `service/dataset.py`
- [ ] 將 `main.py` 的 `metadata_selection()`、`get_count()`、`get_value_from_minorinfo()`、`get_value_from_global_metadata_cache()`、`update_by_metadata()` 搬移至 `service/dataset.py`
- [ ] 將 `operations_of_postgresql.py` 的 `get_metadata()`、`_listing_metadata()`、`_match_table_indexes_and_names()` 搬移至 `service/dataset.py`
- [ ] 各原位留轉接函式，驗證主流程與資料庫選單正常

### 3-D　搬移 `service/download.py`

- [ ] 將 `main.py` 的 `_handle_data_download()`、`handle_data_download_by_user_setting()`、`fully_auto_update()` 搬移至 `service/download.py`
- [ ] 確認 `auto_update_data_moa.py` 的 `update_by_metadata()` 呼叫改為 `from service.download import ...`（解除目前 `auto_update_data_moa` → `main` 的循環依賴風險）
- [ ] 驗證：下載主流程與自動更新流程正常

### 3-E　搬移 `service/maintenance.py`

- [ ] 將 `db_maintenance.py` 的 `create_indexes_for_all_tables()` 搬移至 `service/maintenance.py`
- [ ] `db_maintenance.py` 原位留轉接，`operations_of_postgresql.py` 的 import 改為新路徑
- [ ] 驗證：資料庫管理選單「建立索引」功能正常

### 3-F　搬移 `cli/` 層（最後搬，上層依賴下層）

- [ ] 將 `main.py` 的 `main()` 選單主體搬移至 `cli/main_menu.py`，`main.py` 保留 `__main__` 初始化與 `cli/main_menu.py` 的呼叫
- [ ] 將 `operations_of_postgresql.py` 的 `operations_of_postgresql()` 及所有 `_` 前綴函式搬移至 `cli/db_menu.py`，`operations_of_postgresql.py` 原位留轉接
- [ ] 驗證：兩個 CLI 入口點完整流程測試通過

### 3-G　清理轉接函式（所有階段完成後）

- [ ] 確認所有 import 路徑均已更新為新模組路徑
- [ ] 逐一移除 Phase 3 各步驟留下的轉接函式
- [ ] 確認 `database_manager.py` 僅剩 Layer 2 Repository 函式（Layer 1 已在 `core/repository.py`，Layer 3 已在 `service/dataset.py`）
- [ ] 最終完整流程驗證

---

## 系統測試

- [ ] 5.6 系列其餘間接呼叫函式的測試補齊（與重構 Phase 1 並行進行）

---

## 已完成

### 資料下載與存入資料庫

- [x] `parse_webpage_to_metadata()`：解析指定網頁，建立原始資料 metadata（2025/6/2 測試完成）
- [x] `fetch_and_process_json_data()`：下載 JSON 資料集並關連 metadata（測試完成）
- [x] `process_map_json_data()`：地理圖資更新分布圖 URL 並下載新圖檔（已取消直接下載圖檔）
- [x] `handle_data_download_by_category()`：整合下載流程並存入 PostgreSQL，兼任 < 499 筆統計資料（2025/6/2 測試完成）
- [x] `download_and_combine_json()` / `detect_optimal_skip()`：大型統計資料多頁下載，實作倍增探針 + 二分收斂找末頁（2025/6/16 測試完成）
- [x] `handle_large_data_download()`：main.py 中大型統計資料的獨立選單入口

### 資料庫管理

- [x] `create_empty_table_unexistent()`（2025/6/2 測試完成）
- [x] `insert_or_update_metadata()`（2025/6/2 測試完成）
- [x] `check_metadata_update_status()`（2025/6/2 測試完成）
- [x] `check_dataset_content_update_status()`（2025/6/16 測試完成）
- [x] `save_dataframe_to_postgresql()`（測試完成）
- [x] `_insert_records_to_postgresql()`（測試完成）
- [x] `get_max_id_for_data()`（2025/6/16 修改測試完成）
- [x] `clean_table_name()`（2025/6/2 測試完成）
- [x] `connect_db()`（2025/6/2 測試完成）
- [x] `replace_url_parameters()`（2025/6/2 測試完成）
- [x] `table_columns_sql()`（2025/6/2 測試完成）
- [x] `update_local_metadata_file()`（2025/6/2 測試完成）

### 資料庫操作選單（`operations_of_postgresql`）

- [x] 建立操作選單
- [x] `_get_all_metadata_lists()`：讀取所有資料集概要列表（2025/6/16 測試完成）
- [x] `_get_metadata_by_identifier()`：依識別碼讀取 metadata（2025/6/16 修改測試完成）
- [x] 讀取資料集內容

### 資料庫架構變革

- [x] 建立 `files` 表格（`create_files_table_if_not_exists()`）
- [x] 建立 `record_files` 橋接表（`create_record_files_table_if_not_exists()`）
- [x] `create_file_entry_with_upsert()`：新增或更新 files 表格條目
- [x] `get_files_for_record()`：透過 JOIN 查詢指定記錄的所有關聯檔案
- [x] ID 命名規則確立：metadata = 分類ID(3) + 序列(4)；原始資料 = `1` + 前述；處理後 = `2` + 前述
- [x] `minor_info` 同步寫入 SQL（`save_minor_info_to_sql()`）