# db_maintenance.py
from sort_data_by_date import _interactive_sort_data
from logs_handle import logger
from json_file_operations import load_minor_info, save_minor_info
from database_manager import _execute_sql, _table_exists, save_minor_info_to_sql

def create_indexes_for_all_tables():
    print("--- 開始執行資料庫索引維護作業 ---")
    
    minor_info: dict = load_minor_info()
    all_sort_configs = minor_info.get("all_sort_configs", [])
    is_modified = False
    for config in all_sort_configs:
        table_id = config.get("category_table_id")
        file_name = config.get("file_name", "未知標題")
        current_sort_keys = config.get("sort_keys", [])
        
        if not _table_exists(table_id):
            continue

        # --- 新增功能：互動式欄位確認 ---
        print(f"\n" + "="*50)
        print(f"正在處理表格: <{file_name}> ({table_id})")
        
        # 1. 取得第一筆資料作為樣板 (Sample)
        sample_query = f'SELECT * FROM "{table_id}" LIMIT 1;'
        sample_data = _execute_sql(sample_query, fetch_all=True) # 回傳 list[dict]
        
        if not sample_data:
            # 如果是空表，嘗試取得欄位結構
            logger.warning(f"表格 {table_id} 是空的，無法取得欄位樣板。")
            sample_data = [{}] 
        
        # 2. 顯示現有配置
        available_cols = list(sample_data[0].keys()) if sample_data[0] else []
        print(f"🔹 目前配置的比對欄位: {current_sort_keys}")
        print(f"🔸 該資料表所有可用欄位: {available_cols}")

        # 3. 詢問使用者是否更新
        user_choice = input(f"是否要更新 <{file_name}> 的比對欄位 (sort_keys)? (y/N): ").strip().lower()
        if user_choice == 'y':
            _, new_sort_keys = _interactive_sort_data(sample_data)
            
            # 檢查新舊 sort_keys 是否真的不同
            if new_sort_keys and new_sort_keys != current_sort_keys:
                print(f"✅ 已檢測到變更，更新配置: {new_sort_keys}")
                config["sort_keys"] = new_sort_keys
                current_sort_keys = new_sort_keys
                is_modified = True  # 標記需要存檔
            else:
                print("ℹ️ 配置未變動，跳過更新。")
        # 4. 後續建立索引邏輯
        comparison_columns = [item[0] for item in current_sort_keys if isinstance(item, list) and item[0]]
        
        if comparison_columns:
            index_name = f"idx_{table_id}_match"
            cols_str = ", ".join([f'"{col}"' for col in comparison_columns])
            sql = f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_id}" ({cols_str});'
            _execute_sql(sql)
            _execute_sql(f'ANALYZE "{table_id}";')
            logger.success(f"✅ <{file_name}> 索引已就緒。")

    # 最終階段：根據標記決定是否存檔
    if is_modified:
        logger.warning("檢測到配置變更，正在同步檔案與資料庫...")
        save_minor_info(minor_info)
        save_minor_info_to_sql(minor_info)
        logger.info("✅ 配置已持久化儲存。")
    else:
        logger.info("ℹ️ 無任何配置變更，無需存檔。")

    print("--- 資料庫索引維護作業完成 ---")

if __name__ == "__main__":
    # 獨立執行維護指令
    create_indexes_for_all_tables()