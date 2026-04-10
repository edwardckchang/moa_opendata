import psycopg2
from psycopg2 import sql
import json # 導入 json 模組
from logs_handle import logger
from typing import List, Dict, Optional
import os
from datetime import datetime, timezone # 導入 datetime 和 timezone
from utils import parse_date_string
current_base_dir = os.path.dirname(os.path.abspath(__file__))
from tqdm import tqdm # 導入 tqdm
from menu_utils import yes_no_menu
import pandas as pd
from utils import Checkpoint

DB = None

GLOBAL_METADATA_CACHE = None # 聲明一個模組級別變數來儲存 metadata

Application_Area = {
'安全飲食': "101",
'地理圖資': "102",
'農業旅遊': "103",
'農糧': "104",
'漁業': "105",
'畜牧': "106",
'農民輔導': "107",
'農業金融': "108",
'動植物防疫檢疫': "109",
'水土保持': "110",
'農村再生': "111",
'造林生產': "112",
'森林經營': "113",
'農業科技': "114",
'主計': "115",
'農業法規': "116",
'農田水利': "117",
'其他': "118",
'農業氣象': "119",
'動物保護': "120"
}

def get_minor_info_data():
    refer_skip_value = []
    all_sort_configs = []
    if not _table_exists("refer_skip_value"):
        refer_skip_value = []
    else:
        sql_query = "SELECT * FROM refer_skip_value;"
        refer_skip_value = _execute_sql(sql_query, fetch_all=True)
    if not _table_exists("all_sort_configs"):
        all_sort_configs = []
    else:
        sql_query = "SELECT * FROM all_sort_configs;"
        all_sort_configs = _execute_sql(sql_query, fetch_all=True)
        for d in all_sort_configs:
            # 將 keys 轉為 list 才能取得第二個 key 的名稱
            keys = list(d.keys())
            if len(keys) > 1:
                target_key = keys[1]
                raw_value = d[target_key]
                
                # 確保它是字串才進行解析
                if isinstance(raw_value, str):
                    try:
                        d[target_key] = json.loads(raw_value)
                    except (json.JSONDecodeError, TypeError):
                        # 如果不是 JSON 格式就跳過
                        pass
    if not _table_exists("all_merge_configs"):
        all_merge_configs = []
    else:
        sql_query = "SELECT * FROM all_merge_configs;"
        all_merge_configs = _execute_sql(sql_query, fetch_all=True)
        for d in all_merge_configs:
            raw_value = d.get("merge_keys")
            if isinstance(raw_value, str):
                try:
                    d["merge_keys"] = json.loads(raw_value)
                except (json.JSONDecodeError, TypeError):
                    pass

    # [整合內容] 修改回傳字典
    with Checkpoint("資料與型別") as cpt:
        if cpt:
            cpt.show("all_merge_configs", all_merge_configs)
    return {
        "refer_skip_value": refer_skip_value, 
        "all_sort_configs": all_sort_configs, 
        "all_merge_configs": all_merge_configs
    }
    

def _generate_global_data():
    """
    這個函數用來生成 GLOBAL_METADATA_CACHE 的值。(字典)
    """
    if not _table_exists("metadata_index"):
        return {}
    logger.notice("INFO: 正在從資料庫生成 metadata...")
    sql_query = "SELECT * FROM metadata_index;"
    all_metadata_records = _execute_sql(sql_query, fetch_all=True)
    if all_metadata_records:
        for record in all_metadata_records:
            table_name = record.get('category_table_id')
            if table_name and _table_exists(table_name):
                count_sql = f"SELECT COUNT(*) as total_count FROM \"{table_name}\";"
                count_res = _execute_sql(count_sql, fetch_one=True)
                record['資料筆數'] = count_res[0] if count_res else 0
            else:
                record['資料筆數'] = 0
        metadata_dict = {record['category_table_id']: record for record in all_metadata_records}
        logger.notice(f"INFO: 已生成 {len(metadata_dict)} 條 metadata 記錄。")
        return metadata_dict
    else:
        logger.warning("WARNING: 未能從資料庫生成任何 metadata 記錄。")
        return {}

def get_global_data():
    """
    初始化或更新模組內部的 GLOBAL_METADATA_CACHE，
    並提供一個公共接口來獲取 GLOBAL_METADATA_CACHE 的值。
    """
    global GLOBAL_METADATA_CACHE
    if GLOBAL_METADATA_CACHE is None:
        GLOBAL_METADATA_CACHE = _generate_global_data()
        logger.notice("INFO: GLOBAL_METADATA_CACHE 初始化或更新完成。")
    return GLOBAL_METADATA_CACHE

def _ensure_db_connection():
    """
    檢查資料庫連接是否建立。如果沒有，則打印錯誤並返回 False。
    """
    if DB is None:
        logger.error("錯誤：未連接到資料庫.")
        return False
    return True

def _table_exists(table_name: str) -> bool:
    """
    檢查指定表格是否存在於資料庫中。
    """
    if not _ensure_db_connection():
        return False
    cur = DB.cursor()
    try:
        table_exists_sql = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = '{table_name}'
        );
        """
        cur.execute(table_exists_sql)
        return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"檢查表格 '{table_name}' 是否存在失敗: {e}")
        return False
    finally:
        cur.close()

def _get_all_tables() -> List[str]:
    """
    獲取資料庫中所有表格的名稱。
    """
    if not _ensure_db_connection():
        return []
    cur = DB.cursor()
    try:
        sql_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
        """
        cur.execute(sql_query)
        # fetchall() returns a list of tuples, get the first element of each tuple
        tables = [row[0] for row in cur.fetchall()]
        return tables
    except Exception as e:
        logger.error(f"獲取所有表格名稱失敗: {e}")
        return []
    finally:
        cur.close()

def _execute_sql(sql_query: str, params: tuple = None, fetch_one: bool = False, fetch_all: bool = False):
    """
    執行 SQL 查詢，並可選擇返回單條或所有結果。
    自動處理游標關閉和事務提交/回滾。
    """
    if not _ensure_db_connection():
        return None

    cur = DB.cursor()
    try:
        logger.debug(f"執行 SQL: {sql_query}\nParams: {params}")
        cur.execute(sql_query, params)
        
        result = None
        if fetch_one:
            result = cur.fetchone()
        elif fetch_all:
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            # 對於非查詢操作（如不帶 RETURNING 的 INSERT/UPDATE/DELETE），返回 True 表示成功
            result = True 
        
        # 關鍵修改：在執行成功後提交事務
        # 這會確保所有 DML 操作（包括帶 RETURNING 的）都能持久化
        DB.commit() 
        
        return result

    except Exception as e:
        DB.rollback() # 發生錯誤時回滾事務
        logger.error(f"執行 SQL 失敗: {type(e).__name__}: {e}\nSQL: {sql_query}\nParams: {params}")
        return False # 查詢或操作失敗
    finally:
        cur.close()

def connect_db(username, password, dbname, host="localhost", port=5432):
    global DB
    if DB is None:
        try:
            conn_str = f"host={host} port={port} dbname={dbname} user={username} password={password}"
            DB = psycopg2.connect(conn_str)
            if not DB:
                logger.error("資料庫連接失敗，程式終止。")
                return None
            logger.notice(f"成功連線到PostgreSQL資料庫: {dbname}@{host}:{port}")            
            return DB
        except Exception as e:
            logger.error(f"無法連接到資料庫 {dbname}@{host}:{port}: {e}")
            DB = None
    return None

def get_related_files(
    record_id: str,
    file_type_filter: Optional[str] = None
) -> List[Dict]:
    """
    查詢指定主記錄 ID 所關聯的所有檔案資訊。

    參數:
    record_id (str): 主記錄的唯一識別 ID (可以是 metadata_id, raw_record_id, processed_record_id)。
    file_type_filter (Optional[str]): 可選參數，用於篩選特定檔案類型 (例如 'pdf', 'png')。
                                      如果為 None，則返回所有類型。

    返回:
    List[Dict]: 包含相關檔案資訊的字典列表 (每個字典代表一個檔案，包含 file_id, file_path, file_type 等)。
                如果沒有找到檔案，則返回空列表。
    """
    # SQL 查詢基礎部分
    sql_query = """
        SELECT
            f.file_id,
            f.file_path,
            f.file_name,
            f.file_type,
            f.file_size,
            f.upload_date
        FROM
            files AS f
        JOIN
            record_files AS rf ON f.file_id = rf.file_id
        WHERE
            rf.record_id = %s
    """
    query_params = [record_id]

    # 如果有檔案類型過濾條件，則增加 WHERE 子句
    if file_type_filter:
        sql_query += " AND f.file_type = %s"
        query_params.append(file_type_filter)

    # 使用 _execute_sql 函數執行查詢並獲取所有結果
    rows = _execute_sql(sql_query, tuple(query_params), fetch_all=True)

    if rows is None or rows is False: # _execute_sql 返回 False 表示失敗，None 表示連接問題
        return [] # 查詢失敗，返回空列表
    return rows

def _get_id_from_query_results(results: List[Dict], id_prefix: str, table_name: str) -> str:
    """
    從查詢結果中獲取 ID。
    對於資料集或檔案的 ID，尋找「可用前的最大 ID」，以便外部邏輯可以基於此生成下一個可用 ID。
    對於資料的 ID，返回最大 ID。
    """
    if not results:
        if table_name != "files":
            if len(id_prefix) == 3:
                id_start = id_prefix + "0000"
            elif len(id_prefix) == 7:
                id_start = id_prefix + "0000000"
        elif table_name == "files":
            id_start = "0"
        else:
            logger.error(f"{id_prefix} 格式錯誤。")
            return None
        logger.notice(f"在表格 '{table_name}' 中未找到任何 {id_prefix} 系列id，從 {id_start} 開始。")
        return id_start
    # 尋找含有 "id" 的鍵
    id_key = None
    if table_name == "files":
        id_key = "file_id"
    elif table_name == "metadata_index":
        id_key = "category_table_id"
    else: # For other data tables
        id_key = "category_table_data_id"

    if table_name == "files":
        id_num_list = []
        next_numeric_id = 1 # 初始化 next_numeric_id
        logger.debug(f"_get_id_from_query_results - files: 接收到的 results: {results}, id_prefix: {id_prefix}, table_name: {table_name}")
        for row in results:
            id_str = str(row.get(id_key))
            logger.debug(f"_get_id_from_query_results - files: 處理 row: {row}, id_str: {id_str}")
            if row and id_str.startswith(id_prefix):
                id_num = id_str.replace(f"{id_prefix}file","")
                logger.debug(f"_get_id_from_query_results - files: 提取 id_num: {id_num}")
                if id_num.isdigit():
                    id_num_list.append(int(id_num))
                    logger.debug(f"_get_id_from_query_results - files: id_num_list 目前: {id_num_list}")
        sorted_numeric_ids = sorted(id_num_list)
        logger.debug(f"_get_id_from_query_results - files: sorted_numeric_ids: {sorted_numeric_ids}")
        if not sorted_numeric_ids:
            logger.notice(f"在表格 '{table_name}' 中未找到任何 {id_prefix} 系列id，從 0 開始。")
            return "0"
        for current_numeric_id in sorted_numeric_ids:
            logger.debug(f"_get_id_from_query_results - files: 檢查 current_numeric_id: {current_numeric_id}, next_numeric_id: {next_numeric_id}")
            if current_numeric_id == next_numeric_id:
                next_numeric_id += 1
            elif current_numeric_id > next_numeric_id:
                # 找到空缺，此時 next_numeric_id 即為第一個空缺值
                logger.debug(f"_get_id_from_query_results - files: 找到空缺，next_numeric_id: {next_numeric_id}")
                break
        logger.debug(f"_get_id_from_query_results - files: 返回 next_numeric_id - 1: {next_numeric_id - 1}")
        return f"{next_numeric_id - 1}" # 返回計算出的「可用前的最大使用中ID」，格式為字串。
    elif table_name == "metadata_index" and len(id_prefix) == 3:
        # 尋找空缺 ID 的邏輯
        existing_numeric_ids = set()
        for row in results:
            try:
                full_id_str = str(row.get(id_key))
                if full_id_str.startswith(id_prefix) and len(full_id_str) == 7 and full_id_str.isdigit():
                    existing_numeric_ids.add(int(full_id_str))
                else:
                    logger.warning(f"警告: ID '{full_id_str}' 不符合預期的前綴 '{id_prefix}'，跳過。")
            except (ValueError, TypeError) as e:
                logger.warning(f"警告: 無法將 ID '{row.get(id_key)}' 轉換為數字失敗，跳過。錯誤: {e}")
                continue

        sorted_numeric_ids = sorted(list(existing_numeric_ids))
        if not sorted_numeric_ids:
            # 如果沒有現有 ID，返回起始 ID (例如 1040000)
            return f"{id_prefix}0000"
        next_numeric_id = int(str(id_prefix) + "0001") # 此變數用於追蹤在尋找空缺 ID 過程中，「下一個預期的 ID 值」，其最終值將用於計算「可用前的最大 ID」。
        for current_numeric_id in sorted_numeric_ids:
            if current_numeric_id == next_numeric_id:
                next_numeric_id += 1
            elif current_numeric_id > next_numeric_id:
                # 找到空缺，此時 next_numeric_id 即為第一個空缺值
                break
        return f"{next_numeric_id - 1}" # 返回計算出的「可用前的最大 ID」，格式為字串。
    else:
        # 獲取最大 ID 的原有邏輯
        max_id = 0
        for row in results:
            try:
                current_id = int(row.get(id_key))
                if current_id > max_id:
                    max_id = current_id
            except (ValueError, TypeError) as e:
                logger.warning(f"警告: 無法將 ID '{row.get(id_key)}' 轉換為數字，跳過。錯誤: {e}")
                continue
        return f"{max_id}"
    
def create_empty_table_unexistent(metadata_schema: dict, table_name: str) -> None:
    """
    如果表格不存在，建立 空白 表格,用於儲存所有資料集的 metadata.
    """
    if not _ensure_db_connection():
        return

    try:
        if not _table_exists(table_name):
            logger.notice(f"表格 '{table_name}' 不存在，正在建立...")
            columns_sql = table_columns_sql(metadata_schema)
            
            primary_key_sql = ""
            foreign_key_sql = "" # 初始化 foreign_key_sql
            if table_name == "metadata_index":
                primary_key_sql = ",\n        PRIMARY KEY (category_table_id)"
            elif table_name in ("refer_skip_value", "all_sort_configs", "all_merge_configs"):
                primary_key_sql = ""
                foreign_key_sql = f",\n        category_table_id VARCHAR(255) NOT NULL UNIQUE REFERENCES metadata_index(category_table_id) ON DELETE CASCADE"
            else:
                primary_key_sql = ",\n        PRIMARY KEY (category_table_data_id)"
                
                # 根據 table_name 是否包含 '_processed' 來動態生成 REFERENCES 的目標表格名稱
                if table_name.endswith('_processed'):
                    # 如果是 _processed 表格，則 REFERENCES 目標是原始表格的 category_table_data_id
                    original_table_name = table_name.replace('_processed', '')
                    foreign_key_sql = f",\n        foreign_key VARCHAR(255) REFERENCES \"{original_table_name}\"(category_table_data_id) ON DELETE CASCADE"
                else:
                    # 如果不是 _processed 表格，則 REFERENCES 目標是 metadata_index 的 category_table_id
                    foreign_key_sql = ",\n        foreign_key VARCHAR(255) REFERENCES metadata_index(category_table_id) ON DELETE CASCADE"
            timestmp = f""",created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()"""
            if table_name == "refer_skip_value" or "all_sort_configs":
                timestmp = ""
            create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns_sql}{timestmp}{primary_key_sql}{foreign_key_sql}
    );
    """
            if _execute_sql(create_table_sql):
                logger.notice(f"表格 '{table_name}' 已建立。")
                # 再次檢查表格是否存在，以確保提交成功
                if not _table_exists(table_name):
                    logger.warning(f"警告：表格 '{table_name}' 報告已建立，但實際檢查後不存在。可能存在事務提交問題。")
            else:
                logger.error(f"建立表格 '{table_name}' 失敗，_execute_sql 返回 False。")
        else:
            logger.notice(f"表格 '{table_name}' 已存在，跳過建立表格。")
    except Exception as e:
        logger.error(f"建立表格 '{table_name}' 失敗: {e}")

def insert_or_update_metadata(metadata: dict):
    """
    將單一資料集的 metadata 插入或更新到 metadata_index 表格中.
    """
    if not _ensure_db_connection():
        return None
    try:
        category_table_id = metadata.get("category_table_id")
        if not category_table_id:
            logger.error(f"metadata 字典中缺少 'category_table_id' 欄位，無法插入或更新 metadata。")
            metadata["category_table_id"] = -1
            return None

        # 檢查是否已存在相同 category_table_id 的 metadata
        excluded_keys = ["created_at", "last_updated_at"]
        columns = []
        placeholders = []
        values = []
        update_set_clauses = []

        for key, value in metadata.items():
            if key == "category_table_id":
                continue
            if key not in excluded_keys:
                columns.append(f'"{key}"')
                placeholders.append('%s')
                update_set_clauses.append(f'"{key}" = EXCLUDED."{key}"')
                values.append(value)

        raw_col_names = [col.strip('"') for col in columns]
        _ensure_columns_exist("metadata_index", raw_col_names)

        upsert_sql = f"""
        INSERT INTO metadata_index (
            "category_table_id", {', '.join(columns)}, created_at, last_updated_at
        ) VALUES (
            %s, {', '.join(placeholders)}, NOW(), NOW()
        )
        ON CONFLICT (category_table_id) DO UPDATE SET
            {', '.join(update_set_clauses)},
            last_updated_at = NOW();
        """
        
        upsert_values = [category_table_id] + values
        if _execute_sql(upsert_sql, tuple(upsert_values)):
            existing_record = GLOBAL_METADATA_CACHE.get(category_table_id, {})
            existing_record.update(metadata)
            existing_record['last_updated_at'] = datetime.now(timezone.utc)
            GLOBAL_METADATA_CACHE[category_table_id] = existing_record
            logger.notice(f"Metadata (category_table_id: {category_table_id}) 已成功插入或更新到 metadata_index 表格和快取.")
            success_metadata_index = True
        else:
            logger.error(f"Metadata (category_table_id: {category_table_id}) 插入或更新到 metadata_index 表格失敗.")
            return False
        selected_pdf_path = metadata.get("介接說明文件")
        success_file_entry = True
        if selected_pdf_path:
            file_id = create_file_entry_with_upsert(selected_pdf_path, category_table_id, "readme, PDF")
            file_status = insert_record_file_entry(category_table_id, file_id)
            if file_id and file_status == True:
                logger.info(f"已關連 '{metadata.get('標題')}' 的介接說明文件，檔案 ID: {file_id}")
            elif file_status == None:
                pass
            else:
                success_file_entry = False
                logger.error(f"關連 '{metadata.get('標題')}' 的介接說明文件失敗。")                
            if success_metadata_index and success_file_entry:
                return True
            else:
                return False
        if success_metadata_index:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"插入或更新 metadata 到 metadata_index 表格失敗: {e}")

def _insert_or_update_minor_info(info: list[dict], table_name: str):
    """
    將 minor_info 存入 PostgreSQL。
    若 category_table_id 衝突，則執行 DO UPDATE SET 更新所有欄位。
    """
    # 區塊 1 & 2：連線檢查與前處理
    if not _ensure_db_connection():
        logger.error("資料庫未連線。")
        return None

    if not info:
        logger.warning("沒有有效次要資訊可插入表格。")
        return None

    processed_data = []
    for item in info:
        processed_item = {}
        for k, v in item.items():
            if isinstance(v, (list, dict)):
                processed_item[k] = json.dumps(v, ensure_ascii=False)
            else:
                processed_item[k] = v
        processed_data.append(processed_item)

    # 區塊 3：產生 SQL 欄位與子句
    # 確保資料庫表格有資料中的所有欄位，若缺少則自動新增
    all_data_cols = list(processed_data[0].keys())
    _ensure_columns_exist(table_name, all_data_cols)

    # 依照你的要求，直接使用 keys() 順序，不特別移動 category_table_id
    first_record = processed_data[0]
    columns = list(first_record.keys())
    columns_for_sql = [f'"{col}"' for col in columns]
    placeholders = ", ".join(["%s"] * len(columns))

    # update_set_clauses 包含所有欄位，符合 insert_or_update_metadata 的風格
    update_set_clauses = [f'"{col}" = EXCLUDED."{col}"' for col in columns]
    update_set_sql = ", ".join(update_set_clauses)

    # 區塊 4：產生完整的 upsert_sql
    upsert_sql = f"""
    INSERT INTO "{table_name}" ({", ".join(columns_for_sql)})
    VALUES ({placeholders})
    ON CONFLICT ("category_table_id") DO UPDATE SET
        {update_set_sql};
    """

    # 區塊 5：執行迴圈與回傳邏輯
    if processed_data:
        logger.notice(f"開始批量 UPSERT 到表格 '{table_name}'，共 {len(processed_data)} 條記錄...")
        for record_dict in processed_data:
            # 依據 columns_for_sql 的順序提取值
            values = [record_dict[col.strip('"')] for col in columns_for_sql]
            
            if not _execute_sql(upsert_sql, tuple(values)):
                logger.error(f"Upsert 單筆記錄到 {table_name} 失敗，ID: {record_dict.get('category_table_id')}")
                # 依據你的風格，單筆失敗通常紀錄 log，整體執行完後回傳 True
        
        logger.notice(f"已成功完成 '{table_name}' 的批量插入/更新作業。")
        return True
    else:
        logger.warning(f"沒有有效資料可插入表格 '{table_name}'。")
        return None

def save_minor_info_to_sql(minor_info: dict):
    def _save_info(info: list, table_name: str, metadata: dict):
        info_update = []
        for category_table_id, value in metadata.items():
            title = value.get("標題")
            if not title:
                continue
            for value_dict in info:
                if value_dict.get("標題") == title or value_dict.get("file_name") == title:  
                    value_dict["category_table_id"] = category_table_id
                    info_update.append(value_dict)
                    continue
        if not info_update:
            return None
        data_schema = {k: "text" for k in info_update[0].keys()}
        create_empty_table_unexistent(data_schema, table_name)
        if _insert_or_update_minor_info(info_update, table_name):
            return True
    if not _ensure_db_connection():
        return None
    try:
        if not minor_info:
            return None
        metadata = get_global_data()
        refer_skip_value = minor_info.get("refer_skip_value") or []
        all_sort_configs = minor_info.get("all_sort_configs") or []
        all_merge_configs = minor_info.get("all_merge_configs") or []
        with Checkpoint("資料檢查") as cpt:
            if cpt:
                cpt.show("all_merge_configs", all_merge_configs)
        if refer_skip_value:
            if not _save_info(refer_skip_value, "refer_skip_value", metadata):
                logger.error(f"儲存 'refer_skip_value' 失敗。")
                return None

        if all_sort_configs:
            if not _save_info(all_sort_configs, "all_sort_configs", metadata):
                logger.error(f"儲存 'all_sort_configs' 失敗。")
                return None
        if all_merge_configs:
            if not _save_info(all_merge_configs, "all_merge_configs", metadata):
                logger.error(f"儲存 'all_merge_configs' 失敗。")
                return None
        return True
    except Exception as e:
        logger.error(f"儲存 'minor_info' 失敗: {e}")
                            


def save_dataframe_to_postgresql(data: list, table_name: str, title: str):
    """
    將資料 (字典列表) 存入 PostgreSQL 資料庫的指定表格。

    Args:
        data (list): 要儲存的資料 (字典列表)。
        table_name (str): 目標表格名稱 (例如 "raw_data_geo_001")。
    """
    if not _ensure_db_connection():
        return None

    try:
        if not data:
            logger.warning(f"沒有 '{title}' 的資料可存入表格 '{table_name}'.")
            return None
        if "代碼" in title or "空間" in title:
            if _table_exists(table_name):
                if delete_all_data_from_table(table_name):
                    logger.notice(f"清理 '{title}' 的資料表格 '{table_name}' 資料清空成功。")
                else:
                    logger.error(f"清理 '{title}' 的資料表格 '{table_name}' 資料清空失敗。")
                    return None

        # 處理資料中的 None 和 JSON 類型
        processed_data = []
        for item in data:
            processed_item = {}
            for k, v in item.items():
                if v is None:
                    processed_item[k] = None
                elif isinstance(v, (list, dict)):
                    processed_item[k] = json.dumps(v, ensure_ascii=False)
                else:
                    processed_item[k] = v
            processed_data.append(processed_item)

        # 獲取所有欄位名稱
        columns_for_sql = [f'"{col.replace(" ", "_").replace(".", "_").replace("-", "_")}"' for col in processed_data[0].keys()]
        return _insert_records_to_postgresql(processed_data, table_name, columns_for_sql)

    except Exception as e:
        logger.error(f"儲存資料到 PostgreSQL 表格 '{table_name}' 失敗: {e}")

def _insert_records_to_postgresql(data: list, table_name: str, columns_for_sql: list):
    """
    將字典列表中的記錄插入到 PostgreSQL 資料庫的指定表格。
    """
    if not _ensure_db_connection():
        return None

    if not data:
        logger.warning(f"沒有資料可插入表格 '{table_name}'.")
        return None

    try:
        raw_col_names = [col.strip('"') for col in columns_for_sql]
        _ensure_columns_exist(table_name, raw_col_names)
        placeholders = ", ".join(['%s'] * len(columns_for_sql))
        insert_sql = f"INSERT INTO \"{table_name}\" ({', '.join(columns_for_sql)}) VALUES ({placeholders});"
        
        if data:
            logger.info(f"批量插入表格 '{table_name}' 中，總計 {len(data)} 條記錄...")
            for record_dict in tqdm(data):
                values = [record_dict[col.strip('"')] for col in columns_for_sql]
                if not _execute_sql(insert_sql, tuple(values)):
                    logger.error(f"插入記錄到表格 '{table_name}' 失敗。")
            logger.notice(f"已成功批量插入總計 {len(data)} 條記錄到表格 '{table_name}' 。")
            return True
        else:
            logger.warning(f"沒有資料可插入表格 '{table_name}'.")
            return False
    except Exception as e:
        logger.error(f"插入資料到 PostgreSQL 表格 '{table_name}' 失敗: {e}")

def check_metadata_update_status(metadata: dict) -> tuple[bool, str]:
    """
    檢查 metadata 的更新狀態，判斷是否需要更新資料。
    Args:
        metadata (dict): 從網頁解析出的 metadata 字典。
    Returns:
        tuple[bool, str]: 第一個元素為布林值，表示是否需要更新 (True 為需要，False 為不需要)。
                          第二個元素為字串，如果需要新增，則為計算出的 category_table_id；
                          如果不需要新增或更新，則為空字串。
    """
    if not _ensure_db_connection():
        return False, ""

    page_title = metadata.get("標題")
    web_update_date_str = metadata.get("資料更新日期")
    data_category = metadata.get("資料分類")
    category_id_prefix = Application_Area.get(data_category)

    if not all([page_title, web_update_date_str, data_category, category_id_prefix]):
        logger.error(f"元資料缺乏關鍵資訊，無法檢查更新狀態。")
        return False, ""

    try:
        db_update_date_result = _execute_sql("SELECT \"資料更新日期\", category_table_id FROM metadata_index WHERE \"標題\" = %s;", (page_title,), fetch_one=True)

        if db_update_date_result is not None:
            db_update_date_from_db = db_update_date_result[0]
            db_category_table_id = db_update_date_result[1]

            if str(db_update_date_from_db) == web_update_date_str:
                logger.notice(f"資料集 '{page_title}' 的更新日期與資料庫中相同 ({web_update_date_str})，無需更新。")
                return False, str(db_category_table_id)
            else:
                logger.notice(f"資料集 '{page_title}' 的更新日期已更新 (舊: {db_update_date_from_db}, 新: {web_update_date_str})，需要更新。")
                return True, str(db_category_table_id)
        else:
            logger.notice(f"資料集 '{page_title}' 在資料庫中不存在，將進行新增。")
            next_category_table_id = get_next_available_category_table_id(data_category, category_id_prefix)
            return True, str(int(next_category_table_id) + 1) # 外部引用時會+1

    except Exception as e:
        logger.error(f"檢查資料集 '{page_title}' 的 metadata 更新狀態失敗: {e}")
        return False, ""

def delete_table_and_metadata_entry(category_table_id: str, delete_table: bool = True) -> bool:
    """
    執行資料集（資料表格及其 metadata 記錄）的實際刪除操作。
    Args:
        category_table_id (str): 要刪除的資料集的 category_table_id。
        delete_table (bool): 是否刪除資料表格本身。預設為 True。
    Returns:
        bool: 如果刪除成功則返回 True，否則返回 False。
    """
    if not _ensure_db_connection():
        logger.error("無法連接到資料庫，請檢查配置。")
        return False

    try:
        # Get initial list of tables
        initial_tables = _get_all_tables()
        if not initial_tables and _ensure_db_connection():
             logger.warning("警告：無法獲取初始表格列表，可能影響後續檢查。")

        # 判斷要刪除的是原始表格還是處理後表格
        is_processed_table_id = category_table_id.endswith('_processed')
        
        original_table_name = category_table_id.replace('_processed', '') if is_processed_table_id else category_table_id
        processed_table_name = f"{original_table_name}_processed"
        table_deleted = True
        if delete_table:
            if is_processed_table_id:
                # 如果是處理後表格，只刪除處理後表格
                if _table_exists(processed_table_name):
                    drop_processed_table_sql = f'DROP TABLE IF EXISTS "{processed_table_name}" CASCADE;'
                    if _execute_sql(drop_processed_table_sql):
                        logger.notice(f"處理後資料表格 '{processed_table_name}' 已從 PostgreSQL 資料庫中刪除。")

                    else:
                        logger.error(f"刪除處理後資料表格 '{processed_table_name}' 失敗。")
                        return False
                else:
                    logger.warning(f"警告：處理後表格 '{processed_table_name}' 不存在，跳過刪除。")
            else:
                # 如果是原始表格，刪除原始表格和處理後表格
                if _table_exists(original_table_name):
                    drop_table_sql = f'DROP TABLE IF EXISTS "{original_table_name}" CASCADE;'
                    if _execute_sql(drop_table_sql):
                        logger.notice(f"原始資料表格 '{original_table_name}' 已從 PostgreSQL 資料庫中刪除。")
                    else:
                        logger.error(f"刪除原始資料表格 '{original_table_name}' 失敗。")
                        return False
                else:
                    logger.warning(f"警告：原始表格 '{original_table_name}' 不存在，跳過刪除。")
                current_tables = _get_all_tables() #檢查是否CASCADE
                if original_table_name not in current_tables and processed_table_name not in current_tables:
                    logger.notice(f"原始表格 '{original_table_name}' 和處理後表格 '{processed_table_name}' 都已透過 CASCADE 刪除，跳過再次刪除處理後表格。")
                else:
                    if _table_exists(processed_table_name):
                        drop_processed_table_sql = f'DROP TABLE IF EXISTS "{processed_table_name}" CASCADE;'
                        if _execute_sql(drop_processed_table_sql):
                            logger.notice(f"處理後資料表格 '{processed_table_name}' 已從 PostgreSQL 資料庫中刪除。")
                        else:
                            logger.error(f"刪除處理後資料表格 '{processed_table_name}' 失敗。")
                            table_deleted = False
                    else:
                        logger.warning(f"警告：處理後表格 '{processed_table_name}' 不存在，跳過刪除。")
            # 刪除處理後資料表格的邏輯結束
        # 從 metadata_index 表格中刪除對應的列
        # 僅在刪除原始表格時才刪除 metadata 記錄
        delete_metadata_sql = f'DELETE FROM "metadata_index" WHERE category_table_id = %s;'
        if _execute_sql(delete_metadata_sql, (original_table_name,)):
            if original_table_name in GLOBAL_METADATA_CACHE:
                del GLOBAL_METADATA_CACHE[original_table_name]
                logger.notice(f"資料集 (category_table_id: {original_table_name}) 的 metadata 記錄已從 'metadata_index' 和快取中刪除。")
            else:
                logger.notice(f"資料集 (category_table_id: {original_table_name}) 的 metadata 記錄已從 'metadata_index' 中刪除，但在快取中未找到。")
        else:
            logger.error(f"從 'metadata_index' 中刪除資料集 (category_table_id: {original_table_name}) 的 metadata 記錄失敗。")
            return False
        return True if table_deleted else False
    except Exception as e:
        logger.error(f"刪除資料集 (category_table_id: {category_table_id}) 時發生錯誤: {e}")
        return False
    
def rename_data_tables(old_category_table_id: str, new_category_table_id: str) -> bool:
    """
    重新命名資料庫中的原始表格及其對應的 _processed 表格，並更新 metadata_index 中的 category_table_id。
    Args:
        old_category_table_id (str): 舊的 category_table_id。
        new_category_table_id (str): 新的 category_table_id。
    Returns:
        bool: 如果重新命名成功則返回 True，否則返回 False。
    """
    if not _ensure_db_connection():
        return False

    success = True

    # 檢查新的 category_table_id 是否已存在於 metadata_index 中
    existing_new_id = _execute_sql("SELECT category_table_id FROM metadata_index WHERE category_table_id = %s;", (new_category_table_id,), fetch_one=True)
    if existing_new_id:
        logger.error(f"錯誤：新的 category_table_id '{new_category_table_id}' 已存在於 metadata_index 中，無法重新命名。")
        return False

    old_original_table_name = old_category_table_id
    new_original_table_name = new_category_table_id

    # 1. 重新命名原始表格
    if _table_exists(old_original_table_name):
        try:
            rename_sql = f'ALTER TABLE "{old_original_table_name}" RENAME TO "{new_original_table_name}";'
            if _execute_sql(rename_sql):
                logger.notice(f"原始表格 '{old_original_table_name}' 已成功重新命名為 '{new_original_table_name}'。")
            else:
                logger.error(f"重新命名原始表格 '{old_original_table_name}' 到 '{new_original_table_name}' 失敗。")
                success = False
        except Exception as e:
            logger.error(f"重新命名原始表格 '{old_original_table_name}' 時發生錯誤: {e}")
            success = False
    else:
        logger.warning(f"警告：原始表格 '{old_original_table_name}' 不存在，跳過重新命名。")

    # 2. 重新命名 _processed 表格
    old_processed_table_name = f"{old_original_table_name}_processed"
    new_processed_table_name = f"{new_original_table_name}_processed"
    if _table_exists(old_processed_table_name):
        try:
            rename_processed_sql = f'ALTER TABLE "{old_processed_table_name}" RENAME TO "{new_processed_table_name}";'
            if _execute_sql(rename_processed_sql):
                logger.notice(f"處理後表格 '{old_processed_table_name}' 已成功重新命名為 '{new_processed_table_name}'。")
            else:
                logger.error(f"重新命名處理後表格 '{old_processed_table_name}' 到 '{new_processed_table_name}' 失敗。")
                success = False
        except Exception as e:
            logger.error(f"重新命名處理後表格 '{old_processed_table_name}' 時發生錯誤: {e}")
            success = False
    else:
        logger.warning(f"警告：處理後表格 '{old_processed_table_name}' 不存在，跳過重新命名。")

    # 3. 更新 metadata_index 表格
    try:
        update_metadata_sql = f"""
            UPDATE "metadata_index"
            SET category_table_id = %s
            WHERE category_table_id = %s;
        """
        if _execute_sql(update_metadata_sql, (new_category_table_id, old_category_table_id)):
            if old_category_table_id in GLOBAL_METADATA_CACHE:
                # 獲取舊的記錄，更新鍵，然後刪除舊鍵
                record = GLOBAL_METADATA_CACHE.pop(old_category_table_id)
                record['category_table_id'] = new_category_table_id # 更新記錄中的 ID
                GLOBAL_METADATA_CACHE[new_category_table_id] = record
                logger.notice(f"metadata_index 中 category_table_id '{old_category_table_id}' 已更新為 '{new_category_table_id}'，並同步更新快取。")
            else:
                logger.warning(f"metadata_index 中 category_table_id '{old_category_table_id}' 已更新為 '{new_category_table_id}'，但在快取中未找到舊記錄。")
        else:
            logger.error(f"更新 metadata_index 中 category_table_id 失敗。")
            success = False
    except Exception as e:
        logger.error(f"更新 metadata_index 時發生錯誤: {e}")
        success = False

    return success

def delete_all_data_from_table(table_name: str) -> bool:
    """
    刪除指定資料表格中的所有資料記錄。
    Args:
        table_name (str): 要清空資料的表格名稱。
    Returns:
        bool: 如果刪除成功則返回 True，否則返回 False。
    """
    if not _ensure_db_connection():
        logger.error("無法連接到資料庫，請檢查配置。")
        return False

    try:
        if not _table_exists(table_name):
            logger.warning(f"警告：資料表格 '{table_name}' 不存在，跳過清空資料。")
            return True # 表格不存在，視為成功清空 (因為沒有資料可清空)

        delete_sql = f'DELETE FROM "{table_name}";'
        if _execute_sql(delete_sql):
            logger.notice(f"資料表格 '{table_name}' 中的所有資料記錄已成功刪除。")
            return True
        else:
            logger.error(f"刪除資料表格 '{table_name}' 中的所有資料記錄失敗。")
            return False
    except Exception as e:
        logger.error(f"刪除資料表格 '{table_name}' 中的所有資料記錄時發生錯誤: {e}")
        return False

def check_dataset_content_update_status(title: str, comparison_columns: List[str], current_dataset_data: List[dict], category_table_id_prefix: str, data_filter = True) -> List[dict]:
    """
    檢查資料庫中資料集的內容與當前資料集內容的差異，並返回需要插入的記錄。
    使用指定的欄位組合作為比對鍵。

    Args:
        title (str): 資料庫中的表格主要標題名稱。
        comparison_columns (List[str]): 用於比對的欄位名稱列表，這些欄位組合起來應能唯一識別一條記錄。
                                        例如：["圖檔中文名稱", "分布圖Url"]。
        current_dataset_data (list): 當前從網頁下載的資料集內容 (字典列表)。
        category_table_id_prefix (str): category_table_id  (例如 "1010001")，作為部分場合的前綴，同時，也是表格名。

    Returns:
        List[dict]:需要插入的記錄 (字典列表)。
    """
    if data_filter:
        current_dataset_data = preserve_old_data_by_date(current_dataset_data)
    logger.notice(f"下載資料集 '{title}' 過濾得到 {len(current_dataset_data)} 筆記錄。")
    records_to_insert = []
    existing_record_ids = [] # 新增一個列表來儲存比對鍵相同的記錄 ID
    table_name = category_table_id_prefix
    # 獲取當前最大的 category_table_data_id，並在函式內部維護一個計數器
    max_category_table_data_id = int(get_max_category_table_data_id(table_name, category_table_id_prefix))

    if not _ensure_db_connection():
        logger.error(f"無法建立資料庫連接。")
        return []

    if not current_dataset_data:
        logger.warning(f"資料集 '{title}' 當前資料為空，無需比較。")
        return []

    if not comparison_columns and "代碼" not in title:
        logger.error(f"檢查資料集 '{title}' (表格: '{category_table_id_prefix}') 內容更新狀態時未提供用於比對的欄位名稱，無法進行內容比對。")
        return []

    try:
        if "代碼" not in title:
            # 1. 從資料庫中查詢所有用於比對的欄位值以及 category_table_data_id，並構建資料庫中現有記錄的「複合唯一識別符」集合
            quoted_cols = [f'"{col}"' for col in comparison_columns]
            select_clause = ", ".join(quoted_cols)
            if _table_exists(category_table_id_prefix):
                db_records_raw = _execute_sql(f"SELECT {select_clause}, category_table_data_id FROM \"{category_table_id_prefix}\";", fetch_all=True)
                if data_filter:
                    db_records_raw = preserve_old_data_by_date(db_records_raw)
                logger.notice(f"資料庫資料集 '{title}' 過濾得到 {len(db_records_raw)} 筆記錄。")
            else:
                db_records_raw = []
            db_records_map = {} # 用於快速查找資料庫中現有記錄的字典
            if db_records_raw:
                for record_dict in db_records_raw:
                    identifier_values = []
                    for col_name in comparison_columns:
                        value = record_dict.get(col_name)
                        identifier_values.append(str(value) if value is not None else None)
                    
                    identifier_tuple = tuple(identifier_values)
                    db_records_map[identifier_tuple] = record_dict

        # 2. 遍歷當前資料集，檢查每條記錄是否存在於資料庫中
        # with tqdm(total=len(current_dataset_data)) as pbar:
        for current_item in current_dataset_data:
            current_item_identifier_values = []
            if "代碼" not in title:
                for col_name in comparison_columns:
                    value = current_item.get(col_name)
                    current_item_identifier_values.append(str(value) if value is not None else None)
                
                current_identifier_tuple = tuple(current_item_identifier_values)

                if current_identifier_tuple in db_records_map:
                    # 如果比對鍵相同，則無需處理，將 ID 加入列表
                    db_record = db_records_map[current_identifier_tuple]
                    existing_record_ids.append(db_record.get('category_table_data_id'))
                else:
                    # 如果比對鍵組合不存在，則視為新記錄
                    max_category_table_data_id += 1
                    current_item["category_table_data_id"] = max_category_table_data_id
                    records_to_insert.append(current_item)
            else:
                max_category_table_data_id += 1
                current_item["category_table_data_id"] = max_category_table_data_id
                records_to_insert.append(current_item)
                # pbar.update(1)

        if existing_record_ids:
            logger.notice(f"以下記錄已存在且比對鍵相同，無需更新或插入: {existing_record_ids[:3]}...{existing_record_ids[-3:]} (共 {len(existing_record_ids)} 個)")
        
        logger.notice(f"與資料庫比對完成。需要插入 {len(records_to_insert)} 條記錄。")
        return records_to_insert

    except psycopg2.errors.UndefinedTable:
        logger.error(f"資料表 '{title}' 不存在，無法進行內容比對。請先確保表格已建立。")
        return []
    except Exception as e:
        logger.error(f"檢查資料集 '{title}' 內容更新狀態失敗: {e}")
        return []
        
def get_next_available_category_table_id(data_category: str, category_id_prefix: str) -> str:
    """
    從 metadata_index 表中獲取指定資料分類的最大 category_table_id。
    如果沒有找到，則返回 category_id_prefix + "0000"。
    """
    if not _ensure_db_connection():
        return None

    try:
        sql_query = "SELECT category_table_id FROM metadata_index WHERE \"資料分類\" = %s;"
        results = _execute_sql(sql_query, (data_category,), fetch_all=True)
        return _get_id_from_query_results(results,category_id_prefix,"metadata_index")
    except Exception as e:
        logger.error(f"獲取資料分類 '{data_category}' 的最大 category_table_id 失敗: {e}")
        return None

def get_max_category_table_data_id(table_name: str, category_table_id_prefix: str) -> str:
    """
    從指定表格中獲取 category_table_data_id 的最大值。
    如果沒有找到，則返回 category_table_id_prefix + "0000000" (資料 ID)。
    """
    if not _ensure_db_connection():
        return None

    try:
        if _table_exists(table_name):
            sql_query = f"SELECT category_table_data_id FROM \"{table_name}\";"
            results = _execute_sql(sql_query, fetch_all=True)
        else:
            results= []
        return _get_id_from_query_results(results,category_table_id_prefix,table_name)
    except Exception as e:
        logger.error(f"獲取表格 '{table_name}' 的最大 category_table_data_id 失敗: {e}")
        return None
    
def table_columns_sql(metadata_schema: dict) -> str:
    col_definitions = []
    for col_name, col_type in metadata_schema.items():
        # 跳過 'foreign_key' 欄位，因為它將作為 FOREIGN KEY 約束單獨處理
        if col_name == "foreign_key":
            continue
        pg_type = "TEXT"
        safe_col_name = f'"{col_name.replace(" ", "_").replace(".", "_").replace("-", "_")}"'
        col_definitions.append(f'{safe_col_name} {pg_type}')

    columns_sql = ", ".join(col_definitions)
    return columns_sql

def create_files_table_if_not_exists() -> None:
    """
    建立 'files' 表格，如果它不存在。
    新增：file_path 欄位的 UNIQUE 約束。
    """
    table_name = "files"
    if not _ensure_db_connection():
        return

    try:
        if not _table_exists(table_name):
            logger.notice(f"表格 '{table_name}' 不存在，正在建立...")
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                file_id VARCHAR(255) PRIMARY KEY,       -- 檔案的唯一識別ID (PK)，通常由檔案唯一關聯的資料來源ID所組合
                file_path TEXT NOT NULL UNIQUE,         -- 檔案在儲存系統中的完整路徑，強制唯一
                file_name VARCHAR(255) NOT NULL,        -- 原始檔案名稱
                file_type VARCHAR(50) NOT NULL,         -- 檔案類型 (e.g., 'pdf', 'png')
                file_size TEXT,                         -- 檔案大小 (e.g., "6.60 KB") (可選)
                upload_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- 檔案被記錄的時間 (可選)
                description TEXT                       -- 檔案的簡要描述 (可選)
            );
            -- 即使有了 UNIQUE 約束，單獨的索引對於查詢還是有好處
            CREATE INDEX IF NOT EXISTS idx_files_filetype ON "{table_name}" (file_type);
            """
            if _execute_sql(create_sql):
                logger.notice(f"表格 '{table_name}' 已建立。")
            else:
                logger.error(f"建立表格 '{table_name}' 失敗。")
        else:
            logger.notice(f"表格 '{table_name}' 已存在，跳過建立。")
    except Exception as e:
        logger.error(f"建立表格 '{table_name}' 過程中發生錯誤: {e}")

def create_record_files_table_if_not_exists() -> None:
    """
    建立 'record_files' 表格，如果它不存在。
    這個表用於連結任何主數據記錄與檔案。
    """
    table_name = "record_files"
    if not _ensure_db_connection():
        return

    try:
        if not _table_exists(table_name):
            logger.notice(f"表格 '{table_name}' 不存在，正在建立...")
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                record_id VARCHAR(255) NOT NULL,        -- 關聯的主數據記錄的ID (FK)
                file_id VARCHAR(255) NOT NULL,          -- 關聯的檔案ID (FK)
                
                PRIMARY KEY (record_id, file_id),       -- 複合主鍵，確保唯一性
                
                FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE
                -- 這裡假設 files 表格在 record_files 之前已經存在
            );
            CREATE INDEX IF NOT EXISTS idx_recordfiles_recordid ON "{table_name}" (record_id);
            """
            if _execute_sql(create_sql):
                logger.notice(f"表格 '{table_name}' 已建立。")
            else:
                logger.error(f"建立表格 '{table_name}' 失敗。")
        else:
            logger.notice(f"表格 '{table_name}' 已存在，跳過建立。")
    except Exception as e:
        logger.error(f"建立表格 '{table_name}' 過程中發生錯誤: {e}")

def create_file_entry_with_upsert(file_path: str, file_id_prefix: str, file_type: str, description: Optional[str] = None) -> Optional[str]:
    """
    在 'files' 表格中創建或更新一個檔案記錄。
    以表格或資料 ID 作為 file_id的組合鍵，並利用資料庫的 UNIQUE 約束和 ON CONFLICT DO UPDATE。

    返回:
    Optional[int]: 新建立或更新的檔案的 file_id (字串)；如果操作失敗則返回 None。
    """
    logger.debug(f"進入 create_file_entry_with_upsert。file_path: '{file_path}', file_id_prefix: '{file_id_prefix}', file_type: '{file_type}'")
    if not _ensure_db_connection():
        return None
    try:
        file_size = _get_file_size_in_bytes(file_path)
        if not file_size:
            logger.error(f"無法獲取檔案 '{file_path}' 的大小。")
            return None
        logger.debug(f"檔案 '{file_path}' 大小為: {file_size}")
    except Exception as e:
        logger.error(f"獲取檔案 '{file_path}' 的大小時發生錯誤: {e}")
        return None
    
    file_id = None # 初始化 file_id
    try:
        # 嘗試查詢絕對路徑
        sql_query_abs = f"SELECT file_id, file_path FROM files WHERE file_path = %s;"
        logger.debug(f"查詢絕對路徑: SQL: '{sql_query_abs}', Params: ('{file_path}',)")
        results_abs = _execute_sql(sql_query_abs, (file_path,), fetch_one=True)
        logger.debug(f"絕對路徑查詢結果 (results_abs): {results_abs}")

        if results_abs:
            # 找到絕對路徑匹配的記錄
            file_id = results_abs[0]
            logger.notice(f"檔案路徑 '{file_path}' (絕對路徑) 已存在於 'files' 表格中。實際使用的 file_id 為 '{file_id}'。")
        else:
            # 如果絕對路徑沒有匹配，嘗試查詢相對路徑
            relative_file_path = os.path.relpath(file_path, current_base_dir)
            sql_query_rel = f"SELECT file_id, file_path FROM files WHERE file_path = %s;"
            logger.debug(f"絕對路徑未找到，嘗試查詢相對路徑: '{relative_file_path}'. SQL: '{sql_query_rel}', Params: ('{relative_file_path}',)")
            results_rel = _execute_sql(sql_query_rel, (relative_file_path,), fetch_one=True)
            logger.debug(f"相對路徑查詢結果 (results_rel): {results_rel}")

            if results_rel:
                # 找到相對路徑匹配的記錄，需要更新其 file_path 為絕對路徑
                file_id = results_rel[0]
                old_file_path_in_db = results_rel[1]
                logger.notice(f"檔案路徑 '{old_file_path_in_db}' (相對路徑) 已存在於 'files' 表格中，將更新為絕對路徑 '{file_path}'。現有 file_id: '{file_id}'")
                
                # 更新 file_path
                update_sql = """
                    UPDATE files
                    SET file_path = %s,
                        file_size = %s,
                        description = %s,
                        upload_date = CURRENT_TIMESTAMP
                    WHERE file_id = %s
                    RETURNING file_id;
                """
                update_params = (file_path, file_size, description, file_id)
                logger.debug(f"執行更新操作: SQL: '{update_sql}', Params: {update_params}")
                update_result = _execute_sql(update_sql, update_params, fetch_one=True)
                
                if update_result:
                    logger.success(f"成功更新檔案 '{file_id}' 的路徑從 '{old_file_path_in_db}' 到 '{file_path}'。返回 file_id: '{update_result[0]}'")
                    return update_result[0]
                else:
                    logger.error(f"更新檔案 '{file_id}' 的路徑失敗。")
                    return None
            else:
                # 絕對路徑和相對路徑都沒找到，按照原有的邏輯生成新的 file_id
                sql_query_like = f"SELECT file_id FROM files WHERE file_id LIKE %s;"
                logger.debug(f"絕對路徑和相對路徑都未找到。準備生成新的 file_id。查詢 file_id LIKE '{file_id_prefix}%'...")
                results_like = _execute_sql(sql_query_like, (file_id_prefix + '%',), fetch_all=True) # 使用 % 匹配任何後綴
                logger.debug(f"results_like for '{file_id_prefix}%': {results_like}")
                max_id =  _get_id_from_query_results(results_like, file_id_prefix, "files")
                file_id = file_id_prefix + "file" + str(int(max_id)+1)
                logger.notice(f"檔案路徑 '{file_path}' 不存在於 'files' 表格中，生成新的 file_id: '{file_id}'。")
    except Exception as e:
        logger.error(f"在 create_file_entry_with_upsert 中獲取或生成 file_id 失敗: {e}")
        return None
    
    # 執行 INSERT 或 ON CONFLICT DO UPDATE
    sql_query = """
        INSERT INTO files (
            file_id, file_path, file_name, file_type, file_size, description
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (file_path) DO UPDATE SET
            file_size = EXCLUDED.file_size,
            description = EXCLUDED.description,
            upload_date = CURRENT_TIMESTAMP -- 檔案資訊更新時，更新時間戳
        RETURNING file_id; -- 返回實際插入或更新的 id
    """
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    params = (
        file_id,
        file_path,
        file_name,
        file_type,
        file_size,
        description
    )
    logger.debug(f"準備執行最終 INSERT/UPSERT 操作。SQL: '{sql_query}', Params: {params}")
    result = _execute_sql(sql_query, params, fetch_one=True) # 使用 fetch_one 來獲取 RETURNING 的值

    if result:
        actual_file_id = result[0]
        logger.notice(f"檔案路徑 '{file_path}' 已處理。實際使用的 file_id 為 '{actual_file_id}'。")
        return actual_file_id
    else:
        logger.error(f"處理檔案記錄到 'files' 表格失敗。")
        return None

def _get_file_size_in_bytes(file_path: str) -> Optional[str]:
    """
    獲取指定檔案的大小，並以適合閱讀的格式（B, KB, MB, GB, TB）返回。
    格式化規則：
    - 小於 100 位元組：直接顯示位元組數 (例如 "50 B")。
    - KB, MB, GB, TB 單位：
        - 如果數值小於 1000：顯示一位小數 (例如 "1.2 KB", "99.5 MB")。
        - 如果數值大於等於 1000：進位到下一單位並顯示兩位小數 (例如 "1.00 MB" 而不是 "1024 KB")。

    參數:
    file_path (str): 檔案的完整路徑。

    返回:
    Optional[str]: 格式化後的檔案大小字串，如果檔案不存在或無法訪問則返回 None。
    """
    try:
        if not os.path.exists(file_path):
            logger.error(f"錯誤：檔案 '{file_path}' 不存在。")
            return None
        
        file_size_bytes = os.path.getsize(file_path)
        
        if file_size_bytes == 0:
            return "0 B"
        
        # 如果小於 100 位元組，直接返回位元組數
        if file_size_bytes < 100:
            return f"{file_size_bytes} B"

        units = ["B", "KB", "MB", "GB", "TB"]
        # 從 KB 開始計算，因為 B 已經特殊處理
        i = 1
        size = float(file_size_bytes) / 1024 # 從 Bytes 轉換為 KB

        while size >= 1000 and i < len(units) - 1:
            size /= 1024
            i += 1
        
        # 格式化輸出
        if size < 10:
            formatted_size = f"{size:.2f} {units[i]}"
        elif size < 100:
            formatted_size = f"{size:.1f} {units[i]}"
        elif size < 1000:
            formatted_size = f"{int(size)} {units[i]}"
        else: # size >= 1000
            formatted_size = f"{int(size)} {units[i]}"

        logger.notice(f"檔案 '{file_path}' 的大小為 {formatted_size}。")
        return formatted_size
    except Exception as e:
        logger.error(f"獲取檔案 '{file_path}' 大小時發生錯誤: {e}")
        return None

def insert_record_file_entry(record_id: str, file_id: str) -> bool:
    """
    將 record_id 和 file_id 插入到 'record_files' 表格中。
    
    參數:
    record_id (str): 主記錄的唯一識別 ID。
    file_id (str): 檔案的唯一識別 ID。
    
    返回:
    bool: 如果插入成功則返回 True，否則返回 False。
    """
    if not _ensure_db_connection():
        return False
    
    table_name = "record_files"
    try:
        # 檢查記錄是否已存在，避免重複插入
        check_sql = f"SELECT 1 FROM \"{table_name}\" WHERE record_id = %s AND file_id = %s;"
        existing_entry = _execute_sql(check_sql, (record_id, file_id), fetch_one=True)
        
        if existing_entry:
            logger.notice(f"記錄 (record_id: {record_id}, file_id: {file_id}) 已存在於 '{table_name}' 表格中，跳過插入。")
            return None
        
        insert_sql = f"""
        INSERT INTO \"{table_name}\" (record_id, file_id)
        VALUES (%s, %s);
        """
        params = (record_id, file_id)
        
        if _execute_sql(insert_sql, params):
            logger.notice(f"記錄 (record_id: {record_id}, file_id: {file_id}) 已成功插入到 '{table_name}' 表格。")
            return True
        else:
            logger.error(f"插入記錄到 '{table_name}' 表格失敗。")
            return False
    except Exception as e:
        logger.error(f"插入記錄到 '{table_name}' 表格時發生錯誤: {e}")
        return False
    
def get_table_columns(table_name: str) -> List[Dict]:
    """
    獲取指定表格的所有欄位。
    """
    if not _ensure_db_connection():
        return []
    get_sql = f"SELECT column_name FROM information_schema.columns WHERE table_name = %s;"
    try:
        columns = _execute_sql(get_sql, (table_name,), fetch_all=True)
        logger.debug(f"get_table_columns: _execute_sql 返回值類型: {type(columns)}, 值: {columns}")
        if columns is False or columns is None: # 如果 _execute_sql 失敗或沒有結果，則回傳空列表
            return []
        # 將從資料庫獲取的欄位名稱列表轉換為 get_comparison_columns 期望的單一字典格式
        # 例如：[{'column_name': '交易日期'}, ...] 轉換為 [{'交易日期': None, '作物代號': None, ...}]
        formatted_columns = {col['column_name']: None for col in columns}
        return [formatted_columns]
    except Exception as e:
        logger.error(f"獲取表格 '{table_name}' 的欄位時發生錯誤: {type(e).__name__}: {e}")
        return []

def _get_dataset_content(category_table_id: str, data_len: int = None) -> Optional[pd.DataFrame]:
    """
    從指定資料集的表格中取出內容。如果指定資料筆數，則從最開始取出指定數值筆數的資料。

    Args:
        category_table_id (str): 資料集的表格名稱。
        data_len (int, optional): 要取出的資料筆數。如果為 None，則取出所有資料。

    Returns:
        Optional[pd.DataFrame]: 包含資料的 pandas DataFrame，如果表格不存在或查詢失敗則返回 None。
    """
    if not _ensure_db_connection():
        return None

    table_name = category_table_id # 表格名稱就是 category_table_id

    if not _table_exists(table_name):
        logger.error(f"錯誤：資料表格 '{table_name}' 不存在。")
        return None

    sql_query = f'SELECT * FROM "{table_name}"'
    params = None

    if data_len is not None and data_len > 0:
        sql_query += f' LIMIT %s'
        params = (data_len,)
        logger.notice(f"正在從表格 '{table_name}' 取出前 {data_len} 筆資料...")
    else:
        logger.notice(f"正在從表格 '{table_name}' 取出所有資料...")

    try:
        rows = _execute_sql(sql_query, params, fetch_all=True)

        if rows is None or rows is False:
            logger.error(f"從表格 '{table_name}' 獲取資料失敗。")
            return None
        
        if not rows:
            logger.notice(f"表格 '{table_name}' 中沒有資料。")
            return pd.DataFrame() # 返回空的 DataFrame

        df = pd.DataFrame(rows)
        logger.notice(f"已從表格 '{table_name}' 成功獲取 {len(df)} 筆資料。")
        return df

    except Exception as e:
        logger.error(f"從表格 '{table_name}' 獲取資料時發生錯誤: {e}")
        return None

def get_dataset_content_for_list(category_table_id: str) -> Optional[pd.DataFrame]:
    """
    顯示指定資料集的內容清單。

    Args:
        category_table_id (str): 資料集的表格名稱。

    Returns:
        Optional[pd.DataFrame]: 包含資料的 pandas DataFrame，如果表格不存在或獲取資料失敗則返回 None。
    """
    if not _ensure_db_connection():
        return None

    table_name = category_table_id

    if not _table_exists(table_name):
        logger.error(f"錯誤：資料表格 '{table_name}' 不存在。")
        return None

    # 獲取資料總筆數
    count_sql = f'SELECT COUNT(*) FROM "{table_name}";'
    count_result = _execute_sql(count_sql, fetch_one=True)

    if count_result is None or count_result is False:
        logger.error(f"獲取表格 '{table_name}' 的資料筆數失敗。")
        return None

    total_records = count_result[0]
    logger.notice(f"表格 '{table_name}' 總共有 {total_records} 筆資料。")

    if total_records == 0:
        logger.notice(f"表格 '{table_name}' 中沒有資料。")
        return pd.DataFrame() # 返回空的 DataFrame

    # 詢問使用者是否顯示全部或指定筆數
    if yes_no_menu(f"資料集內容含有 {total_records} 筆資料，是否全部顯示？"):
        df = _get_dataset_content(category_table_id)
        return df
    else:
        while True:
            try:
                data_len_input = input("請輸入要顯示的資料筆數：")
                data_len = int(data_len_input)
                if data_len > 0:
                    break
            except Exception:
                logger.notice("輸入不是有效的整數。")

        df = _get_dataset_content(category_table_id, data_len)
        return df

def _ensure_columns_exist(table_name: str, columns: list[str]) -> bool:
    """
    確保表格中存在指定的所有欄位，若缺少則自動新增（類型預設 TEXT）。
    columns 為欄位名稱字串列表（未加引號的原始名稱）。
    """
    if not _ensure_db_connection():
        return False
    existing_result = _execute_sql(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s;",
        (table_name,), fetch_all=True
    )
    if existing_result is False or existing_result is None:
        logger.error(f"_ensure_columns_exist: 無法取得表格 '{table_name}' 的欄位資訊。")
        return False
    existing_cols = {row['column_name'] for row in existing_result}
    for col in columns:
        if col not in existing_cols:
            alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT;'
            if _execute_sql(alter_sql):
                logger.warning(f"表格 '{table_name}' 新增欄位: {col}")
            else:
                logger.error(f"表格 '{table_name}' 新增欄位 '{col}' 失敗。")
                return False
    return True

def preserve_old_data_by_date(data, year = 2025, month = 1, day = 1):
    """
    自動偵測時間欄位，並保留指定年份之後的資料。
    """
    if not data:
        return []
    try:
        int_year = int(year)
        int_month = int(month)
        int_day = int(day)
        if int_year < 1950 or int_month > 12 or int_day > 31:
            logger.error("日期不正確，請重新輸入。")
            return data
    except (ValueError, IndexError):
        logger.error("日期不是有效的格式，請重新輸入。")
        return data
    df = pd.DataFrame(data)
    all_cols = df.columns.tolist()

    # 1. 自動偵測時間欄位
    not_filter_keywords = ["crop_uid", "分布圖"]
    date_keywords = ["date", "日期", "年度", "year", "年份"]
    col_name = None
    for col in df.columns:
        if any(key in str(col).lower() for key in not_filter_keywords):
            logger.notice("沒有時間欄位的資料集，跳過過濾邏輯。")
            return data
        if any(key in str(col).lower() for key in date_keywords):
            col_name = col
            break
        
    # 2. 如果找不到，改為手動輸入
    if col_name is None:
        logger.info("找不到時間欄位，請手動選擇。")
        for i, col in enumerate(all_cols):
            print(f"{i}. {col}")         
            print(f"{i+1}. 這個資料集沒有時間欄位。")   
        while True:
            try:
                selection = input("請輸入欄位索引編號: ")
                if int(selection) < len(all_cols):
                    col_name = all_cols[int(selection)]
                elif int(selection) == len(all_cols):
                    col_name = None
                break
            except (ValueError, IndexError):
                print("輸入無效，請輸入列表中的數字編號。")

    # 3. 執行過濾邏輯
    preserve_date = pd.Timestamp(int_year, int_month, int_day)
    logger.debug(f"保留開始日期基準: {preserve_date}")
    unique_dates = df[col_name].astype(str).unique()
    date_map = {d: parse_date_string(d) for d in unique_dates}
    df['_temp_parsed_date'] = df[col_name].astype(str).map(date_map)
    filtered_df = df[df['_temp_parsed_date'] >= preserve_date].copy()
    filtered_df.drop(columns=['_temp_parsed_date'], inplace=True)
    logger.notice(f"過濾完成：使用欄位 [{col_name}]，保留 {len(filtered_df)} / {len(df)} 筆資料。")
    if filtered_df.empty:
        logger.warning(f"過濾後沒有剩餘資料，請檢查年份 {year} 是否過晚或欄位 [{col_name}] 內容是否正確。")
    return filtered_df.to_dict('records')
    
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv, dotenv_values
    from logs_handle import logger, setup_logging
    
    load_dotenv() # 在程式啟動時載入 .env 檔案
    config = dotenv_values() # 讀取 .env 檔案中的值
    USERNAME = config.get("USERNAME")
    PASSWORD = config.get("PASSWORD")
    DBNAME = config.get("DBNAME")
    DB = connect_db(USERNAME, PASSWORD, DBNAME)    
    setup_logging(level=10) # 在程式啟動時配置日誌系統，可以根據需要調整級別
    # file_id_prefix = "1040001"
    # sql_query_like = f"SELECT file_id FROM files WHERE file_id LIKE %s;"
    # results_like = _execute_sql(sql_query_like, (file_id_prefix + '%',), fetch_all=True) # 使用 % 匹配任何後綴
    # max_id =  _get_id_from_query_results(results_like, file_id_prefix, "files")
    # file_id = file_id_prefix + "file" + str(int(max_id)+1)
    # print("file ID:", file_id)
    # print("results:", results_like)
    minor_info = get_minor_info_data()
    print(minor_info)