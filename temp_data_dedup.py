from logs_handle import logger, setup_logging
import utils
import data_parser
import os
import database_manager
import pandas as pd
import psycopg2
from typing import List
from utils import parse_date_string
from dotenv import load_dotenv, dotenv_values
import re
import datetime
import json_file_operations
import operations_of_postgresql
import sort_data_by_date

setup_logging(level=10) # 在程式啟動時配置日誌系統，可以根據需要調整級別
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.width', 120)
pd.set_option('display.max_colwidth', 60)
pd.set_option('display.colheader_justify', 'left')
load_dotenv() # 在程式啟動時載入 .env 檔案
config = dotenv_values() # 讀取 .env 檔案中的值
USERNAME = config.get("USERNAME")
PASSWORD = config.get("PASSWORD")
DBNAME = config.get("DBNAME")
DB = database_manager.connect_db(USERNAME, PASSWORD, DBNAME)    

def _convert_roc_to_ad(roc_year_str):
    """
    將民國年份字串轉換為西元年份整數。
    例如：'111' -> 2022
    """
    try:
        roc_year = int(roc_year_str)
        return roc_year + 1911
    except ValueError:
        logger.warning(f"無法將民國年份 '{roc_year_str}' 轉換為數字。")
        return None # 返回 None 表示轉換失敗

def parse_date_string(str_value: str):
    """
    解析多種日期時間格式，包括民國日期。
    """
    # 嘗試解析民國僅有年份 (例如: 111)
    str_value = str_value.replace(' ', '') # 移除字串中間的空格
    roc_year_only_match = re.match(r'^(\d{1,3})$', str_value)
    if roc_year_only_match:
        roc_year_str = roc_year_only_match.group(1)
        ad_year = _convert_roc_to_ad(roc_year_str)
        if ad_year is not None:
            try:
                return datetime.datetime(ad_year, 1, 1) # 視為該年份的1月1日
            except ValueError as e:
                logger.warning(f"解析民國年份 '{str_value}' (轉換為 '{ad_year}') 時發生錯誤: {e}，將其視為最小日期。")
                return datetime.datetime.min
        else:
            logger.warning(f"民國年份 '{roc_year_str}' 轉換失敗，無法解析日期 '{str_value}'，將其視為最小日期。")
            return datetime.datetime.min

    # 嘗試解析民國日期格式 (例如: 111.10.17 或 99/9/9)
    roc_full_date_match = re.match(r'^((\d{1,3})[./](\d{1,2})[./](\d{1,2}))$', str_value)
    if roc_full_date_match:
        roc_year_str, month_str, day_str = roc_full_date_match.groups()[1:]
        ad_year = _convert_roc_to_ad(roc_year_str)
        if ad_year is not None:
            try:
                # 重新組合成西元格式字串並解析
                ad_date_str = f"{ad_year}/{month_str}/{day_str}"
                return datetime.datetime.strptime(ad_date_str, "%Y/%m/%d")
            except ValueError as e:
                logger.warning(f"解析民國日期 '{str_value}' (轉換為 '{ad_date_str}') 時發生錯誤: {e}，將其視為最小日期。")
                return datetime.datetime.min
        else:
            logger.warning(f"民國年份 '{roc_year_str}' 轉換失敗，無法解析日期 '{str_value}'，將其視為最小日期。")
            return datetime.datetime.min

    # 嘗試解析多種西元日期時間格式
    for fmt in ["%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d", "%Y%m%d", "%Y-%m", "%Y"]:
        try:
            return datetime.datetime.strptime(str_value, fmt)
        except ValueError:
            continue
    
    # 如果所有格式都無法解析，則返回 datetime.min 以確保其排在所有有效日期之前
    logger.warning(f"無法解析日期 '{str_value}'，將其視為最小日期。")
    return datetime.datetime.min

def preserve_old_data_by_date(data, year = 2025):
    """
    自動偵測時間欄位，並保留指定年份之後的資料。
    """
    if not data:
        return []
    try:
        int_year = int(year)
        if int_year < 1950:
            logger.error("日期不正確，請重新輸入。")
            return data
    except (ValueError, IndexError):
        logger.error("日期不是有效的格式，請重新輸入。")
        return data
    df = pd.DataFrame(data)
    all_cols = df.columns.tolist()

    # 1. 自動偵測時間欄位
    date_keywords = ["date", "日期", "年度", "year", "年份"]
    col_name = None
    for col in df.columns:
        if any(key in str(col).lower() for key in date_keywords):
            col_name = col
            break
        
    # 2. 如果找不到，改為手動輸入
    if col_name is None:
        logger.info("找不到時間欄位，請手動選擇。")
        for i, col in enumerate(all_cols):
            print(f"{i}. {col}")            
        while True:
            try:
                selection = input("請輸入欄位索引編號: ")
                col_name = all_cols[int(selection)]
                break
            except (ValueError, IndexError):
                print("輸入無效，請輸入列表中的數字編號。")

    # 3. 執行過濾邏輯
    preserve_date = pd.Timestamp(int_year, 1, 1)
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

def check_dataset_content_update_status(title: str, comparison_columns: List[str], current_dataset_data: List[dict], category_table_id_prefix: str) -> List[dict]:
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
    records_to_insert = []
    existing_record_ids = [] # 新增一個列表來儲存比對鍵相同的記錄 ID
    table_name = category_table_id_prefix
    # 獲取當前最大的 category_table_data_id，並在函式內部維護一個計數器
    max_category_table_data_id = int(database_manager.get_max_category_table_data_id(table_name, category_table_id_prefix))

    if not database_manager._ensure_db_connection():
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
            if database_manager._table_exists(category_table_id_prefix):
                db_records_raw = database_manager._execute_sql(f"SELECT {select_clause}, category_table_data_id FROM \"{category_table_id_prefix}\";", fetch_all=True)
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

        if existing_record_ids:
            logger.notice(f"以下記錄已存在且比對鍵相同，無需更新或插入: {existing_record_ids[:3]}...{existing_record_ids[-3:]} (共 {len(existing_record_ids)} 個)")
        
        logger.notice(f"與資料庫比對完成。需要插入 {len(records_to_insert)} 條記錄。")
        return records_to_insert

    except psycopg2.errors.UndefinedTable:
        logger.error(f"資料表 '{title}' 不存在，無法進行內容比對。請先確保表格已建立。")
        return [], []
    except Exception as e:
        logger.error(f"檢查資料集 '{title}' 內容更新狀態失敗: {e}")
        return [], []
    

def mimic_comparaion_of_data():
    """
    從所有資料集清單選擇項目並讀取模擬資料與資料庫資料，並以時間篩選後進行比對的邏輯。
    """
    # 以下為模擬資料處理
    target_data_path = r"C:\Python\data_moa_gov_tw\raw_data"
    nor_target_data_path = os.path.normpath(target_data_path)
    json_filepath_list = []
    for root, dirs, files in os.walk(nor_target_data_path):
        for file in files:
            # 檢查副檔名是否為 .json (不分大小寫比較更穩健)
            if file.lower().endswith(".json"):
                # 組合完整路徑並加入列表
                full_path = os.path.join(root, file)
                json_filepath_list.append(full_path)
    # 從資料集清單選擇，獲得標題與表格id
    df_display = operations_of_postgresql._listing_metadata()
    selected_row_dict = utils.select_row_by_index(df_display, "請輸入序號選擇要查看的資料集簡介 (q 退出): ")
    if selected_row_dict:
        # 從選定的行字典中獲取表格id，用於獲取完整的 metadata
        identifier_value = selected_row_dict.get('表格id')
        title = selected_row_dict.get('標題')
    # 讀取模擬資料
    for json_filepath in json_filepath_list:
        if title in json_filepath:
            nor_json_filepath = os.path.normpath(json_filepath)
            json_data = json_file_operations.load_json_data(nor_json_filepath)
            print(f"取得 '{title}' 的模擬用資料集共 {len(json_data)} 筆。")
            break

    # 取得比對欄位
    comparison_columns = utils.get_comparison_columns(json_data)
    if not comparison_columns:
        logger.warning("未選擇任何欄位，無法進行資料清理。")
        return

    category_table_id_prefix = identifier_value
    current_dataset_data = json_data
    
    # 取得資料庫資料，使用check_dataset_content_update_status片段
    records_to_insert = []
    existing_record_ids = [] # 新增一個列表來儲存比對鍵相同的記錄 ID
    table_name = category_table_id_prefix
    # 獲取當前最大的 category_table_data_id，並在函式內部維護一個計數器
    max_category_table_data_id = int(database_manager.get_max_category_table_data_id(table_name, category_table_id_prefix))

    if not database_manager._ensure_db_connection():
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
            if database_manager._table_exists(category_table_id_prefix):
                db_records_raw = database_manager._execute_sql(f"SELECT {select_clause}, category_table_data_id FROM \"{category_table_id_prefix}\";", fetch_all=True)
            else:
                db_records_raw = []
            print(f"在資料集 '{title}' 中共找到 {len(db_records_raw)} 筆資料。")
            last_five = db_records_raw[-5:]
            for item in last_five:
                print(item)
            # 以下為預定插入check_dataset_content_update_status的片段
            db_records_raw = preserve_old_data_by_date(db_records_raw)
            logger.notice(f"在資料集 '{title}' 按照時間過濾後共找到 {len(db_records_raw)} 筆資料。")
            last_five = db_records_raw[-5:]
            for item in last_five:
                print(item)

    except:
        pass






if __name__ == "__main__":

    mimic_comparaion_of_data()