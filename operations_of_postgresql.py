import pandas as pd
from dotenv import load_dotenv, dotenv_values
from logs_handle import logger, setup_logging
from database_manager import _ensure_db_connection, _execute_sql, DB, connect_db, delete_table_and_metadata_entry, _table_exists, get_global_data # 導入輔助函數和全局 DB
from database_manager import rename_data_tables, get_table_columns, get_dataset_content_for_list, get_minor_info_data
import os, re
import shutil # 導入 shutil 模組用於高階檔案操作
from typing import Optional, List, Tuple # 導入 Optional
from json_file_operations import delete_metadata_entry_from_json # 導入新的 JSON 檔案操作函式
from utils import display_dataframe, select_row_by_index, get_comparison_columns
from menu_utils import yes_no_menu
from sort_data_by_date import load_minor_info
import json
from db_maintenance import create_indexes_for_all_tables

def _clean_string(text: str) -> str:
    """
    清理字串，移除標點符號和符號，並轉換為小寫。
    """
    if not isinstance(text, str):
        return ""
    text = re.sub(r'[^\w\s]', '', text) # 移除標點符號和符號
    return text.lower().strip()

def get_metadata(identifier: Optional[str] = None, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    從指定表格中獲取 metadata。
    如果提供 identifier，則根據標題或 category_table_id 獲取單一資料集的完整簡介。
    如果未提供 identifier，則獲取所有資料集的指定欄位或所有欄位。
    返回 Pandas DataFrame。

    Args:
        table_name (str): 要查詢的表格名稱。
        identifier (Optional[str]): 可選的標識符（標題或 category_table_id）。
                                    如果提供，則查詢單一記錄。
        columns (Optional[List[str]]): 可選的欄位列表。如果提供，則只查詢這些欄位。
                                       如果為 None，則查詢所有欄位。
    """
    if not _ensure_db_connection():
        return pd.DataFrame()
    
    try:
        global_data = get_global_data()
        if not global_data:
            logger.error("未能成功從元資料表格獲取資料。")
            return pd.DataFrame()
        if identifier:
            # 從快取中查找單一記錄
            # 這裡 identifier 是 category_table_id 或標題
            found_record = None
            if identifier in global_data:
                found_record = global_data[identifier]
            else:
                # 如果 identifier 不是 category_table_id，則嘗試匹配標題
                for record in global_data.values():
                    if identifier in record.get("標題", ""):
                        found_record = record
                        break
            if found_record:
                # 如果指定了欄位，則只返回這些欄位
                if columns:
                    filtered_record = {k: found_record[k] for k in columns if k in found_record}
                    df = pd.DataFrame([filtered_record], columns=columns)
                else:
                    df = pd.DataFrame([found_record])
                return df
            else:
                logger.warning(f"未在快取中找到標識符 '{identifier}' 的 metadata。")
                return pd.DataFrame()
        else:
            # 從快取中獲取所有記錄
            if global_data:
                all_records = list(global_data.values())
                if columns:
                    # 如果指定了欄位，則只返回這些欄位
                    filtered_records = [{k: record[k] for k in columns if k in record} for record in all_records]
                    df = pd.DataFrame(filtered_records, columns=columns)
                else:
                    df = pd.DataFrame(all_records)
                return df
            else:
                logger.warning(f"快取中沒有 metadata。")
                return pd.DataFrame()
    except Exception as e:
        logger.error(f"獲取表格 'metadata_index' 的 metadata 失敗: {e}")
        return pd.DataFrame()
    
def _listing_metadata():
    """
    顯示所有資料集清單的邏輯。
    """
    display_columns = ['標題', '資料分類', '表格id', '資料筆數', '已處理']

    # 從資料庫獲取原始欄位名稱的 DataFrame
    metadata_df_raw = get_metadata(columns=['標題', '資料分類', 'category_table_id', '資料筆數'])
    if not metadata_df_raw.empty and len(metadata_df_raw) > 0:
        df_display = metadata_df_raw.copy()
        # 將 'category_table_id' 欄位重新命名為 '表格id' 以便顯示
        if 'category_table_id' in df_display.columns:
            df_display.rename(columns={'category_table_id': '表格id'}, inplace=True)
        
        # 獲取所有資料庫表格名稱，用於判斷是否有 _processed 表格
        all_db_tables_raw = _execute_sql("SELECT tablename FROM pg_tables WHERE schemaname = 'public';", fetch_all=True)
        db_tables_set = {table['tablename'] for table in all_db_tables_raw if all_db_tables_raw and table['tablename'] not in ['metadata_index', 'files', 'recored_files']}

        # 新增 '已處理' 欄位，基於 category_table_id 判斷
        df_display['已處理'] = df_display['表格id'].apply(
            lambda x: '是' if f"{x}_processed" in db_tables_set else ''
        )
        # 確保顯示的欄位順序和名稱正確
        df_display = df_display[display_columns]
        # 根據 '表格id' 進行排序
        df_display = df_display.sort_values(by='表格id', ascending=True).reset_index(drop=True)
        return df_display
    else:
        logger.error("資料庫中沒有資料集清單。")
        return

def _get_and_display_metadata_list():
    """
    處理顯示所有資料集清單的邏輯。
    """
    df_display = _listing_metadata()
    selected_row_dict = select_row_by_index(df_display, "請輸入序號選擇要查看的資料集簡介 (q 退出): ")
    if selected_row_dict:
        # 從選定的行字典中獲取表格id，用於獲取完整的 metadata
        identifier_value = selected_row_dict.get('表格id')
        title = selected_row_dict.get('標題')
        if identifier_value:
            _display_dataset_summary(identifier_value)
            _show_dataset(title, identifier_value)
        else:
            logger.warning("無法從選定的資料集中獲取有效的標識符。")
    else:
        logger.info("使用者取消查看資料集簡介。")

def _display_dataset_summary(table_name: str):
    """
    顯示指定資料集的概要資訊。
    """
    one_metadata_df = get_metadata(identifier=table_name)
    if not one_metadata_df.empty and len(one_metadata_df) > 0:
        title = one_metadata_df["標題"].iloc[0] # 顯示標題
        print(f"資料集 '{title}' 概要")
        one_metadata_t = one_metadata_df.T
        for index, row in one_metadata_t.iterrows():
            if index == "category_table_id":
                index = "表格id"
            elif index == "created_at" or index == "last_updated_at":
                continue
            print(f"<{index}>\n{row[0]}")
    else:
        logger.warning(f"資料庫中沒有指定資料集 '{table_name}' 的概要資訊。")

def _search_and_display_datasets():
    """
    處理顯示特定資料集內容清單的邏輯，使用 _search_and_select_dataset 進行選擇。
    """
    print("\n--- 查詢並顯示資料集簡介 ---")
    selected_table_name, selected_title = _search_and_select_dataset('請輸入要查詢的資料集關鍵字 (q 退出): ')

    if selected_table_name:
        _display_dataset_summary(selected_table_name)
        _show_dataset(selected_title, selected_table_name)
    else:
        logger.info("使用者取消查詢操作。")

def _perform_dataset_deletion(table_name_input: str, title: Optional[str] = None) -> bool:
    """
    執行資料集（資料表格及其 metadata 記錄、相關檔案和 metadata.json 條目）的實際刪除操作。
    根據輸入的表格名稱判斷是原始表格還是處理後表格，並執行相應的刪除邏輯。
    Args:
        table_name_input (str): 使用者輸入的要刪除的資料表格名稱（可能是原始或處理後）。
    Returns:
        bool: 如果刪除成功則返回 True，否則返回 False。
    """
    current_file_dir = os.path.dirname(os.path.abspath(__file__)) # 獲取當前檔案的目錄
    is_processed_table = table_name_input.endswith('_processed')
    category_table_id = table_name_input.replace('_processed', '') # 獲取不帶後綴的 category_table_id
    
    original_table_name = category_table_id
    processed_table_name = f"{category_table_id}_processed"

    confirm_message = ""
    if is_processed_table:
        confirm_message = f"您確定要刪除處理後資料集 '{processed_table_name}' 嗎？這將無法恢復！"
    else:
        confirm_message = f"您確定要刪除資料集 '{original_table_name}' 及其所有關聯數據（包括原始表格、處理後表格、metadata 記錄和相關檔案）嗎？這將無法恢復！"
    if not yes_no_menu(confirm_message):
        logger.info("使用者取消刪除操作。")
        return False
    if not category_table_id:
        logger.error(f"無法確定資料集 ID，終止刪除操作。")
        return False

    original_deletion_success = False # Initialize flag for original dataset deletion success

    if not is_processed_table:
        # 刪除原始資料夾
        raw_data_path = os.path.join(current_file_dir, "raw_data", title) # 使用絕對路徑
        if os.path.exists(raw_data_path):
            try:
                shutil.rmtree(raw_data_path)
                logger.notice(f"已刪除原始資料目錄: {raw_data_path}")
            except OSError as e:
                logger.error(f"刪除原始資料目錄 '{raw_data_path}' 失敗: {e}")
        else:
            logger.notice(f"原始資料目錄 '{raw_data_path}' 不存在，跳過刪除。")

        # 從 metadata/metadata.json 中刪除對應的字典
        # delete_metadata_entry_from_json 已經在 json_file_operations.py 中處理了絕對路徑
        delete_metadata_entry_from_json("metadata/metadata.json", category_table_id)

        # 刪除原始表格
        if _table_exists(category_table_id):
            drop_table_sql = f'DROP TABLE IF EXISTS "{category_table_id}" CASCADE;'
            if _execute_sql(drop_table_sql):
                logger.notice(f"資料表格 '{category_table_id}' 已從 PostgreSQL 資料庫中刪除。")
            else:
                logger.error(f"刪除資料表格 '{category_table_id}' 失敗。")
        else:
            logger.notice(f"資料表格 '{category_table_id}' 不存在，跳過刪除。")

        # 從 metadata_index 表格中刪除對應的列
        # 這裡直接呼叫 delete_table_and_metadata_entry，並讓它處理 metadata 刪除
        if delete_table_and_metadata_entry(category_table_id, delete_table=False):
            logger.notice("資料集刪除成功。")
            original_deletion_success = True
        else:
            logger.error(f"刪除資料集 '{category_table_id}' 的 metadata 記錄失敗。")
            original_deletion_success = False

    # Processed deletion logic, executed in both cases
    # 刪除 processed_data 資料夾 (如果存在)
    processed_data_path = os.path.join(current_file_dir, "processed_data", f"{title}_processed") # 使用絕對路徑
    if os.path.exists(processed_data_path):
        try:
            shutil.rmtree(processed_data_path)
            logger.notice(f"已刪除處理過的資料目錄: {processed_data_path}")
        except OSError as e:
            logger.error(f"刪除處理過的資料目錄 '{processed_data_path}' 失敗: {e}")
    else:
        logger.notice(f"處理過的資料目錄 '{processed_data_path}' 不存在，跳過刪除。")

    # 刪除 processed 表格 (如果存在)
    processed_table_deleted = False # Flag to track processed table deletion success
    if _table_exists(processed_table_name):
        drop_processed_table_sql = f'DROP TABLE IF EXISTS "{processed_table_name}" CASCADE;'
        if _execute_sql(drop_processed_table_sql):
            logger.notice(f"處理後資料表格 '{processed_table_name}' 已從 PostgreSQL 資料庫中刪除。")
            processed_table_deleted = True
        else:
            logger.error(f"刪除處理後資料表格 '{processed_table_name}' 失敗。")
            processed_table_deleted = False
    else:
        logger.notice(f"處理後資料表格 '{processed_table_name}' 不存在，跳過刪除。")
        processed_table_deleted = True # Consider it successful if it didn't exist

    # Determine final return value based on original input and deletion results
    if is_processed_table:
        if processed_table_deleted:
            logger.notice("處理後資料集刪除成功。") # Add back the success log for processed only deletion
        else:
            logger.error("處理後資料集刪除失敗。") # Add back the failure log for processed only deletion
        return processed_table_deleted
    else: # not is_processed_table
        # The success of deleting the original dataset is determined by the metadata deletion
        # which happened inside the if not is_processed_table block and stored in original_deletion_success.
        return original_deletion_success if processed_table_deleted else False

def _search_and_select_dataset(prompt_message: str, data_categories: Optional[List[str]] = None) -> Optional[Tuple[str, str]]:
    """
    通用函數，用於處理使用者輸入、搜尋和選擇資料集。
    Args:
        prompt_message (str): 提示使用者輸入的訊息。
        mode (str): 'table_number', 'keyword', 'category'
        data_categories (Optional[List[str]]): 僅在 mode 為 'category' 時使用。
    Returns:
        Optional[str]: 如果成功選擇，返回 category_table_id, title。
                       如果使用者退出或未找到，返回 None。
    """
    if data_categories is None: # 判斷是否為關鍵字搜尋模式
        while True: # 保持此處的迴圈，因為關鍵字輸入可能需要重複
            keyword_raw = input(prompt_message).strip()
            if keyword_raw.lower() == 'q':
                return None, None
            
            keyword = _clean_string(keyword_raw) # 清理使用者輸入的關鍵字

            if not keyword:
                logger.warning("清理後的關鍵字不能為空，請重新輸入。")
                continue

            # 直接從快取中搜尋
            exact_matches = []
            fuzzy_matches = []

            global_data = get_global_data()
            for category_table_id, metadata_record in global_data.items():
                title = metadata_record.get("標題", "")
                data_category = metadata_record.get("資料分類", "")

                # 精確匹配
                if title == keyword or str(category_table_id) == keyword:
                    exact_matches.append((category_table_id, title, data_category))
                # 模糊匹配
                elif keyword.lower() in title.lower() or keyword.lower() in str(category_table_id) or keyword.lower() in data_category:
                    fuzzy_matches.append((category_table_id, title, data_category))

            if exact_matches:
                selected_dataset_name = exact_matches[0][0]
                logger.notice(f"找到精確匹配資料集: '{exact_matches[0][1]}' (表格名稱: '{selected_dataset_name}')")
                return selected_dataset_name, exact_matches[0][1] # 返回 (category_table_id, title)
            elif fuzzy_matches:
                logger.notice(f"找到多個包含關鍵字 '{keyword}' 的資料集：")
                # 將模糊匹配的資料集轉換為 DataFrame，以便使用 select_row_by_index
                selected_row = select_row_by_index(pd.DataFrame(fuzzy_matches, columns=['category_table_id', '標題', '資料分類'])[['標題', 'category_table_id']], prompt_message)
                if selected_row:
                    selected_dataset_id = selected_row['category_table_id']
                    selected_title = selected_row['標題'] # 獲取標題
                    return selected_dataset_id, selected_title # 返回 (category_table_id, title)
                else:
                    return None, None
            else:
                logger.info(f"沒有找到與關鍵字 '{keyword}' 相關的資料集。")
                continue

    else: # data_categories 不為 None，處理資料分類選擇模式
        df_categories = pd.DataFrame(data_categories, columns=['資料分類'])
        selected_category_row = select_row_by_index(df_categories, prompt_message)
        if not selected_category_row:
            return None, None
        
        selected_category = selected_category_row['資料分類']
        logger.notice(f"您選擇了分類: {selected_category}")

        # 獲取所有資料集清單
        df_full_metadata = _listing_metadata()
        if df_full_metadata.empty or len(df_full_metadata) == 0:
            logger.warning("沒有資料集清單可供選擇。")
            return None, None

        # 從完整的 metadata 中篩選出屬於該分類的資料集
        df_datasets_in_category = df_full_metadata[
            (df_full_metadata['資料分類'] == selected_category)
        ].copy().reset_index(drop=True)

        if df_datasets_in_category.empty or len(df_datasets_in_category) == 0:
            logger.warning(f"在分類 '{selected_category}' 的快取中沒有找到任何資料集。")
            return None, None

        # 根據 '表格id' 欄位進行排序
        df_datasets_in_category = df_datasets_in_category.sort_values(by='表格id').reset_index(drop=True)

        selected_dataset_row = select_row_by_index(df_datasets_in_category, prompt_message)
        if selected_dataset_row:
            selected_table_id = selected_dataset_row['表格id']
            selected_title = selected_dataset_row['標題'] # 獲取標題
            return selected_table_id, selected_title # 返回 (category_table_id, title)
        else:
            return None, None

def _delete_dataset_by_list():
    """
    顯示所有資料集列表，允許使用者根據數字編號選擇並刪除資料集（資料表格及其 metadata 記錄）。
    """
    if not _ensure_db_connection():
        logger.error("無法連接到資料庫，請檢查配置。")
        return
    df_display = _listing_metadata()
    selected_row_dict = select_row_by_index(df_display, "請輸入序號選擇要刪除的資料集 (q 退出): ")
    if selected_row_dict:
        selected_table_id = selected_row_dict.get('表格id')
        selected_title = selected_row_dict.get('標題')
        if selected_table_id and selected_title:
            if _perform_dataset_deletion(selected_table_id, selected_title):
                logger.success(f"資料集 '{selected_title}' 刪除成功。")
            else:
                logger.error(f"資料集 '{selected_title}' 刪除失敗。")
        else:
            logger.warning("無法從選定的資料集中獲取有效的標識符。")
    else:
        logger.info("使用者取消刪除操作。")
        
def _delete_dataset_by_category():
    """
    允許使用者根據資料分類選擇並刪除資料集（資料表格及其 metadata 記錄）。
    """
    data_categories = [
        '安全飲食', '地理圖資', '農業旅遊', '農糧', '漁業', '畜牧', '農民輔導',
        '農業金融', '動植物防疫檢疫', '水土保持', '農村再生', '造林生產', '森林經營',
        '農業科技', '主計', '農業法規', '農田水利', '其他', '農業氣象', '動物保護'
    ]

    selected_table_id, selected_title = _search_and_select_dataset('請輸入序號選擇要刪除的資料集 (q 退出): ', data_categories=data_categories)
    if selected_table_id and selected_title:
        if _perform_dataset_deletion(selected_table_id, selected_title):
            logger.success(f"資料集 '{selected_title}' 刪除成功。")
        else:
            logger.error(f"資料集 '{selected_title}' 刪除失敗。")
    else:
        logger.info("使用者取消刪除操作。")
        return

def _delete_dataset_by_keyword():
    """
    允許使用者根據關鍵字搜尋並刪除資料集（資料表格及其 metadata 記錄）。
    搜尋條件包括：標題完全符合、category_table_id 完全符合、標題包含關鍵字。
    """
    selected_table_id, selected_title = _search_and_select_dataset('請輸入要搜尋的關鍵字 (q 退出): ')
    if selected_table_id and selected_title:
        if _perform_dataset_deletion(selected_table_id, selected_title):
            logger.success(f"資料集 '{selected_title}' 刪除成功。")
        else:
            logger.error(f"資料集 '{selected_title}' 刪除失敗。")
    else:
        logger.info("使用者取消刪除操作。")

def _delete_non_existent_metadata_entries(df: pd.DataFrame):
    """
    根據提供的 DataFrame，刪除 metadata_index 中存在但資料庫中不存在的資料集簡介。
    """
    if not _ensure_db_connection():
        logger.error("無法連接到資料庫，請檢查配置。")
    # 過濾出 '存於metadata_index' 為 True 且 '存於資料庫' 和 '處理後存於資料庫' 都為 False 的資料
    to_delete_df = df[(df['存於metadata_index'] == True) & (df['存於資料庫'] == False) & (df['處理後存於資料庫'] == False)]

    if to_delete_df.empty or len(to_delete_df) == 0:
        logger.notice("沒有需要刪除的資料集簡介。")

    print("\n--- 可以刪除的資料集簡介 ---")
    display_dataframe(to_delete_df[['標題', '表格id']], "待刪除資料集簡介", sort_columns=['表格id'])
    logger.debug(to_delete_df)
    if to_delete_df.empty or len(to_delete_df) == 0:
        return
    if not yes_no_menu("您確定要刪除這些資料集簡介嗎？這將無法恢復！"):
        logger.info("使用者取消刪除操作。")
    try:
        for _, row in to_delete_df.iterrows():
            original_table_name = row['表格id'] # 將 '資料集名稱' 替換為 '表格id'            
            if delete_table_and_metadata_entry(original_table_name, delete_table=False):
                logger.success(f"資料集 '{original_table_name}' 刪除成功。")
            else:
                logger.error(f"資料集 '{original_table_name}' 刪除失敗。")
    except Exception as e:
        logger.error(f"刪除資料集簡介時發生錯誤: {e}")

def _rename_dataset_table():
    """
    允許使用者選擇一個資料集並重新命名其資料庫表格和 metadata_index 中的相關條目。
    """
    print("\n--- 重新命名資料集 ---")
    selected_original_dataset_name, selected_title = _search_and_select_dataset('請輸入要重新命名的資料集關鍵字 (q 退出): ')
    if selected_original_dataset_name:
        # selected_original_dataset_name 現在是 category_table_id
        old_category_table_id = selected_original_dataset_name        
        new_category_table_id = input(f"請輸入 '{old_category_table_id}' 的新 ID (q 退出): ").strip()
        if new_category_table_id.lower() == 'q':
            logger.info("使用者取消重新命名操作。")
            return
        if not new_category_table_id:
            logger.warning("新 ID 不能為空。")
            return
        if new_category_table_id == old_category_table_id:
            logger.warning("新 ID 與舊 ID 相同，無需重新命名。")
            return
        if not yes_no_menu(f"您確定要將 '{old_category_table_id}' 重新命名為 '{new_category_table_id}' 嗎？"):
            logger.info("使用者取消重新命名操作。")
            return
        # 調用 database_manager 中的 rename_data_tables 函數
        if rename_data_tables(old_category_table_id, new_category_table_id):
            logger.success(f"資料集 ID 從 '{old_category_table_id}' 已成功重新命名為 '{new_category_table_id}'。")
        else:
            logger.error(f"重新命名資料集 ID 從 '{old_category_table_id}' 失敗。")
    else:
        logger.info("使用者取消重新命名操作。")

def _match_table_indexes_and_names(should_print: bool = True):
    """
    匹配資料庫表格名稱與 metadata_index 中的原始資料集名稱，並輸出為 CSV 檔案。
    """
    current_file_dir = os.path.dirname(os.path.abspath(__file__)) # 獲取當前檔案的目錄
    if not _ensure_db_connection():
        logger.error("無法連接到資料庫，請檢查配置。")
        return
    global_data = get_global_data()
    if not global_data:
        logger.error("未能成功從元資料表格獲取資料。")
        return # 如果沒有 metadata，則無法進行匹配

    try:
        # 1. 從 get_global_data() 獲取相關資訊
        metadata_info = {record['category_table_id']: {'標題': record['標題']}
                         for record in global_data.values() if 'category_table_id' in record}
        
        # 2. 獲取所有資料庫表格名稱 (這部分仍然需要查詢資料庫來確認實際存在的表格)
        sql_query_tables = "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
        all_db_tables_raw = _execute_sql(sql_query_tables, fetch_all=True)
        
        db_tables_set = set()
        if all_db_tables_raw:
            for table in all_db_tables_raw:
                if table['tablename'] != 'metadata_index':
                    db_tables_set.add(table['tablename'])
        if should_print:
            logger.notice(f"已從資料庫獲取 {len(db_tables_set)} 個表格名稱。")
        # --- 生成並列印「表1」 ---
        table1_data = []
        for category_id, info in metadata_info.items():
            table1_data.append({
                '標題': info['標題'],
                '表格id': category_id,
                '存於metadata_index': True
            })
        df_table1 = pd.DataFrame(table1_data, columns=['標題', '表格id', '存於metadata_index'])
        if should_print:
            display_dataframe(df_table1, "表1 (metadata_index 中的資料集簡介)", sort_columns=['表格id'])
            print("按 Enter 繼續...")
            input()
        # --- 生成並列印「表2」 (原始資料集) ---
        table2_data = []
        for db_table_name in db_tables_set:
            if '_processed' not in db_table_name:
                table2_data.append({
                    '表格id': db_table_name,
                    '存於資料庫': True
                })
        df_table2 = pd.DataFrame(table2_data, columns=['表格id', '存於資料庫'])
        if should_print:
            display_dataframe(df_table2, "表2 (資料庫中存在的原始資料集)", sort_columns=['表格id'])
            print("按 Enter 繼續...")
            input()
        # --- 生成並列印「表3」 (處理後資料集) ---
        table3_data = []
        processed_tables_in_db = set()
        for db_table_name in db_tables_set:
            if '_processed' in db_table_name:
                category_id_from_processed = db_table_name.replace('_processed', '')
                table3_data.append({
                    '表格id': category_id_from_processed,
                    '處理後資料集名稱': db_table_name,
                    '處理後存於資料庫': True
                })
                processed_tables_in_db.add(db_table_name)
        df_table3 = pd.DataFrame(table3_data, columns=['表格id', '處理後資料集名稱', '處理後存於資料庫']) # 明確指定欄位
        if should_print:
            display_dataframe(df_table3, "表3 (資料庫中存在的處理後資料集)", sort_columns=['表格id'])
            print("按 Enter 繼續...")
            input()
        # --- 合併資料並生成最終結果 (使用 Pandas merge) ---
        # 以 '表格id' 為鍵，進行外連接
        output_df = pd.merge(df_table1, df_table2, on='表格id', how='outer')
        output_df = pd.merge(output_df, df_table3, on='表格id', how='outer')

        # 處理 NaN 值：
        # '存於metadata_index', '存於資料庫', '處理後存於資料庫' 欄位，如果為 NaN 則表示不存在，應為 False
        pd.set_option('future.no_silent_downcasting', True)
        bool_cols = ['存於metadata_index', '存於資料庫', '處理後存於資料庫']
        for col in bool_cols:
            output_df[col] = output_df[col].fillna(False).astype(bool)
        
        # '標題', '處理後資料集名稱' 欄位，如果為 NaN 則替換為空字串
        output_df['標題'] = output_df['標題'].fillna('')
        output_df['處理後資料集名稱'] = output_df['處理後資料集名稱'].fillna('')
        output_df['表格id'] = output_df['表格id'].fillna('').astype(str) # 確保 '表格id' 為字串並填充 NaN

        # 確保欄位順序正確
        output_df = output_df[[
            '標題', '表格id', '處理後資料集名稱',
            '存於metadata_index', '存於資料庫', '處理後存於資料庫'
        ]]

        # 輸出結果到 CSV 檔案
        output_csv_path = os.path.join(current_file_dir, "metadata", "matching_tables.csv") # 使用絕對路徑
        output_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        if should_print:
            display_dataframe(output_df, "表合併後", sort_columns=['表格id'])
            logger.success(f"比對結果已儲存到 '{output_csv_path}'。")
        _delete_non_existent_metadata_entries(output_df)
    except Exception as e:
        logger.error(f"匹配資料表索引與表格名稱失敗: {e}")

def _display_dataset_content_list(df_table_content: pd.DataFrame, category_table_id: str, columns_for_display: List[str] = None):
    """
    顯示指定資料集的內容清單。
    """
    
    if df_table_content is None or df_table_content.empty:
        logger.warning(f"資料集 '{category_table_id}' 沒有內容可顯示或獲取失敗。")
        return 
    if columns_for_display is not None:
        df_table = pd.DataFrame()
        for col in columns_for_display:
            df_table[col] = df_table_content[col]
    else:
        df_table = df_table_content
    # 獲取資料集標題以用於顯示
    metadatas = get_global_data()
    metadata_df = metadatas.get(category_table_id)
    if metadata_df is None:
        logger.error("未能成功從元資料表格獲取資料。")
    title = metadata_df.get("標題")
    display_dataframe(df_table, f"資料集 '{title}' 內容清單", truncate=False)
    
def _show_dataset(title: str, category_table_id: str):
    if not yes_no_menu(f"是否顯示資料集 '{title}' 的內容清單？"):
        return
    logger.debug(f"要給予get_table_columns的參數是:{category_table_id}")
    columns = get_table_columns(category_table_id)
    if not columns:
        return
    columns_for_display = get_comparison_columns(columns)
    df_table_content = get_dataset_content_for_list(category_table_id)
    if columns_for_display:
        df_table = pd.DataFrame()
        for col in columns_for_display:
            df_table[col] = df_table_content[col]
    else:
        df_table = df_table_content
    _display_dataset_content_list(df_table, category_table_id, columns_for_display)
    if not yes_no_menu("是否顯示個別欄位內容？"):
        return
    print(f"\n--- 顯示資料集 '{title}' 個別資料內容 ---")
    for index, row_series in df_table.iterrows():
        row_table = pd.DataFrame([row_series], columns=df_table.columns)
        logger.debug(row_table)
        display_dataframe(row_table, f"顯示第{index + 1}筆資料: ", transpose=True, truncate=False)
        is_continue = input("按 enter 繼續... (q 退出): ")
        if is_continue.lower() == 'q':
            break

def _delete_dataset_option():
    """刪除資料集的選單
    """

    while True:
        print("\n--- 刪除資料集選項 ---")
        print("1. 顯示所有資料集列表來刪除資料集")
        print("2. 根據資料分類刪除資料集")
        print("3. 根據關鍵字刪除資料集")
        print("q. 退出")
        delete_choice = input("請輸入您的選擇 (1-3, q): ")

        if delete_choice == '1':
            _delete_dataset_by_list()
        elif delete_choice == '2':
            _delete_dataset_by_category()
        elif delete_choice == '3':
            _delete_dataset_by_keyword()
        elif delete_choice.lower() == 'q':
            logger.info("退出刪除操作選單。")
            break

def _delete_replicate_data(preview: bool = False):
    """
    自動逐一刪除所有資料集內重複資料。
    """
    if not _ensure_db_connection():
        logger.error("無法連接到資料庫，請檢查配置。")
        return
    print("\n--- 刪除資料集內重複資料 ---")
    # if yes_no_menu("即將刪除所有資料集內重複資料，將會花費大量時間，是否繼續？"):    
    minor_info = get_minor_info_data()
    all_sort_configs: list[dict] = minor_info.get("all_sort_configs", [])
    if all_sort_configs is None or not isinstance(all_sort_configs, list):
        all_sort_configs = []
    df_display = _listing_metadata()
    table_id_list = df_display['表格id'].tolist()
    dataset_names = df_display['標題'].tolist()
    for table_id, title in zip(table_id_list, dataset_names):
        if "代碼" in title:
            logger.success(f"表格<{title}>{table_id} 不需清理重複資料。")
            continue
    # table_id = "1040001" # 測試用
    # title = "全國單日平均糧價" # 測試用
    # 尋找與當前 title 匹配的排序設定
        sort_keys = next((c.get("sort_keys") for c in all_sort_configs if c.get("file_name") == title), None)
        if not sort_keys:
            logger.warning(f"找不到表格 {title} {table_id} 的排序/比對設定，跳過。")
            continue
        if isinstance(sort_keys, str):
            sort_keys = json.loads(sort_keys)
        print(f"--- 開始處理表格 {title} {table_id} 的重複資料 ---")
        comparison_columns = [column[0] for column in sort_keys if column[0]]
        cols_str = ", ".join([f'"{c}"' for c in comparison_columns])
        if preview:
            # --- 預覽模式：計算重複總數 ---
            stats_sql = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT(CASE WHEN row_num > 1 THEN 1 END) as dup_count
            FROM (
                SELECT ROW_NUMBER() OVER (PARTITION BY {cols_str} ORDER BY category_table_data_id ASC) as row_num
                FROM "{table_id}"
            ) t;
            """
            stats_res = _execute_sql(stats_sql, fetch_one=True)
            total_count = stats_res[0] if stats_res else 0
            dup_count = stats_res[1] if stats_res else 0
            
            if dup_count > 0:
                logger.info(f"表格 <{title}> ({table_id}) 檢測到 {dup_count} / {total_count} 筆重複資料。")
                
                # (選配) 查看前 10 筆重複的內容
                sample_sql = f"""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {cols_str} ORDER BY category_table_data_id ASC) as row_num
                    FROM "{table_id}"
                ) t WHERE t.row_num > 1 LIMIT 10;
                """
                samples = _execute_sql(sample_sql, fetch_all=True)
                if samples:
                    df_samples = pd.DataFrame(samples)
                    
                    # 1. 處理 ID：改名並只取最後 7 位
                    if 'category_table_data_id' in df_samples.columns:
                        df_samples['category_table_data_id'] = df_samples['category_table_data_id'].astype(str).str[-7:]
                        df_samples = df_samples.rename(columns={'category_table_data_id': 'id'})
                    
                    # 2. 移除不要顯示的欄位
                    cols_to_drop = ['版本', 'created_at', 'last_updated_at', 'foreign_key']
                    # 確保欄位存在才刪除，避免報錯
                    existing_drop_cols = [c for c in cols_to_drop if c in df_samples.columns]
                    df_samples = df_samples.drop(columns=existing_drop_cols)
                    sort_df = df_samples.sort_values(by="id")
                    
                    # print(f"重複樣本預覽：\n{sort_df}")
            else:
                logger.success(f"表格<{title}>{table_id} 無重複資料。")
        else:
            # --- 執行模式：正式刪除 ---
            delete_sql = f"""
            DELETE FROM "{table_id}"
            WHERE ctid IN (
                SELECT ctid FROM (
                    SELECT ctid, ROW_NUMBER() OVER (PARTITION BY {cols_str} ORDER BY category_table_data_id ASC) as row_num
                    FROM "{table_id}"
                ) t WHERE t.row_num > 1
            );
            """
            logger.warning(f"正在執行刪除表格<{title}>的重複資料...")
            input("按 Enter 開始清理重複資料...")
            _execute_sql(delete_sql)
            logger.success(f"表格<{title}>重複資料清理完成。")
        # input("按 Enter 繼續下一個表格...")
        
    
def operations_of_postgresql():
    """
    提供資料庫操作的入口點，包含獲取 metadata 清單和查詢指定 metadata 的功能。
    """    
    print("歡迎使用農業開放資料平台資料庫工具！測試中，資訊自動填入預設值。")
    global_data = get_global_data()
    _match_table_indexes_and_names(should_print=False)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 120)
    pd.set_option('display.max_colwidth', 60)
    pd.set_option('display.colheader_justify', 'left')    
    print("\n--- PostgreSQL 資料庫操作 ---")
    while True:
        print("\n請選擇操作：")
        print("0. 連線資料庫")
        print("1. 顯示所有資料集清單 (包含 ID 和標題)，查看資料集簡介")
        print("2. 查詢並顯示特定資料集簡介")
        print("3. 重新命名資料集")
        print("4. 刪除資料集")
        print("5. 匹配資料表索引與表格名稱")
        print("6. 刪除資料集內重複資料")
        print("7. 建立資料集索引（加速資料庫作業速度）")
        print("q. 退出")
        choice = str(input("請輸入您的選擇 (1-7,q): "))
        if choice.lower() == 'q':
            logger.info("退出資料庫操作。")
            try:
                DB.close() # 關閉資料庫連線
                logger.info("資料庫連線已關閉。")
            except:
                pass
            return
        elif choice == '0':
            connect_db(USERNAME, PASSWORD, DBNAME)
        elif choice == '1':
            _get_and_display_metadata_list()
        elif choice == '2':
            _search_and_display_datasets()
        elif choice == '3':
            _rename_dataset_table()
        elif choice == '4':
            _delete_dataset_option()
        elif choice == '5':
            _match_table_indexes_and_names()
        elif choice == '6':
            _delete_replicate_data()
        elif choice == '7':
            create_indexes_for_all_tables()
        else:
            break


if __name__ == "__main__":
    load_dotenv() # 在程式啟動時載入 .env 檔案
    config = dotenv_values() # 讀取 .env 檔案中的值
    USERNAME = config.get("USERNAME")
    PASSWORD = config.get("PASSWORD")
    DBNAME = config.get("DBNAME")
    DB = connect_db(USERNAME, PASSWORD, DBNAME)
    setup_logging(15)
    operations_of_postgresql()
