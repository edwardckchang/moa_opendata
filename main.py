from json_file_operations import update_local_metadata_file, save_minor_info # 導入 update_local_metadata_file 函數
from dotenv import load_dotenv, dotenv_values # 導入 load_dotenv 和 dotenv_values
from database_manager import connect_db, save_dataframe_to_postgresql, create_empty_table_unexistent, insert_or_update_metadata
from database_manager import check_metadata_update_status,check_dataset_content_update_status, get_global_data, get_minor_info_data
from database_manager import create_files_table_if_not_exists, create_record_files_table_if_not_exists, save_minor_info_to_sql
from database_manager import _execute_sql, _table_exists
from data_parser import parse_webpage_to_metadata, fetch_and_process_json_data
from data_parser import check_metadata_integrity, download_data, detect_optimal_skip
from utils import select_row_by_index
from menu_utils import yes_no_menu, make_AUTO_YES, disable_auto_confirm
from operations_of_postgresql import operations_of_postgresql
from logs_handle import setup_logging, logger # 確保導入 logger
import pandas as pd
# from function_call_map import draw_fuction_call_map
import time

def fully_auto_update(auto_input_list, autodetect_skip= False, level = 20):
    global minor_info
    make_AUTO_YES()
    setup_logging(level)
    execution_records = {}
    for auto_input in auto_input_list:
        title = auto_input.get("標題")
        logger.info(f"開始 '{title}' 的自動更新...")
        skip = auto_input.get("skip")
        webpage_url = auto_input.get("連結")
        user_inputs = {"url": webpage_url, "skip": skip}
        metadata, minor_info = _handle_data_download(webpage_url, user_inputs, autodetect_skip)
        try:
            if metadata:
                logger.success(f"'{title}' 的自動更新完成。")
                execution_records[title] = "已更新"
            else:
                logger.warning(f"'{title}' 本次未更新。")
                execution_records[title] = "未更新"                            
        except Exception as e:
            logger.error(f"'{title}' 自動更新時發生錯誤: {e}")
            execution_records[title] = "更新失敗"             

        logger.notice("等待5秒，進行下一個資料集的更新...")
        time.sleep(1)                    
    save_minor_info(minor_info)
    if not save_minor_info_to_sql(minor_info):
        logger.error("未能成功儲存次要資訊到資料庫。")
    logger.execution(f"更新結果:{execution_records}")
    disable_auto_confirm()
    return
    
def _handle_data_download(webpage_url: str, user_inputs: dict, autodetect_skip = False, data_len = None):
    # 確保 minor_info 在正確處理完後是修改過的
    global minor_info
    if not webpage_url:
        logger.error("網頁URL不能為空，終止處理。")
        return False, minor_info
    logger.notice(f"正在解析網頁: {webpage_url}")
    metadata = parse_webpage_to_metadata(webpage_url)   # 需要在不影響其
    if metadata:
        page_title = metadata.get("標題")
        json_url = metadata.get("資料介接")
        if not check_metadata_integrity(metadata, webpage_url):
            logger.error(f"未成功提取網頁 '{webpage_url} Metadata。")
            return False, minor_info
        else:
            logger.notice(f"成功提取 '{page_title}' 的網頁 Metadata。")
    else:
        logger.error(f"未成功提取網頁 '{webpage_url}' Metadata。")
        return False, minor_info
    update_status, category_table_id = check_metadata_update_status(metadata)
    auto_input = next((item for item in minor_info['refer_skip_value'] if item.get("標題") == page_title), {})
    if not auto_input:
        auto_input = {
        "標題": page_title,
        "連結": webpage_url,
        "category_table_id": category_table_id}
    if data_len is None:
        data_len = auto_input.get("資料筆數", None)
        if not data_len:
            data_len = get_count(category_table_id)
            auto_input["資料筆數"] = data_len
    if not update_status: # None 表示無需更新
        logger.notice(f"資料集 '{page_title}' 內容無需更新，終止下載。")        
        return None, minor_info
    data_filter = True
    if autodetect_skip:
        data_filter = False
        terminal_skip = detect_optimal_skip(json_url, page_title, data_len)
        if not terminal_skip:
            logger.notice("沒有成功取得末端資料值")
            return False, minor_info        
        input(f"<{page_title}>使用偵測到的最末端資料值: {terminal_skip}")
        auto_input["skip"] = terminal_skip
        user_inputs["skip"] = terminal_skip
    else:
        skip = auto_input.get("skip", None)
        if skip is None:
            while True:
                skip = str(input("請輸入適當的skip值，基礎為0，資料筆數常大於5000則設5000："))
                if skip.lower() == "q":
                    return False, minor_info                
                try:
                    int_skip = int(skip)
                except:
                    continue
                if int_skip >= 0:
                    break
        auto_input["skip"] = skip
        user_inputs["skip"] = skip
    # 如果需要更新或新增，category_table_id 已經是正確的值
    metadata["category_table_id"] = category_table_id
    logger.notice(f"當前的資料集id為: {category_table_id}")
    if not update_local_metadata_file(metadata):
        logger.notice("metadata json存檔失敗")
        return False, minor_info
    else:
        logger.notice(f"'{page_title}' 的 Metadata 已更新到 metadata.json。")
    logger.notice("\n提取的 Metadata:")
    for key, value in metadata.items():
        logger.notice(f"{key}: {value}")    
    update_date = metadata.get("資料更新日期")
    auto_input["資料更新日期"] = update_date
    user_inputs["url"] = json_url
    # json_data_raw = download_and_deduplicate_data(page_title, user_inputs)
    json_data_new, json_data_raw, sort_keys_used, success_download_data = download_data(page_title, user_inputs, data_filter)
    if sort_keys_used:
        for config in minor_info.get("all_sort_configs", []):
            if config.get("file_name") == page_title:
                # 2. 檢查是否有變動，若有則更新
                if config.get("sort_keys") != sort_keys_used:
                    config["sort_keys"] = sort_keys_used
                    logger.info(f"已更新資料集 <{page_title}> 的比對鍵 (sort_keys)。")
                break
    try:
        new_len = len(json_data_new) if json_data_new else 0
        tatol_len = len(json_data_raw) if json_data_raw else 0
        logger.notice(f"已下載、去重並合併資料完成現有資料 {tatol_len} 條，新資料 {new_len} 條。")
    except Exception as e:
        logger.error(f"下載、去重並合併資料時發生錯誤: {e}")            
    if json_data_raw and success_download_data == True:
    # 檢查資料集內容是否需要更新，農作物代碼與地理圖資不需要要檢查，而是全覆蓋
        if "分布圖Url" in json_data_raw[0] and json_data_raw[0]["分布圖Url"]:
            records_to_insert = json_data_raw
            logger.notice(f"需要插入 {len(records_to_insert)} 條記錄。")
        elif "代碼" in page_title:
            records_to_insert = json_data_raw
            logger.notice(f"需要插入 {len(records_to_insert)} 條記錄。")
        else:
            comparison_columns = [column[0] for column in sort_keys_used if column[0]]
            logger.notice(f"排序用的欄位: {comparison_columns}")
            records_to_insert = check_dataset_content_update_status(page_title, comparison_columns, json_data_raw, category_table_id, data_filter)
            logger.notice(f"需要插入 {len(records_to_insert)} 條記錄。")
    elif success_download_data == True:
        records_to_insert = [] # 有成功下載資料但被清空
    else:
        logger.error("未成功下載 JSON 資料。")
        return False, minor_info
    if not records_to_insert:
        logger.info(f"資料集 '{page_title}' 內容無需更新，終止下載。")
        if not insert_or_update_metadata(metadata): # 可能有日期變動，僅更新 metadata    
            return None, minor_info
        else:
            logger.success(f"插入或更新 '{page_title}' 的 Metadata 成功。")
        return metadata, minor_info
    downloaded_json_data, downloaded_json_path = fetch_and_process_json_data(records_to_insert, page_title, category_table_id)
    # 確保原始資料表格存在
    if downloaded_json_data: # 確保 downloaded_json_data 不為空
        # 從 downloaded_json_data 推斷 schema
        raw_data_schema = {k: "text" for k in downloaded_json_data[0].keys()}
        create_empty_table_unexistent(raw_data_schema, category_table_id)
    else:
        logger.warning(f"下載的 '{page_title}' JSON 資料為空，無法建立原始資料表格。")
        return None, minor_info
    # 將需要插入的記錄存入資料庫
    if downloaded_json_data:
        if not insert_or_update_metadata(metadata): # 將 metadata 存入資料庫並獲取更新後的 metadata        
            return None, minor_info
        logger.success(f"插入或更新 '{page_title}' 的 Metadata 成功。")
        if not save_dataframe_to_postgresql(downloaded_json_data, category_table_id, page_title):            
            return None, minor_info
        logger.logs(f"已成功批量插入 '{page_title}' 的總計 {len(downloaded_json_data)} 條記錄到表格 '{category_table_id}' 。")
    data_len = get_count(category_table_id)
    auto_input["資料筆數"] = data_len
    minor_info['refer_skip_value'] = [item for item in minor_info['refer_skip_value'] if item.get("標題") != page_title]
    minor_info['refer_skip_value'].append(auto_input)
    return metadata, minor_info

def handle_data_download_by_user_setting():
    """
    根據使用者選擇處理資料的完全下載和儲存。
    """
    global minor_info
    user_inputs = {"url":"", "skip":""}
    while True:
        print("\n請選擇操作：")
        print("1. 輸入資料集介紹頁網址")
        print("2. 選擇資料集")
        print('3. 重新下載所有資料集')
        print("q. 退出")
        choice = str(input("請輸入您的選擇："))
        if choice.lower() == "q":
            return
        elif choice == "1":
            while True:
                page_link = input("請輸入要下載的目標資料集介紹頁網址（非下載網址）：")
                if page_link.lower() == "q":
                    return
                elif "http" in page_link:
                    break
                elif "http" not in page_link:
                    print("不是有效的網址，請重新輸入。")
        elif choice == "2":
            select_row = metadata_selection()
            if select_row:        
                table_id = select_row.get('表格ID', '')
                a_metadata = global_metadata_cache.get(table_id)
                if not a_metadata:
                    logger.error(f"無法取得資料集ID[{table_id}]的元資料，請檢查資料庫結構。")
                    return
                page_link = a_metadata.get("連結")
                if not page_link:
                    logger.error(f"無法取得資料集ID[{table_id}]的連結，請檢查元資料。")
            else:
                print("沒有選擇任何資料集。")
                return
        elif choice == "3":
            auto_input_list: list[dict] = minor_info.get("refer_skip_value")
            fully_auto_update(auto_input_list, True)
            return
        metadata, minor_info = _handle_data_download(page_link, user_inputs, True)
        title = metadata.get("標題", "'無標題' ")
        if metadata:
            logger.success(f"'{title}' 的自動更新完成。")
        else:            
            logger.warning(f"'{title}' 本次未更新。")
        save_minor_info(minor_info)
        if not save_minor_info_to_sql(minor_info):
            logger.error("未能成功儲存次要資訊到資料庫。")

def get_value_from_minorinfo(subject_to_get, title_to_get, key_for_value):    
    value_to_get = None
    sub_value: list[dict] = minor_info.get(subject_to_get, [])
    if sub_value:
        for vdict in sub_value:
            if vdict.get("標題", "") == title_to_get or vdict.get("file_name", "") == title_to_get:
                value_to_get = vdict.get(key_for_value, None)
    
    return value_to_get

def metadata_selection():
    if not global_metadata_cache:
        logger.error("未能成功讀取元資料。")
        return {}
    list_metadata = list(global_metadata_cache.values())
    pd_metadata = pd.DataFrame(list_metadata)
    # 2. 保留特定欄位並改名
    # 確保欄位存在，避免報錯
    columns_to_keep = ['標題', 'category_table_id']
    pd_metadata = pd_metadata[columns_to_keep].copy()
    pd_metadata.rename(columns={'category_table_id': '表格ID'}, inplace=True)
    # 3. 加入欄位 '資料筆數'，並從資料庫取得
    print("正在統計資料庫各表筆數...")
    pd_metadata['資料筆數'] = pd_metadata['表格ID'].apply(get_count)
    select_row = select_row_by_index(pd_metadata, "請選擇要更新的資料集: ", ['表格ID'])  
    return select_row

def get_count(table_id):
    if _table_exists(table_id):
        # 這裡使用你底層的 SQL 執行工具
        res = _execute_sql(f'SELECT COUNT(*) FROM "{table_id}";', fetch_all=True)
        return res[0]['count'] if res else 0
    return 0

def update_by_metadata():
    """
    從資料庫中獲取元資料來進行資料更新最新資料。
    """
    user_inputs = {"url": ""}
    global minor_info
    if not minor_info:
        logger.error("未能成功讀取次要資訊。")
        return
    auto_input_list: list[dict] = minor_info.get("refer_skip_value")
    if auto_input_list:
        if yes_no_menu("是否開始自動更新？"):
            fully_auto_update(auto_input_list)
        else:
            select_row = metadata_selection()
            if select_row:
                title = select_row.get('標題', '')                
                table_id = select_row.get('表格ID', '')
                a_metadata = global_metadata_cache.get(table_id)
                if not a_metadata:
                    logger.error(f"無法取得資料集ID[{table_id}]的元資料，請檢查資料庫結構。")
                    return
                webpage_url = a_metadata.get("連結")
                if not webpage_url:
                    logger.error(f"無法取得資料集ID[{table_id}]的連結，請檢查元資料。")
                    return
            else:
                print("沒有選擇任何資料集。")
                return
            try:
                user_inputs["url"] = webpage_url
                skip = get_value_from_minorinfo("refer_skip_value", title, "skip")
                user_inputs["skip"] = skip
                metadata, minor_info = _handle_data_download(webpage_url, user_inputs)
                save_minor_info(minor_info)
                if not save_minor_info_to_sql(minor_info):
                    logger.error("儲存次要資訊到資料庫時發生錯誤。")
                if metadata:
                    logger.success(f"'{title}' 的更新完成。")
                else:
                    logger.info(f"'{title}' 本次未更新。")
            except Exception as e:
                logger.error(f"更新時發生錯誤: {e}")
        
def main():
    """
    主程式入口點，處理使用者互動和功能調用。
    """
    global DB
    print("歡迎使用農業資料開放平台資料庫工具！測試中，資訊自動填入預設值。")
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 120)
    pd.set_option('display.max_colwidth', 60)
    pd.set_option('display.colheader_justify', 'left')
    while True:
        DB = connect_db(USERNAME, PASSWORD, DBNAME)
        print("\n請選擇操作：")
        print("1. 輸入目標網址下載並儲存資料")
        print("2. 更新統計資料")
        print("3. 待審核資料處理")
        print("4. 進入資料庫操作系統")
        print("q. 退出")
        choice = str(input("請輸入您的選擇："))
        if choice.lower() == 'q':
            print("感謝使用，程式即將退出。")
            break
        elif choice == '1':
            handle_data_download_by_user_setting()
        elif choice == '2':
            update_by_metadata()
        elif choice == '3':
            print("此功能還未實現。")
        elif choice == '4':
            operations_of_postgresql()
        DB.close()

metadata = {
    "標題": "",
    "連結": "",
    "資料分類": "",
    "提供單位": "",
    "上架日期": "",
    "更新頻率": "",
    "資料更新日期": "",
    "資料描述": "",
    "原始資料來源": "",
    "資料介接": "",
    "介接說明文件": "",
    "category_table_id" : "",
}
global_metadata_cache: dict[dict] = {}
minor_info: dict[list[dict]] = {}
DB = None
def init(db_connection):
    global global_metadata_cache, minor_info, DB
    DB = db_connection
    global_metadata_cache = get_global_data()
    minor_info = get_minor_info_data()

if __name__ == "__main__":
    load_dotenv() # 在程式啟動時載入 .env 檔案
    config = dotenv_values() # 讀取 .env 檔案中的值
    USERNAME = config.get("USERNAME")
    PASSWORD = config.get("PASSWORD")
    DBNAME = config.get("DBNAME")
    DB = connect_db(USERNAME, PASSWORD, DBNAME)  
    init(DB)
    setup_logging(level=10) # 在程式啟動時配置日誌系統，可以根據需要調整級別
    # 確保 metadata_index 表格存在
    create_empty_table_unexistent(metadata, "metadata_index")
    create_files_table_if_not_exists()
    create_record_files_table_if_not_exists()
    main()
    # draw_fuction_call_map(main, "dot")
    try:
        DB.close() # 關閉資料庫連線
        logger.notice("資料庫連線已關閉。")
    except:
        pass