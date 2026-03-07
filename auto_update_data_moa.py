from menu_utils import make_AUTO_YES, yes_no_menu
from main import update_by_metadata, init
from database_manager import connect_db, create_empty_table_unexistent
from database_manager import create_files_table_if_not_exists, create_record_files_table_if_not_exists
from dotenv import load_dotenv, dotenv_values # 導入 load_dotenv 和 dotenv_values
from logs_handle import setup_logging, logger # 確保導入 logger
import pandas as pd

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

def main():
    print("歡迎使用農業開放資料平台資料庫工具！測試中，資訊自動填入預設值。")
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 120)
    pd.set_option('display.max_colwidth', 60)
    pd.set_option('display.colheader_justify', 'left')
    
    make_AUTO_YES()
    yes_no_menu("是否開始自動更新？")
    update_by_metadata()

if __name__ == '__main__':
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
    # global_metadata_cache = get_global_data()
    # minor_info = get_minor_info_data()
    main()
    try:
        DB.close() # 關閉資料庫連線
        logger.info("資料庫連線已關閉。")
    except:
        pass