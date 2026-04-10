import json
import os, re
from datetime import datetime
from typing import List, Dict, Any, Optional
from logs_handle import logger
from menu_utils import yes_no_menu

def delete_metadata_entry_from_json(metadata_json_path: str, category_table_id: str) -> bool:
    """
    從指定的 metadata.json 檔案中刪除與 category_table_id 相關的 metadata 條目。

    Args:
        metadata_json_path (str): metadata.json 檔案的路徑。
        category_table_id (str): 要刪除的資料集的 category_table_id。

    Returns:
        bool: 如果成功刪除或未找到需要刪除的條目則返回 True，否則返回 False。
    """
    if os.path.exists(metadata_json_path):
        try:
            with open(metadata_json_path, 'r', encoding='utf-8') as f:
                metadata_list: List[Dict[str, Any]] = json.load(f)
            
            initial_count = len(metadata_list)
            updated_metadata_list = [
                item for item in metadata_list
                if item.get("category_table_id") != category_table_id
            ]

            if len(updated_metadata_list) < initial_count:
                with open(metadata_json_path, 'w', encoding='utf-8') as f:
                    json.dump(updated_metadata_list, f, ensure_ascii=False, indent=4)
                logger.notice(f"已從 '{metadata_json_path}' 中刪除與 '{category_table_id}' 相關的 metadata 條目。")
                return True
            else:
                logger.notice(f"在 '{metadata_json_path}' 中沒有找到與 '{category_table_id}' 相關的 metadata 條目。")
                return True # 即使沒有找到，也視為成功，因為目標是確保條目不存在

        except FileNotFoundError:
            logger.error(f"檔案 '{metadata_json_path}' 不存在。")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"解析 '{metadata_json_path}' 失敗: {e}")
            return False
        except Exception as e:
            logger.error(f"處理 '{metadata_json_path}' 時發生錯誤: {e}")
            return False
    else:
        logger.notice(f"檔案 '{metadata_json_path}' 不存在，跳過處理。")
        return True # 檔案不存在，視為成功，因為目標是確保條目不存在

def load_json_data(file_path: str):
    """
    從指定 JSON 檔案讀取資料。
    Args:
        file_path (str): JSON 檔案的路徑。
    Returns:
        list[dict] | None: 解析後的字典列表或是字典，如果發生錯誤則返回 None。
    """
    try:
        file_path = os.path.normpath(file_path)
    except:
        pass
    if not os.path.exists(file_path):
        logger.error(f"檔案 '{file_path}' 不存在。")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"解析 JSON 檔案 '{file_path}' 時發生錯誤: {e}")
        return None
    except Exception as e:
        logger.error(f"讀取檔案 '{file_path}' 時發生錯誤: {e}")
        return None

    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        if not isinstance(data, dict):
            logger.error(f"檔案 '{file_path}' 中的資料格式不符合預期 (字典列表或單一字典)。")
            return None
    
    return data

def save_json_data(data: list, file_path: str) -> str:
    """
    將 JSON 資料儲存到指定路徑。
    Args:
        data (list): 要儲存的 JSON 資料。
        file_path (str): 儲存的路徑。
    Returns:
        str: 儲存後的檔案路徑。
    """
    # 如果 file_path 包含目錄，則創建目錄
    dir_name = os.path.dirname(file_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    return file_path

def search_metadata_from_json(search_string: str) -> list:
    """
    從 metadata/metadata.json 檔案中搜索符合條件的資料。
    搜索條件為 '標題' 或 '原始資料集名稱' 包含 search_string。
    """
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_file_dir, 'metadata', 'metadata.json')
    results = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
            for item in metadata:
                if search_string.lower() in str(item).lower():
                    results.append(item)
    except FileNotFoundError:
        logger.error(f"錯誤：檔案 {file_path} 不存在。")
    except json.JSONDecodeError:
        logger.error(f"錯誤：檔案 {file_path} 不是有效的 JSON 格式。")
    return results

def update_local_metadata_file(new_metadata: dict) -> bool:
    """
    取代或新增 metadata 到 metadata.json 檔案.
    """
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    metadata_list = []
    metadata_file_path = os.path.join(current_file_dir, "metadata", "metadata.json")
    if os.path.exists(metadata_file_path):
        metadata_list = load_json_data(metadata_file_path)
        if metadata_list is None:
            metadata_list = [] # 如果檔案損壞或為空,則初始化為空列表

    found_match = False
    category_table_id = new_metadata.get("category_table_id")
    if category_table_id:
        for item in metadata_list:
            if item.get("category_table_id") == category_table_id:
                logger.notice("已找到並取代原有資料。")
                item = new_metadata
                found_match = True
                break
    
    if not found_match:
        metadata_list.append(new_metadata)
        logger.notice("未找到資料，已新增新的資料。")
    
    if save_json_data(metadata_list, metadata_file_path):
        return True
    else:
        logger.error(f"更新 '{new_metadata.get('標題')}' 的 Metadata 到 metadata.json 時發生錯誤。")
    return False

def load_interruption_info_and_prompt_restore() -> Optional[tuple[list, dict]]:
    """
    檢查 hand_download/ 目錄中是否存在中斷資訊檔案，並在找到時詢問使用者是否要還原這些資訊。

    函式功能說明:
    此函式用於檢查 hand_download/ 目錄中是否存在中斷資訊檔案，並在找到時詢問使用者是否要還原這些資訊。
    如果使用者選擇還原，則從中斷資訊中讀取已下載的資料內容並回傳，同時刪除中斷資訊檔案。

    回傳值說明:
    *   如果使用者選擇還原，則回傳一個元組 (restored_data: list, interruption_details: dict)。
        *   restored_data 是從 interruption_details['interruption_record'] 中讀取的 list[dict] 資料。
        *   interruption_details 是從中斷資訊 JSON 檔案中讀取並解析後的字典。
    *   如果沒有可用的中斷資訊，或使用者選擇不還原，則回傳 None。
    """
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    interruption_dir = os.path.join(current_file_dir, "hand_download")
    interruption_pattern = re.compile(r"interruption_record_(\d{8}_\d{6})\.json")
    
    if not os.path.exists(interruption_dir):
        logger.notice(f"中斷資訊目錄 '{interruption_dir}' 不存在。")
        return None

    latest_record = None
    latest_timestamp = None

    for filename in os.listdir(interruption_dir):
        match = interruption_pattern.match(filename)
        if match:
            timestamp_str = match.group(1)
            try:
                current_timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                if latest_timestamp is None or current_timestamp > latest_timestamp:
                    latest_timestamp = current_timestamp
                    latest_record = filename
            except ValueError:
                logger.warning(f"無效的時間戳記格式在檔案 '{filename}' 中。")
                continue

    if not latest_record:
        logger.notice("未找到任何中斷資訊檔案。")
        return None

    record_path = os.path.join(interruption_dir, latest_record)
    interruption_info = load_json_data(record_path)
    if interruption_info is None:
        logger.error(f"讀取中斷資訊檔案 '{record_path}' 失敗。")
        return None
    
    print("中斷資訊摘要:")
    print(f"  日期: {interruption_info.get('date', 'N/A')}")
    print(f"  名稱: {interruption_info.get('name', 'N/A')}")
    print(f"  URL: {interruption_info.get('url', 'N/A')}")
    
    if not yes_no_menu("是否要還原上次中斷的下載？"):
        logger.info("使用者選擇不還原中斷的下載。")
        return None

    restored_data = interruption_info.get('interruption_record')
    if restored_data is None:
        logger.error("中斷資訊中未包含 'interruption_record'，無法還原資料。")
        return None
    
    if not isinstance(restored_data, list):
         logger.error("中斷資訊中的 'interruption_record' 格式不正確，無法還原資料。")
         return None

    logger.info(f"已成功讀取中斷資訊中的資料內容。")

    # 成功讀取中斷資訊後，刪除檔案
    try:
        if os.path.exists(record_path):
            os.remove(record_path)
            logger.notice(f"已刪除中斷資訊檔案: '{record_path}'")
    except OSError as e:
        logger.error(f"刪除中斷資訊檔案時發生錯誤: {e}")
        # 即使刪除失敗，如果資料已讀取，仍嘗試回傳
        return (restored_data, interruption_info)

    return (restored_data, interruption_info)

def save_interruption_info(downloaded_data_content: list, interruption_details: dict):
    """
    在 download_and_deduplicate_data 函式中途失敗時，將已下載的資料內容（list[dict] 格式）
    作為新的暫存檔儲存，並儲存中斷資訊。

    Args:
        downloaded_data_content (list): while skip >= 0: 迴圈中已下載的資料內容，其格式為 list[dict]。
        interruption_details (dict): 包含中斷資訊的字典，預期包含 date, name, url, false_at_skip 鍵值。
    """
    from datetime import datetime # 確保 datetime 模組已導入

    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_file_dir, "hand_download")
    os.makedirs(output_dir, exist_ok=True) # 確保目錄存在

    # 1. 生成時間戳記
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not downloaded_data_content:
        return None
    # 2. 處理中斷資訊
    interruption_details["interruption_record"] = downloaded_data_content
    interruption_record_filename = f"interruption_record_{timestamp}.json"
    interruption_record_path = os.path.join(output_dir, interruption_record_filename)
    if save_json_data(interruption_details, interruption_record_path):
        logger.info(f"已將中斷資訊儲存至檔案: '{interruption_record_path}'")
        return True
    else:
        logger.error(f"儲存中斷資訊檔 '{interruption_record_path}' 時發生錯誤。")
        return False


def load_minor_info() -> dict[list[dict]]:
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    minor_info_path = os.path.join(current_file_dir, "metadata", "minor_info.json")
    return load_json_data(minor_info_path)

def save_minor_info(data: dict[list[dict]]) -> str:
    """
    將 minor_info 資料儲存到 metadata/minor_info.json 檔案。

    Args:
        data (list | dict): 要儲存的資料，格式為 list 或 dict。

    Returns:
        str: 儲存後的檔案路徑。
    """
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    minor_info_path = os.path.join(current_file_dir, "metadata", "minor_info.json")
    return save_json_data(data, minor_info_path)
