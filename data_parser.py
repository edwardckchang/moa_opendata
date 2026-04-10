import requests
from bs4 import BeautifulSoup
import json
import os,re,time
from typing import Optional
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
from database_manager import delete_all_data_from_table, preserve_old_data_by_date
from logs_handle import logger
from sort_data_by_date import sort_json_file_interactively
from json_file_operations import save_json_data, load_interruption_info_and_prompt_restore, save_interruption_info
from utils import clean_table_name, remove_duplicates_from_list_of_dicts
from menu_utils import handle_save_menu, yes_no_menu
import pandas as pd

def check_metadata_integrity(metadata: dict, webpage_url: str) -> bool:
    """
    檢查 metadata 的完整性，確保所有關鍵資訊都存在。
    Args:
        metadata (dict): 從網頁解析出的 metadata 字典。
        webpage_url (str): 原始網頁 URL。
    Returns:
        bool: 如果 metadata 完整則為 True，否則為 False。
    """
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    web_update_date = metadata.get("資料更新日期")
    page_title = metadata.get("標題")
    json_url = metadata.get("資料介接")
    data_category = metadata.get("資料分類")

    if not json_url or not page_title or not data_category or not web_update_date:
        logger.error(f"網頁 '{webpage_url}' 的元資料缺乏關鍵資訊包含標題、網頁更新時間、資料分類或API連結，終止下載。")
        return False

    # 檢查 page_title 是否包含 "代碼"
    if page_title and "代碼" in page_title:
        logger.warning(f"偵測到標題 '{page_title}' 包含 '代碼'，將刪除相關json檔案。")
        raw_data_path = os.path.join(current_file_dir, "raw_data", f"{page_title}.json")
        hand_download_path = os.path.join(current_file_dir, "hand_download", f"{page_title}.json")

        # 刪除 raw_data 下的檔案
        if os.path.exists(raw_data_path):
            try:
                os.remove(raw_data_path)
                logger.notice(f"已刪除檔案: '{raw_data_path}'")
            except OSError as e:
                logger.error(f"刪除檔案 '{raw_data_path}' 時發生錯誤: {e}")

        # 刪除 hand_download 下的檔案
        if os.path.exists(hand_download_path):
            try:
                os.remove(hand_download_path)
                logger.notice(f"已刪除檔案: '{hand_download_path}'")
            except OSError as e:
                logger.error(f"刪除檔案 '{hand_download_path}' 時發生錯誤: {e}")

    return True

def _find_and_select_pdf_document(page_title: str):
    """
    根據網頁標題在 raw_data/pdf 目錄下尋找匹配的 PDF 檔案，並詢問使用者是否使用。
    如果找到並使用者同意，則返回檔案路徑；否則返回 None。
    """
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    pdf_directory = os.path.join(current_file_dir, "raw_data", "pdf")
    cleaned_page_title_for_match = clean_table_name(page_title)
    try:
        cleaned_page_title_for_match = cleaned_page_title_for_match.replace("圖", "")
    except:
        pass
    
    if not os.path.exists(pdf_directory):
        logger.error(f"PDF 目錄 '{pdf_directory}' 不存在。程式中斷。")
        return None # 目錄不存在
    pdf_files = [f for f in os.listdir(pdf_directory) if f.endswith('.pdf')]
    for pdf_file in pdf_files:
        cleaned_pdf_filename = os.path.basename(pdf_file)
        if cleaned_page_title_for_match in cleaned_pdf_filename or page_title in cleaned_pdf_filename:
            matched_pdf_path = os.path.join(pdf_directory, pdf_file)
            if not os.path.exists(matched_pdf_path):
                logger.error(f"在 '{pdf_directory}' 中找到與網頁標題 '{page_title}' (清理後: '{cleaned_page_title_for_match}') 匹配的 PDF 檔案，但生成路徑 '{matched_pdf_path}' 不存在。")
                return None
            logger.notice(f"找到匹配的 PDF 檔案: '{matched_pdf_path}'，將用於介接說明文件。")
            return matched_pdf_path
    logger.warning(f"未在<{pdf_directory}>中找到與網頁標題 '{page_title}' (清理後: '{cleaned_page_title_for_match}') 匹配的 PDF 檔案。")
    return None

def replace_url_parameters(original_url, new_params):
    """
    替換網址中的查詢參數。

    Args:
        original_url (str): 原始網址字串。
        new_params (dict): 包含要替換的新參數的字典。

    Returns:
        str: 替換參數後的網址字串。
    """
    # 1. 解析原始網址
    parsed_url = urlparse(original_url)

    # 2. 解析查詢字串為字典
    # parse_qs 會將重複的參數名儲存為列表 (例如: ?key=val1&key=val2 -> {'key': ['val1', 'val2']})
    # 但在 WMS 這種情況下，參數通常不會重複，所以直接用 update 即可
    query_params = parse_qs(parsed_url.query)

    # 3. 更新參數值
    # 注意：parse_qs 得到的字典值是列表，所以更新時也要確保是列表或能被 urlencode 正確處理
    for key, value in new_params.items():
        query_params[key] = [str(value)] # 確保值是字串且包在列表中，符合 urlencode 的預期

    # 4. 將更新後的參數字典重新編碼成查詢字串
    # doseq=True 確保值為列表時，會產生 key=val1&key=val2 這種形式，但這裡我們強制轉為單一值列表。
    # 實際上，通常不需要特別設定 doseq=True，因為我們已經處理成單一值的列表。
    updated_query = urlencode(query_params, doseq=True)

    # 5. 重組網址
    # parsed_url.replace(query=...) 會返回一個新的 ParseResult 物件
    new_parsed_url = parsed_url._replace(query=updated_query)

    # 6. 將 ParseResult 物件轉換回網址字串
    return urlunparse(new_parsed_url)

def _download_and_save_image(url: str, directory: str, filename: str) -> Optional[str]:
    """
    下載圖片並儲存到指定路徑。
    """
    path = os.path.join(directory, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # 檢查檔案是否存在，如果存在則刪除
    if os.path.exists(path):
        logger.notice(f"檔案 '{path}' 已存在，正在刪除舊檔案...")
        try:
            os.remove(path)
            logger.notice(f"舊檔案 '{path}' 已成功刪除。")
        except OSError as e:
            logger.error(f"刪除舊檔案 '{path}' 時發生錯誤: {e}")
            return None # 如果無法刪除舊檔案，則終止操作

    try:
        response = requests.get(url, timeout=10) # 增加 timeout
        response.raise_for_status()
        with open(path, 'wb') as f:
            f.write(response.content)
        logger.notice(f"圖片已成功下載並儲存至: '{path}'")
        return path
    except requests.exceptions.RequestException as e:
        logger.error(f"下載圖片 '{url}' 時發生網路錯誤: {e}")
        return None
    except (IOError, OSError) as e:
        logger.error(f"儲存圖片至 '{path}' 時發生檔案系統錯誤: {e}")
        return None
    except Exception as e:
        logger.error(f"下載或儲存圖片 '{url}' 時發生未預期錯誤: {e}")
        return None

def fetch_and_process_json_data(json_data_raw: list, cleaned_page_title: str, category_table_id) -> list:
    """
    處理原始 JSON 資料，並下載其中的分布圖（如果存在）。
    返回處理後的 JSON 資料和本地儲存路徑。
    """
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    if not json_data_raw:
        logger.warning(f"'{cleaned_page_title}' 的json_data_raw為空，終止處理。")
        return ([], "")
    if isinstance(json_data_raw, dict):
        json_data_raw = [json_data_raw]
    elif not isinstance(json_data_raw, list):
        logger.error(f"'{cleaned_page_title}' 的json_data_raw不是字典也不是列表，終止處理。")
        return ([], "")
    json_data_first = json_data_raw[0]
    if "分布圖Url" in json_data_first or "代碼" in cleaned_page_title:
        logger.notice("資料為分布圖或代碼，重新建立資料序列。")
        id_start = int(f"{category_table_id}0000000") # 地圖資料或代碼永遠從最初開始 (全覆蓋)
        for json_data in json_data_raw:
            id_start+=1
            try:
                first_value = json_data[next(iter(json_data))]
                logger.debug(f"正在處理 '{cleaned_page_title}' 的JSON 資料，資料表鍵值: '{first_value}'")
                # map_title = json_data["圖檔中文名稱"]
                # logger.notice(f"正在處理 '{cleaned_page_title}' 的JSON 資料，圖檔資料名稱: '{map_title}'")
                # png_url = json_data["分布圖Url"]
                # if png_url: # 不再下載圖片
                json_data["category_table_data_id"] = str(id_start)
                    # json_data["版本"] = "0"
                json_data["foreign_key"] = category_table_id
                # else:
                #     logger.warning(f"下載 '{cleaned_page_title}' 的分布圖 '{first_value}' 失敗，跳過此圖片處理。")
            except Exception as e:
                logger.error(f"處理 '{cleaned_page_title}' 的分布圖 '{first_value}' 時遭遇錯誤: {e}")
    else:
        # logger.notice(f"原始資料 '{cleaned_page_title}' 不包含分布圖 URL，跳過分布圖下載。")
        for json_data in json_data_raw:
            # json_data["版本"] = "0"
            json_data["foreign_key"] = category_table_id

    json_filename = f"{cleaned_page_title}.json"
    json_path = save_json_data(json_data_raw, os.path.join(current_file_dir, "raw_data", json_filename))
    logger.debug(f"已下載 '{cleaned_page_title}' 的原始 JSON 資料路徑: '{json_path}'")
    return json_data_raw, json_path

def parse_webpage_to_metadata(webpage_url: str) -> dict:
    """
    解析指定資料集網頁，提取資料集的相關資訊（例如：資料集名稱、描述、下載連結、更新頻率、資料格式等），
    並將其組織成字典格式的 metadata。
    介接說明文件將直接使用本地提供的 PDF 檔案。
    """
    download_links = {}
    metadata_json = {}
    now = "" # 初始化 now 變數
    try:
        response = requests.get(webpage_url, timeout=10) # 增加 timeout
        response.raise_for_status()  # 檢查 HTTP 請求是否成功
    except requests.exceptions.RequestException as e:
        logger.error(f"從 '{webpage_url}' 下載網頁時發生網路錯誤: {e}")
        return {} # 返回空字典，表示解析失敗
    
    try:
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        logger.error(f"解析網頁 '{webpage_url}' 內容時發生錯誤: {e}")
        return {} # 返回空字典，表示解析失敗

    # 提取網頁標題
    title_tag = soup.find("title")
    page_title = ""
    if title_tag:
        page_title = re.sub(r'\s*-\s*農業資料開放平臺\s*', '', title_tag.get_text(strip=True))
        metadata_json["標題"] = page_title
        metadata_json["連結"] = webpage_url
        clean_page_title = clean_table_name(page_title)
        if yes_no_menu(f"是否尋找<{clean_page_title}>的介接說明文件 PDF 檔案?"):
            selected_pdf_path = _find_and_select_pdf_document(page_title)
            if not selected_pdf_path:
                logger.warning("沒有找到匹配的檔案。")
                selected_pdf_path = ""
        else:
            selected_pdf_path = ""
    data_content_divs = soup.find_all('div', class_='data-search data-content')
    data_content_div = None
    if len(data_content_divs) > 1:
        data_content_div = data_content_divs[1]
    else:
        logger.warning(f"網頁 '{webpage_url}' 中未找到足夠的 'data-search data-content' 區塊來提取 metadata。")
        # 嘗試從第一個區塊提取，如果第二個不存在
        if len(data_content_divs) > 0:
            data_content_div = data_content_divs[0]
            logger.notice(f"改為從第一個 'data-search data-content' 區塊提取 metadata。")
        else:
            logger.error(f"網頁 '{webpage_url}' 中未找到任何 'data-search data-content' 區塊，無法解析 metadata。")
            return {}
    
    # 提取資料更新日期 (根據使用者反饋，在整個 soup 中尋找或在 data-title-wrapper 中尋找)
    # 優先在 data-title_wrapper 中尋找
    data_title_wrapper_div = soup.find('div', class_='data-title-wrapper')
    update_date_found = False
    if data_title_wrapper_div:
        for span_tag in data_title_wrapper_div.find_all('span'):
            span_text = span_tag.get_text(strip=True)
            if "資料更新日期" in span_text:
                date_match = re.search(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', span_text)
                if date_match:
                    now = date_match.group(1)
                    update_date_found = True
                    logger.notice(f"已提取 '{page_title}' 的資料更新日期: '{now}'")
                    break
                else:
                    logger.error(f"從 '{page_title}' 提取的資料更新日期 '{span_text}' 不符合預期格式。")
                    # 不直接返回空字典，嘗試在全頁搜尋
    if not update_date_found: # 如果在 data-title-wrapper 中沒找到，則在整個 soup 中尋找
        logger.warning(f"在 '{page_title}' 中未找到預期的 '資料更新日期' 於 data-title-wrapper，嘗試全頁搜尋。")
        for span_tag in soup.find_all('span'):
            span_text = span_tag.get_text(strip=True)
            if "資料更新日期" in span_text:
                date_match = re.search(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', span_text)
                if date_match:
                    now = date_match.group(1)
                    update_date_found = True
                    logger.notice(f"已提取 '{page_title}' 的資料更新日期: '{now}' (全頁搜尋)")
                    break
                else:
                    logger.error(f"從 '{page_title}' 提取的資料更新日期 '{span_text}' 不符合預期格式 (全頁搜尋)。")
                    # 不直接返回空字典，繼續執行，讓後續檢查判斷是否缺少關鍵資訊

    if not update_date_found:
        logger.error(f"在 '{page_title}' 中未找到 '資料更新日期' 或日期格式不符，無法解析 metadata。")
        return {} # 返回空字典，表示解析失敗
    if data_content_div: # 繼續處理 data_content_div 中的其他 metadata
        # 同時尋找 label 和 span 標籤，且都帶有 class_='label'
        labels_and_spans = data_content_div.find_all(['label', 'span'], class_='label')
        for label in labels_and_spans:
            key = label.get_text(strip=True)
            if key == "資料評分":
                continue            
            # 尋找同層級的 <div class="search-input"> 中的 <p> 或 <a> 標籤作為值
            try:
                value_div = label.find_parent('div', class_='search-input').find_next_sibling('div', class_='search-input')
                value_tag = None
                if value_div:
                    value_tag = value_div.find(['p', 'a'])
                
                if value_tag:
                    if key == "資料介接":
                        # 處理資料介接，提取下載連結
                        # 嘗試從當前 value_div 中尋找所有 <a> 標籤
                        json_link_tag = value_div.find('a', href=True) if value_div else None
                        if json_link_tag:
                            json_url = urljoin(webpage_url, json_link_tag['href'])
                            download_links['json_data'] = json_url
                            metadata_json[key] = json_url # 將連結也存入 metadata
                        else:
                            metadata_json[key] = value_tag.get_text(strip=True) if value_tag else None
                    else:
                        metadata_json[key] = value_tag.get_text(strip=True)
                else:
                    metadata_json[key] = None # 如果沒有找到對應的值標籤
            except AttributeError as e:
                logger.error(f"解析 metadata 鍵 '{key}' 時發生屬性錯誤: {e}。可能找不到預期的 HTML 結構。")
                metadata_json[key] = None # 設定為 None 以避免程式崩潰
            except Exception as e:
                logger.error(f"解析 metadata 鍵 '{key}' 時發生未預期錯誤: {e}")
                metadata_json[key] = None # 設定為 None 以避免程式崩潰    
    metadata_json["介接說明文件"] = selected_pdf_path
    metadata_json["資料更新日期"] = now
    logger.notice(f"已成功解析 '{page_title}' 的 metadata。")
    return metadata_json

def download_and_deduplicate_data(title, user_inputs) -> list[dict]:
    """
    從指定 URL 下載 JSON 資料，進行內部去重。
    所有參數都在函數內部生成。
    Returns:
        string:
               sorted_downloaded_data: 去重後的下載資料。
    """
    restoration_result = load_interruption_info_and_prompt_restore()
    name = title
    if restoration_result:
        json_list, interruption_details = restoration_result
        name = interruption_details.get('name', '')
        base_url = interruption_details.get('url', '')
        skip = interruption_details.get('false_at_skip', "0") # 如果沒有 false_at_skip，則從 0 開始
        skip = int(skip)
        logger.notice(f"已從中斷點還原資料。從 skip={skip} 繼續下載，檔案名稱: {name}，基礎 URL: {base_url}")
    else:
        base_url = user_inputs.get("url", "")
        skip_input = user_inputs.get("skip", "0")
        # input(f"{base_url}, {skip_input}, {name}")
        if not base_url or not name:
            logger.error("URL 或檔案名稱不能為空，終止下載。")
            return []
        try:
            skip = int(skip_input)
        except ValueError:
            logger.error(f"skip起始數值 '{skip_input}' 無效，請輸入一個整數。")
            return []
        json_list = []
    if "?" in base_url:
        start_url = base_url + "&$skip="
    else:
        start_url = base_url + "?$skip="    
    while skip >= 0:
        url = start_url + str(skip)
        logger.info(f"正在從 '{url}' 下載資料...")
        try:
            response = requests.get(url)
            response.raise_for_status()  # 檢查HTTP錯誤
            data = response.json()
            logger.info(f"成功下載 '{len(data)}' 筆資料。")
            # 檢查並確保 data 中的每個元素都是字典
            cleaned_data = []
            for item in data:
                if isinstance(item, dict):
                    cleaned_data.append(item)
                else:
                    logger.warning(f"警告: 下載的資料中包含非字典元素，已跳過。元素類型: {type(item)}, 內容: {item}")
            
            data = cleaned_data # 使用清理後的資料
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            logger.error(f"下載或解析 '{name}' 資料時發生錯誤: {e}")
            interruption_details = {
                "date": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
                "name": name,
                "url": base_url, # 儲存基礎 URL
                "false_at_skip": skip # 記錄中斷時的 skip 值
            }
            save_interruption =  save_interruption_info(json_list, interruption_details)
            if save_interruption:
                logger.warning("已儲存中斷資訊，請檢查 'hand_download' 目錄。")
            elif save_interruption == False:
                logger.error("儲存中斷資訊時發生錯誤。")
            elif save_interruption == None:
                logger.warning("未儲存中斷資訊。")
            return []        
        if not data:
            logger.notice("資料末端：下載的資料為空。")        
        logger.notice(f"下載 {url} 完成，資料數目：{len(data)}")
        json_list = data + json_list
        if skip == 0:
            break
        skip -= 1000
        if len(data) > 1000 or len(data) == 0:
            skip -= 9999
        if skip < 0: # 確保最後一輪的skip不會變成負數，而是0
            skip = 0
        time.sleep(2)
    if not json_list:
        logger.error(f"'{name}' 沒有下載任何資料，終止下載。")
        return []
    # 對下載的資料進行內部去重
    unique_downloaded_data = remove_duplicates_from_list_of_dicts(json_list)
    logger.notice(f"'{name}' 的下載資料內部去重後數目：{len(unique_downloaded_data)}")
    # 將資料反轉為從舊到新，並進行互動式排序
    return unique_downloaded_data

def _combine_and_filter_data(existing_data: list[dict], sorted_downloaded_data: list[dict], name: str, data_filter: bool = True) -> tuple[list, list, str]:
    """
    處理現有資料與新下載資料的合併與去重邏輯。
    Args:
        existing_data (list[dict]): 現有的 JSON 資料列表。
        sorted_downloaded_data (list[dict]): 已排序並去重的新下載 JSON 資料列表。
        name (str): 檔案名稱。
        base_url (str): 基礎 URL。
    Returns:
        tuple:
               truly_new_items: 真正新下載的資料列表。
               combined_data: 合併後的資料列表。
               name: 檔案名稱。
    """
    # 獲取新下載資料的 key 集合，用於過濾現有資料
    if existing_data:
        if data_filter:
            existing_data = preserve_old_data_by_date(existing_data)
    downloaded_keys = set()
    if sorted_downloaded_data: # 確保 sorted_downloaded_data 不為空
        try:
            # 檢查 sorted_downloaded_data[0] 是否為字典
            if isinstance(sorted_downloaded_data[0], dict):
                downloaded_keys = set(sorted_downloaded_data[0].keys())
            else:
                logger.error("sorted_downloaded_data[0] 不是字典，無法獲取鍵。")
                # 這裡可以選擇拋出錯誤，或者返回空集合，或者嘗試從其他元素獲取鍵
                # 為了避免程式崩潰，暫時返回空集合
                downloaded_keys = set()
        except IndexError:
            logger.warning("sorted_downloaded_data 為空列表，無法獲取鍵。")
            downloaded_keys = set()
    
    existing_data_identifiers = set()
    if existing_data and downloaded_keys: # 只有當 existing_data 和 downloaded_keys 都不為空時才進行過濾
        logger.info("開始清理現有資料...")
        for item in existing_data:
            # 只保留在下載資料中存在的 key
            filtered_item = {k: v for k, v in item.items() if k in downloaded_keys}
            try:
                existing_data_identifiers.add(json.dumps(filtered_item, sort_keys=True))
            except TypeError as e:
                logger.error(f"將現有資料項目序列化為 JSON 時發生錯誤: {e}。項目: {filtered_item}")
                continue # 跳過此項目
    logger.notice(f"現有資料唯一識別碼數目：{len(existing_data_identifiers)}")

    truly_new_items = []
    if existing_data and sorted_downloaded_data:
        logger.notice("開始清理並比對新下載資料與現有資料...")
        for item in sorted_downloaded_data:
            # 過濾 item，只保留 downloaded_keys 中的 key
            filtered_item = {k: v for k, v in item.items() if k in downloaded_keys}
            try:
                item_identifier = json.dumps(filtered_item, sort_keys=True)
                if item_identifier not in existing_data_identifiers:
                    truly_new_items.append(item)
            except TypeError as e:
                logger.error(f"將新下載資料項目序列化為 JSON 時發生錯誤: {e}。項目: {filtered_item}")
                continue # 跳過此項目
    elif not existing_data:
        logger.notice("沒有現有資料，所有新下載資料將被視為不重複。")
        truly_new_items = sorted_downloaded_data
        final_combined_data, sort_keys_used, _ = sort_json_file_interactively(data=truly_new_items, current_filename=name)
        return truly_new_items, final_combined_data, sort_keys_used
    elif not sorted_downloaded_data:
        logger.notice("沒有新下載資料，無需合併。")
        final_combined_data, sort_keys_used, _ = sort_json_file_interactively(data=existing_data, current_filename=name)
        return [], final_combined_data, sort_keys_used
    # 新語句結束
    logger.notice(f"'{name}' 的下載資料中新的資料數目：{len(truly_new_items)}")
    if len(truly_new_items) == 0:
        logger.notice("沒有新下載資料，無需合併。")
        final_combined_data, sort_keys_used, _ = sort_json_file_interactively(data=existing_data, current_filename=name)
        return [], final_combined_data, sort_keys_used

    final_combined_data = []
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = "hand_download"
    output_path = os.path.join(current_file_dir, output_dir, f"{name}.json")

    # 只有當 existing_data 和 truly_new_items 都不為空時才進行 key 比較和合併
    # 增加對 truly_new_items[0] 和 existing_data[0] 存在的檢查
    key_check = True if set(truly_new_items[0].keys()) == set(existing_data[0].keys()) else False
    logger.notice(f"對資料內容的檢查:\n原有資料長度：{len(existing_data)}\n新下載資料長度：{len(truly_new_items)}\n鍵是否相等: {key_check}")
    if existing_data and truly_new_items and \
       len(truly_new_items) > 0 and len(existing_data) > 0 and \
       set(truly_new_items[0].keys()) == set(existing_data[0].keys()):
        # 合併現有資料和真正新的不重複資料 (只有在key相同時)
        combined_data = existing_data + truly_new_items
    elif truly_new_items: # 如果 key 不匹配，但有新的下載資料，則只儲存新的下載資料
        combined_data = truly_new_items
        logger.notice(f"現有資料與新下載資料的 key 不匹配，將只儲存新下載資料。總數：{len(combined_data)}")
    elif existing_data: # 如果沒有新的下載資料，但有現有資料，則只儲存現有資料
        combined_data = existing_data
        logger.notice(f"沒有新的下載資料，將只儲存現有資料。總數：{len(combined_data)}")
    else: # 兩者都為空
        logger.warning("沒有任何資料可供儲存。")
        return [], [], []
    final_combined_data, sort_keys_used, _ = sort_json_file_interactively(data=combined_data, current_filename=name)
    logger.info(f"最終經過合併、去重與排序後的資料總數：{len(final_combined_data)}")

    os.makedirs(output_dir, exist_ok=True) # 確保目錄存在
    
    if not final_combined_data: # 如果沒有資料，則不詢問儲存
        logger.warning("沒有資料可供儲存，跳過儲存步驟。")
        return [], [], []
    if handle_save_menu(output_path, action_description="合併", save_action_description="儲存", name=name):
        if save_json_data(final_combined_data, output_path):
            return truly_new_items, final_combined_data, sort_keys_used
    return [], [], []

def download_data(title, user_inputs, data_filter = True) -> tuple[list, list, list, bool]:
    """
    主函數，用於從指定 URL 下載 JSON 格式的統計資料。
    使用使用者輸入的url，起始的 skip 值和檔案名稱，
    下載新資料。
    """
    downloaded_data = download_and_deduplicate_data(title, user_inputs)
    if not downloaded_data:
        return [], [], [], False
    logger.logs(f"<{title}>的資料已下載完成，共 {len(downloaded_data)} 筆資料。")
    if data_filter:
        downloaded_data = preserve_old_data_by_date(downloaded_data)
    if not downloaded_data:
        logger.warning("沒有新下載的資料，終止後續處理。")
        return [], [], [], True
    logger.notice(f"已篩選下載的最近資料，共 {len(downloaded_data)} 筆資料。")
    New_item, combined_data, sort_keys_used = _combine_and_filter_data([], downloaded_data, title, data_filter)
    if not sort_keys_used:
        logger.warning("未取得排序鍵，終止後續處理。")
        return [], [], [], False
    if not New_item:
        logger.warning("沒有新下載的資料，終止後續處理。")
        return [], [], [], True
    return New_item, combined_data, sort_keys_used, True

def _fetch_data_logic(base_url, name, skip, max_retries=3):
    """
    負責單次下載請求，包含 URL 處理、重試機制與資料清洗。
    回傳: 成功則回傳 list[dict]，徹底失敗則回傳 None。
    """
    separator = "&$skip=" if "?" in base_url else "?$skip="
    url = f"{base_url}{separator}{skip}"
    # input(url)
    
    for i in range(max_retries):
        try:
            # 探針與下載共用，設定合理的 timeout
            response = requests.get(url, timeout=25)
            response.raise_for_status()
            raw_data = response.json()
            # input(len(raw_data))
            # 資料清洗：確保回傳的一定是 list，且過濾非字典元素
            if not isinstance(raw_data, list):
                logger.warning(f"<{name}> skip={skip} 回傳格式非列表，視為空資料。")
                return []
                
            cleaned_data = [item for item in raw_data if isinstance(item, dict)]
            # input(len(cleaned_data))
            
            return cleaned_data

        except (requests.exceptions.RequestException, Exception) as e:
            wait_time = (i + 1) * 2
            logger.warning(f"⚠️ <{name}> skip={skip} 請求失敗 ({e})，{wait_time}秒後進行第 {i+1} 次重試...")
            if i < max_retries - 1:
                time.sleep(wait_time)
            else:
                logger.error(f"❌ <{name}> skip={skip} 在 {max_retries} 次重試後依然失敗。")
    
    return None # 徹底出局

def detect_optimal_skip(base_url, name, data_len=None):
    logger.info(f"🔍 啟動步進探針：自動偵測 <{name}> 的分頁末端...")

    # 1. 探測分頁上限 (max_limit 作為我們的「步長」)
    first_page = _fetch_data_logic(base_url, name, skip=0)
    if first_page is None: return None
    if not first_page: return 0
    
    max_limit = 9999 if len(first_page) > 1000 else 1000
    
    # 2. 設定搜索區間
    low = 0
    # high 必須是步長的倍數
    high = int(data_len) if data_len else max_limit
    if high % max_limit != 0:
        high = (high // max_limit + 1) * max_limit

    # 3. 倍增階段 (Exponential Search) - 按頁翻倍
    while True:
        data = _fetch_data_logic(base_url, name, skip=high)
        if data is None: return None
            
        count = len(data)
        logger.info(f"📡 步進倍增：skip={high}, 取得 {count} 筆")
        
        # 只要 count > 0，代表這一頁還有東西（不論是否滿頁）
        if count < max_limit: 
            # 找到包含末端的區間了。
            # 如果 count == 0，代表 high 這頁空了；
            # 如果 0 < count < max_limit，代表 high 就是最後一頁。
            break 
        
        low = high
        high *= 2

    # 4. 二分逼近階段 (Binary Search) - 按頁收斂
    logger.info(f"🎯 步進收斂：區間 [{low} ~ {high}]，步長 {max_limit}")
    
    # 修改：當區間縮小到剩下一頁的距離，就停止
    while (high - low) > max_limit:
        mid = (low + high) // 2
        # 修改：強制對齊步長 (例如 1000, 2000...)
        mid = (mid // max_limit) * max_limit
        
        # 安全檢查：避免 mid 卡死在 low
        if mid <= low:
            break

        data = _fetch_data_logic(base_url, name, skip=mid)
        if data is None: return None
        
        # 修改：判斷邏輯改為「是否有資料」
        if len(data) > 0:
            # 這一頁還有資料，所以這是我目前已知的「最後存活點」
            low = mid   
        else:
            # 這一頁完全沒資料，這是我的「絕對上限」
            high = mid
            
    # 修改：回傳 low。
    # 因為 low 是我們最後一次確認「還有資料 (count > 0)」的 skip 位置。
    logger.logs(f"✅ 探針完成！建議 <{name}> 的下載從最後有效skip開始：{low}")
    return f'{low}'

    