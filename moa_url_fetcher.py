"""這個檔案已不再使用和維護。未來的任何程式碼更新將排除此檔案。"""
import time
import json # 新增導入 json 模組
import tkinter as tk # 導入 tkinter 模組
from tkinter import scrolledtext # 導入 scrolledtext 模組
import configparser # 導入 configparser 模組
import sys # 導入 sys 模組

from selenium import webdriver
from logs_handle import logger, setup_logging_to_tkinter # 導入 logger.info 和 setup_logging_to_tkinter 函式
from ui_interface import TerminalWindow, StdoutRedirector # 導入 TerminalWindow 和 StdoutRedirector 類別
from ui_interface import ButtonManager # 導入 ButtonManager 類別
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import os
import threading # 新增導入 threading 模組

def load_progress_data(progress_file_path: str) -> dict:
    """
    從進度檔案讀取進度資料。
    Args:
        progress_file_path (str): 進度檔案的路徑。
    Returns:
        dict: 進度資料字典，如果檔案不存在或解析失敗則返回空字典。
    """
    progress_data = {}
    if os.path.exists(progress_file_path):
        try:
            with open(progress_file_path, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
        except json.JSONDecodeError as e:
            logger.info(f"無法解析進度檔案 '{progress_file_path}'，將視為無進度。錯誤: {e}")
        except Exception as e:
            logger.info(f"讀取進度檔案 '{progress_file_path}' 時發生錯誤: {e}")
    return progress_data

def check_and_get_unfinished_topics(progress_file_path: str, all_topics_list: list) -> tuple[list, bool]:
    """
    檢查上次處理到哪個應用主題沒有完成，並返回未完成的主題列表和是否有已完成的主題。
    進度檔案現在儲存 download_status 和 update_status。

    Args:
        progress_file_path (str): 儲存進度狀態的檔案路徑 (例如: 'progress_status.json')。
        all_topics_list (list): 包含所有應用主題名稱的列表。

    Returns:
        tuple[list, bool]: 未完成的應用主題名稱列表和一個布林值，表示是否有已完成的主題。
    """
    logger.info(f"進入 check_and_get_unfinished_topics 函數。progress_file_path: {progress_file_path}, all_topics_list 長度: {len(all_topics_list)}", "debug")
    progress_data = {}
    has_completed_downloads = False
    if os.path.exists(progress_file_path):
        with open(progress_file_path, 'r', encoding='utf-8') as f:
            try:
                progress_data = json.load(f)
                logger.info(f"成功載入進度檔案 '{progress_file_path}'，資料類型: {type(progress_data)}", "debug")
                # 檢查是否有任何主題的 download_status 已完成
                if any(item.get('download_status') == 'completed' for item in progress_data.values() if isinstance(item, dict)):
                    has_completed_downloads = True
            except json.JSONDecodeError as e:
                logger.info(f"無法解析進度檔案 '{progress_file_path}'，將視為無進度。錯誤: {e}")
                progress_data = {}

    unfinished_topics = []
    for topic_name in all_topics_list:
        # 如果主題不存在或 download_status 不是 'completed'，則視為未完成
        topic_status = progress_data.get(topic_name)
        if not isinstance(topic_status, dict) or topic_status.get('download_status') != 'completed':
            unfinished_topics.append(topic_name)

    return unfinished_topics, has_completed_downloads

def update_topic_progress(topic_identifier: str, download_status: str, update_status: str, progress_file_path: str):
    """
    更新特定應用主題的進度狀態，包括下載狀態和更新狀態。

    Args:
        topic_identifier (str): 應用主題的唯一識別符。
        download_status (str): 主題的下載狀態 (例如: 'pending', 'in_progress', 'completed', 'failed')。
        update_status (str): 主題的更新狀態 (例如: 'not_checked', 'updated', 'not_updated')。
        progress_file_path (str): 儲存進度狀態的檔案路徑。
    """
    progress_data = {}
    if os.path.exists(progress_file_path):
        with open(progress_file_path, 'r', encoding='utf-8') as f:
            try:
                progress_data = json.load(f)
            except json.JSONDecodeError:
                logger.info(f"無法解析進度檔案 '{progress_file_path}'，將從空進度開始。")
                progress_data = {}

    # 獲取現有的主題狀態，如果不存在則初始化為空字典
    current_topic_data = progress_data.get(topic_identifier, {})
    current_topic_data['download_status'] = download_status
    current_topic_data['update_status'] = update_status
    progress_data[topic_identifier] = current_topic_data

    with open(progress_file_path, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=4)

def check_for_updates(data_entry_list: list, moa_data_path: str) -> list:
    """
    檢查資料更新，比較每一個data_entry和data_list_of_moa.json中的"標題"和"資料更新日期"，
    並在data_entry中添加"更新狀態"欄位。

    Args:
        data_entry_list (list): 待檢查的資料列表，每個元素應為字典，包含"標題"和"資料更新日期"。
        moa_data_path (str): data_list_of_moa.json 檔案的路徑。

    Returns:
        list: 包含所有資料條目及其"更新狀態"的列表。
    """
    existing_moa_data = {}

    # 讀取現有的 data_list_of_moa.json
    if os.path.exists(moa_data_path):
        with open(moa_data_path, 'r', encoding='utf-8') as f:
            try:
                moa_list = json.load(f)
                # 將現有資料轉換為以"標題"為鍵的字典，方便查找
                existing_moa_data = {item.get("標題"): item for item in moa_list}
            except json.JSONDecodeError:
                logger.info(f"無法解析 '{moa_data_path}'，檔案可能損壞或為空。")
                moa_list = []
    else:
        logger.info(f"找不到 '{moa_data_path}'，將視為所有資料為新資料。")

    for entry in data_entry_list:
        title = entry.get("標題")
        update_date = entry.get("資料更新日期")
        
        if not title or not update_date:
            logger.info(f"資料條目 '{entry}' 缺少 '標題' 或 '資料更新日期' 欄位，跳過檢查。")
            entry["更新狀態"] = "invalid_data"
            continue

        if title in existing_moa_data:
            existing_date = existing_moa_data[title].get("資料更新日期")
            if existing_date != update_date:
                logger.info(f"資料 '{title}' 發現更新：舊日期 '{existing_date}'，新日期 '{update_date}'")
                entry["更新狀態"] = "updated"
            else:
                entry["更新狀態"] = "not_updated"
        else:
            # 如果標題不存在於現有資料中，則視為新資料
            entry["更新狀態"] = "new_data" # 新增 'new_data' 狀態
    return data_entry_list
def expand_categories():
    """
    點擊按鈕展開應用主題區塊的所有分類。
    """
    try:
        category_navs = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.category-nav")))
        target_button = None
        for nav in category_navs:
            try:
                title_element = nav.find_element(By.CSS_SELECTOR, "div.category-title")
                if title_element.text.strip() == "應用主題":
                    target_button = nav.find_element(By.CSS_SELECTOR, "button.category-more")
                    break
            except NoSuchElementException:
                continue

        if target_button:
            target_button.click()
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".category-list.is-opened")))
            driver.execute_script("window.scrollTo(0, 0);") # 捲動到頁面最上方
        else:
            raise NoSuchElementException("未能找到 '應用主題' 區塊的 '展開分類' 按鈕。")

    except TimeoutException as e:
        logger.info(f"展開應用主題分類時超時: {e}", "critical")
    except NoSuchElementException as e:
        logger.info(f"展開應用主題分類時找不到元素: {e}", "critical")
    except Exception as e:
        logger.info(f"展開應用主題分類時發生未知錯誤: {e}", "critical")

def get_category_topics() -> list:
    """
    獲取所有應用主題分類的名稱。

    Returns:
        list: 包含所有應用主題名稱的列表。
    """
    try:
        category_wrapper = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "category-btn-wrapper")))
        category_buttons = category_wrapper.find_elements(By.CLASS_NAME, "category-btn")
        logger.info(f"在應用主題區塊中找到 {len(category_buttons)} 個分類。")
        category_texts = [button.text for button in category_buttons]
        logger.info(f"所有主題分類名稱: {category_texts}")
        return category_texts
    except Exception as e:
        logger.info(f"獲取應用主題分類時發生錯誤: {e}", "critical")
        return []


def extract_data_from_page() -> list:
    """
    從當前頁面提取資料。

    Returns:
        list: 包含當前頁面所有資料條目的列表。
    """
    page_data = []
    try:
        search_results = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.search-result li.result-item")))
        logger.info(f"找到 {len(search_results)} 筆資料。")
        for i, result_item in enumerate(search_results):
            data_entry = {}
            try:
                link_element = result_item.find_element(By.CSS_SELECTOR, "div.result-info a.title.is-4")
                relative_link = link_element.get_attribute("href")
                data_entry["連結"] = f"https://data.moa.gov.tw/{relative_link}"
                data_entry["標題"] = link_element.text.strip()
            except Exception as e:
                logger.info(f"提取資料條目 {i} 的 '連結' 或 '標題' 欄位時發生錯誤: {e}。將設為 'N/A'。")
                data_entry["連結"] = "N/A"
                data_entry["標題"] = "N/A"

            try:
                provider_element = result_item.find_element(By.XPATH, ".//p[@class='result-title' and text()='提供機關']/following-sibling::p[@class='result-value']")
                data_entry["提供機關"] = provider_element.text.strip()
            except Exception as e:
                logger.info(f"提取資料條目 {i} 的 '提供機關' 欄位時發生錯誤: {e}。將設為 'N/A'。")
                data_entry["提供機關"] = "N/A"

            try:
                app_type_element = result_item.find_element(By.XPATH, ".//p[@class='result-title' and text()='應用類型']/following-sibling::p[@class='result-value']")
                data_entry["應用類型"] = app_type_element.text.strip()
            except Exception as e:
                logger.info(f"提取資料條目 {i} 的 '應用類型' 欄位時發生錯誤: {e}。將設為 'N/A'。")
                data_entry["應用類型"] = "N/A"

            try:
                update_date_element = result_item.find_element(By.CSS_SELECTOR, "p.help")
                data_entry["資料更新日期"] = update_date_element.text.replace("資料更新日期：", "").strip()
            except Exception as e:
                logger.info(f"提取資料條目 {i} 的 '資料更新日期' 欄位時發生錯誤: {e}。將設為 'N/A'。")
                data_entry["資料更新日期"] = "N/A"

            page_data.append(data_entry)
        return page_data
    except Exception as e:
        logger.info(f"從頁面提取資料時發生錯誤: {e}", "critical")
        return []

def navigate_pagination(current_page: int, total_pages: int) -> bool:
    """
    處理分頁導航。

    Args:
        current_page (int): 當前頁碼。
        total_pages (int): 總頁碼。

    Returns:
        bool: 如果成功導航到下一頁則返回 True，否則返回 False (已到達最後一頁或發生錯誤)。
    """
    if current_page < total_pages:
        try:
            # 獲取當前頁面第一個結果項目的文本內容
            old_first_result_item = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".data-wrapper ul.search-result > li.result-item")))
            old_first_result_text = old_first_result_item.text.strip()
            logger.info(f"舊頁面第一個結果項目文本: '{old_first_result_text}'", "debug")

            next_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "li.next")))
            next_button.click()
            def first_result_item_text_changes(driver):
                try:
                    new_first_result_item = driver.find_element(By.CSS_SELECTOR, ".data-wrapper ul.search-result > li.result-item")
                    new_first_result_text = new_first_result_item.text.strip()
                    return new_first_result_text != old_first_result_text
                except NoSuchElementException:
                    time.sleep(0.5)
                    return False # 如果元素還沒出現，返回 False 繼續等待

            # 等待直到第一個 result-item 的文本內容發生變化
            wait.until(first_result_item_text_changes)
            driver.execute_script("window.scrollTo(0, 0);") # 捲動到頁面最上方
            time.sleep(0.5) # 給予頁面額外的載入時間
            return True
        except Exception as e:
            logger.info(f"導航到下一頁時發生錯誤: {e}")
            return False
    else:
        logger.info("已到達最後一頁，結束分頁導航。")
        return False

def process_topic_data(topic_name: str, progress_file_path: str) -> list:
    """
    處理單一應用主題的資料提取。

    Args:
        topic_name (str): 當前處理的應用主題名稱。
        progress_file_path (str): 儲存進度狀態的檔案路徑。

    Returns:
        list: 該主題下所有提取的資料列表。
    """
    topic_data = []
    try:
        # 由於頁面可能刷新，需要重新獲取 category_buttons
        category_wrapper = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "category-btn-wrapper")))
        category_buttons = category_wrapper.find_elements(By.CLASS_NAME, "category-btn")
        button = next(btn for btn in category_buttons if btn.text == topic_name)
        button.click()
        # 等待 filter-list 中出現包含當前主題名稱的 li 元素，表示篩選已應用
        wait.until(EC.presence_of_element_located((By.XPATH, f"//ul[@class='filter-list']/li[contains(., '{topic_name}')]")))
        logger.info(f"選擇分類 '{topic_name}'。")
        # 初始化為 in_progress 和 not_checked
        update_topic_progress(topic_name, 'in_progress', 'not_checked', progress_file_path)
        time.sleep(1)
        try:
            select_element = wait.until(EC.presence_of_element_located((By.XPATH, "//div[@class='data-pagina']//div[@class='sum']/div[@class='select']/select")))
            select = Select(select_element)
            select.select_by_value("100")
            logger.info("每頁顯示 100 筆資料。")
            time.sleep(1)
            driver.execute_script("window.scrollTo(0, 0);") # 捲動到頁面最上方
        except Exception as e:
            logger.info(f"選擇每頁顯示資料數量時發生錯誤: {e}。主題 '{topic_name}' 可能只顯示預設數量的資料。")

        while True:
            try:
                current_page_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "li.current-page")))
                page_text = current_page_element.text.strip()
                parts = page_text.split('/')
                current_page = int(parts[0].replace('第', '').replace('頁', '').strip())
                total_pages = int(parts[1].replace('共', '').replace('頁', '').strip())
                logger.info(f"主題 '{topic_name}' 當前頁面: {current_page} / 總頁面: {total_pages}")
            except Exception as e:
                logger.info(f"獲取主題 '{topic_name}' 的頁面編號時發生錯誤: {e}")
                break
            page_data = extract_data_from_page()
            # 檢查非最後一頁的資料筆數是否為100
            if current_page < total_pages and len(page_data) != 100:
                logger.info(f"警告: 主題 '{topic_name}' 第 {current_page} 頁預期有100筆資料，但實際只有 {len(page_data)} 筆，將標記為 'failed'。")
                topic_data = []
                break
            topic_data.extend(page_data)
            logger.info(f"主題 '{topic_name}' 目前已收集到總共 {len(topic_data)} 筆資料。")
            if not navigate_pagination(current_page, total_pages):
                minimum_data_len = (total_pages-1)*100
                if len(page_data) > 0 and current_page == total_pages and len(topic_data) > minimum_data_len:
                    # 下載完成，更新狀態為 completed，更新狀態為 not_updated (待檢查)
                    update_topic_progress(topic_name, 'completed', 'not_updated', progress_file_path)
                    logger.info(f"主題 '{topic_name}' 資料提取完成並已標記為 'completed'。")
                else:
                    # 下載失敗，更新狀態為 failed，更新狀態為 not_checked
                    update_topic_progress(topic_name, 'failed', 'not_checked', progress_file_path)
                    logger.info(f"主題 '{topic_name}' 在最後一頁未提取到資料或總資料數不足，已標記為 'failed'。")
                    topic_data = []
                break
        return topic_data
    except Exception as e:
        logger.info(f"處理應用主題 '{topic_name}' 時發生錯誤: {e}", "critical")
        # 處理異常時，將下載狀態標記為 failed，更新狀態為 not_checked
        update_topic_progress(topic_name, 'failed', 'not_checked', progress_file_path)
        return []

def run_scraper_logic(unfinished_topics: list, all_topics_list: list):
    """
    執行主要的資料抓取邏輯。
    Args:
        unfinished_topics (list): 本次將處理的應用主題名稱列表。
        all_topics_list (list): 包含所有應用主題名稱的列表。
    """
    progress_file_path = "metadata/data_list_progress_status.json"
    output_filename = "metadata/data_list_of_moa.json" # 將輸出檔案路徑改為 metadata 目錄下

    all_collected_data = [] # 儲存所有主題的資料

    try:
        # 展開分類 (在 main 函數中已經處理過，但為了確保 driver 狀態正確，這裡再次執行)
        # expand_categories(wait)
        
        logger.info(f"本次將處理的主題: {unfinished_topics}")

        for topic_name in unfinished_topics:
            topic_data = process_topic_data(topic_name, progress_file_path)
            all_collected_data.extend(topic_data)
            

            # 檢查是否需要顯示確認按鈕
            if len(topic_data) > 0:
                prompt_message = f"應用主題<{topic_name}>已經完成，是否繼續下一個主題?"
            else:
                prompt_message = f"應用主題<{topic_name}>失敗，是否繼續下一個主題?"

            button_manager = ButtonManager()
            selected_action = button_manager.display_buttons(
                [
                    ("繼續", "繼續處理下一個應用主題"),
                    ("結束", "結束程式並儲存已收集的資料")
                ],
                message=prompt_message, # 將 prompt_message 作為 message 參數傳遞
                countdown_seconds=10 # 新增倒數計時 10 秒
            )

            if selected_action == "結束":
                logger.info("使用者選擇結束程式。")
                break # 跳出 for 迴圈，結束所有主題的處理

    except Exception as e:
        logger.info(f"執行資料抓取邏輯時發生錯誤: {e}", "critical")
    finally:
        # 確保輸出檔案的目錄存在
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        all_collected_data_with_status = check_for_updates(all_collected_data, moa_data_path=output_filename)
        logger.info(f"準備將 {len(all_collected_data_with_status)} 筆資料儲存到 '{output_filename}'。")
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(all_collected_data_with_status, f, ensure_ascii=False, indent=4)
        logger.info(f"所有提取的資料已成功儲存到 '{output_filename}'。")
        
        # 檢查所有主題是否都已完成下載，並更新 current_run_mode
        all_topics_downloaded = True
        progress_data = {}
        if os.path.exists(progress_file_path):
            with open(progress_file_path, 'r', encoding='utf-8') as f:
                try:
                    progress_data = json.load(f)
                except json.JSONDecodeError:
                    logger.info(f"無法解析進度檔案 '{progress_file_path}'，將視為無進度。")
                    all_topics_downloaded = False
        
        if all_topics_list: # 確保有主題列表才進行檢查
            for topic_name in all_topics_list:
                topic_status = progress_data.get(topic_name)
                if not isinstance(topic_status, dict) or topic_status.get('download_status') != 'completed':
                    all_topics_downloaded = False
                    break
        else: # 如果沒有主題列表，則不認為全部完成
            all_topics_downloaded = False

        global current_run_mode # 確保可以修改全域變數
        if all_topics_downloaded:
            current_run_mode = "list_finished"
            logger.info("所有應用主題的資料列表已完成下載。")
        else:
            logger.info("部分應用主題的資料列表未完成下載。")

        cleanup_resources(current_run_mode=current_run_mode) # 確保傳遞更新後的 current_run_mode

def cleanup_resources(current_run_mode: str = None): # 新增 current_run_mode 參數，並給予預設值 None
    """
    在程式關閉前執行清理工作，包括儲存 ini 資訊、關閉 driver 和 root。
    Args:
        current_run_mode (str, optional): 當前程式運行的模式。如果為 None，則不儲存 LastRun。
    """
    global driver, root, terminal_win, config, config_file
    
    # 儲存 LastRun 模式
    if current_run_mode is not None:
        if not config.has_section('LastRun'):
            config.add_section('LastRun')
        config.set('LastRun', 'mode', current_run_mode)
        with open(config_file, 'w', encoding='utf-8') as cfgfile:
            config.write(cfgfile)
        logger.info(f"LastRun 模式已儲存為 '{current_run_mode}'。")

    if 'terminal_win' in globals() and terminal_win:
        terminal_win._save_config() # 儲存終端機設定 (ini 資訊)
        logger.info("已儲存 moa_webpage.ini 設定。")
    else:
        logger.info("terminal_win 物件不存在，無法儲存 ini 設定。")

    if 'driver' in globals() and driver:
        driver.quit()
        logger.info("已關閉 WebDriver。")
    else:
        logger.info("WebDriver 物件為 None，無需關閉。")
    
    if 'root' in globals() and root:
        sys.stdout = sys.__stdout__ # Restore original stdout
        # logger.info("已關閉 Tkinter 視窗。")
        # root.quit() 暫時保留terminal
    else:
        logger.info("Tkinter root 物件為 None，無需關閉。")

if __name__ == "__main__":
    url = "https://data.moa.gov.tw/open.aspx"
    progress_file_path = "metadata/data_list_progress_status.json"
    global current_run_mode
    global root
    root = tk.Tk()
    root.title("訊息紀錄")
    config = configparser.ConfigParser()
    config_file = 'moa_webpage.ini'
    x, y, width, height = 100, 10, 1200, 200 # Default values
    font_family, font_size, font_style = 'Microsoft JhengHei', 12, 'bold' # Default font settings
    os.makedirs(os.path.dirname(progress_file_path), exist_ok=True)
    if os.path.exists(config_file):
        config.read(config_file)
        if 'Window' in config:
            x = config.getint('Window', 'x', fallback=x)
            y = config.getint('Window', 'y', fallback=y)
            width = config.getint('Window', 'width', fallback=width)
            height = config.getint('Window', 'height', fallback=height)
            font_family = config.get('Window', 'font_family', fallback=font_family)
            font_size = config.getint('Window', 'font_size', fallback=font_size)
            font_style = config.get('Window', 'font_style', fallback=font_style) # 讀取 font_style

    last_run_mode = config.get('LastRun', 'mode', fallback='start_from_scratch')

    root.geometry(f"{width}x{height}+{x}+{y}")

    root.attributes('-topmost', True)

    text_area = scrolledtext.ScrolledText(root, wrap=tk.WORD, bg="black", fg="white", font=(font_family, font_size, font_style)) # 使用 font_style
    text_area.pack(expand=True, fill="both", padx=5, pady=5)

    button_frame = tk.Frame(root, bg="gray")
    button_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=5)

    global terminal_win
    terminal_win = TerminalWindow(root, text_area, config, config_file, font_family, font_size, font_style) # 傳遞共享的 config 物件和字體設定
    sys.stdout = StdoutRedirector(terminal_win.text_area)
    setup_logging_to_tkinter(terminal_win.text_area)
    logger.info("歡迎來到農業資料開放平台 - 資料清單獲取程式") # 顯示歡迎訊息

    def setup_and_run_scraper():
        global driver, wait, last_run_mode # 宣告使用全域變數
        try:
            chrome_options = Options()
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)
            driver.maximize_window()
            wait = WebDriverWait(driver, 10)

            expand_categories() # 不再傳遞 wait
            category_texts = get_category_topics() # 不再傳遞 wait
            if not category_texts:
                logger.info("未能獲取應用主題分類，程式將結束。")
                cleanup_resources(current_run_mode="failed") # 新增呼叫清理函數，傳遞模式
                return
            
            # 提早讀取進度檔案以判斷上次執行狀態和開始主題
            logger.info(f"準備呼叫 check_and_get_unfinished_topics，progress_file_path: {progress_file_path}, category_texts 數量: {len(category_texts)}", "debug")
            topics_from_progress, has_completed_topics = check_and_get_unfinished_topics(progress_file_path, category_texts)
            logger.info(f"check_and_get_unfinished_topics 返回，topics_from_progress 數量: {len(topics_from_progress)}, has_completed_topics: {has_completed_topics}", "debug")

            # 檢查ini檔案不存在但進度檔案存在的情況
            if not os.path.exists(config_file) and has_completed_topics:
                logger.info("注意：已有下載資料但設定檔不存在，請慎重選擇選項！")

            # 顯示上次執行狀態
            mode_display_map = {
                "update_list": "更新列表",
                "continue_download": "繼續下載列表",
                "start_from_scratch": "從頭開始",
                'list_finished' : "完成列表下載",
                "failed": "上次執行失敗"
            }
            display_mode = mode_display_map.get(last_run_mode, last_run_mode)
            logger.info(f"上次執行: {display_mode}")
            if last_run_mode=="start_from_scratch":
                last_run_mode = "continue_download" #當從頭開始儲存時要設為continue_download，如果下載完成會再改動
            if topics_from_progress:
                logger.info(f"開始主題: 將從 \"{topics_from_progress[0]}\" 應用主題繼續執行")
            else:
                logger.info("沒有未完成的主題，將從頭開始執行。")

            button_manager = ButtonManager()

            # 檢查所有主題是否都已完成下載
            all_download_completed = True
            progress_data_for_check = {}
            if os.path.exists(progress_file_path):
                with open(progress_file_path, 'r', encoding='utf-8') as f:
                    try:
                        progress_data_for_check = json.load(f)
                    except json.JSONDecodeError:
                        logger.info(f"無法解析進度檔案 '{progress_file_path}'，將視為無進度。")
                        all_download_completed = False
            
            if category_texts: # 確保有主題列表才進行檢查
                for topic_name in category_texts:
                    topic_status = progress_data_for_check.get(topic_name)
                    if not isinstance(topic_status, dict) or topic_status.get('download_status') != 'completed':
                        all_download_completed = False
                        break
            else: # 如果沒有主題列表，則不認為全部完成
                all_download_completed = False

            buttons_config = [
                ("繼續下載列表", "從上次未完成的主題開始下載資料列表"),
                ("結束", "退出程式")
            ]

            update_button_text = "下載並儲存所有應用主題的資料列表"
            if not all_download_completed:
                update_button_text = "下載未完成，不可更新"
                buttons_config.insert(0, ("更新列表", update_button_text)) # 移除第三個元素
                disabled_buttons = ["更新列表"] # 設置需要禁用的按鈕
            else:
                buttons_config.insert(0, ("更新列表", update_button_text)) # 啟用按鈕
                disabled_buttons = [] # 沒有需要禁用的按鈕
            
            selected_mode = button_manager.display_buttons(buttons_config, disabled_buttons=disabled_buttons)

            if selected_mode == "更新列表":
                if not all_download_completed:
                    logger.info("下載未完成，無法執行 '更新列表' 模式。")
                    cleanup_resources(current_run_mode=last_run_mode)
                    return
                logger.info("您選擇了 '更新列表' 模式。將從上次未完成的主題開始。")
                # 初始化所有主題的下載和更新狀態
                initial_progress_data = {topic: {"download_status": "pending", "update_status": "not_checked"} for topic in category_texts}
                with open(progress_file_path, 'w', encoding='utf-8') as f:
                    json.dump(initial_progress_data, f, ensure_ascii=False, indent=4)
                logger.info(f"已初始化進度檔案 '{progress_file_path}'。")
                topics_to_process = category_texts
                current_run_mode = "update_list"
            elif selected_mode == "繼續下載列表":
                logger.info("您選擇了 '繼續下載列表' 模式。將從上次未完成的主題開始。")
                topics_to_process = topics_from_progress # 使用之前獲取到的未完成主題列表
                if not topics_to_process:
                    logger.info("沒有未完成的主題，將結束資料列表下載。")
                    current_run_mode = "list_finished" # 如果沒有未完成的主題，則視為列表已完成
                    cleanup_resources(current_run_mode=current_run_mode)
                    return
                else:
                    current_run_mode = "continue_download"
            elif selected_mode == "結束":
                logger.info("使用者選擇結束資料列表下載。")
                cleanup_resources(current_run_mode=last_run_mode)
                return
            else:
                logger.info("無效的選擇，請重新選擇操作模式。")
                cleanup_resources(current_run_mode=last_run_mode)
                return
            run_scraper_logic(topics_to_process, category_texts)
        except Exception as e:
            logger.info(f"設定和運行資料抓取器時發生錯誤: {e}", "critical")
            cleanup_resources(current_run_mode=last_run_mode) # 新增呼叫清理函數，傳遞模式
        finally:
            pass 

    scraper_setup_thread = threading.Thread(target=setup_and_run_scraper, daemon=True)
    scraper_setup_thread.start()
    root.mainloop()
