import tkinter as tk
from tkinter import scrolledtext
import sys, os
import configparser
from logs_handle import logger
import tkinter.font # 新增導入 tkinter.font
import threading

class StdoutRedirector:
    def __init__(self, text_widget):
        self.text_widget = text_widget

    def write(self, message):
        self.text_widget.insert(tk.END, message)
        self.text_widget.see(tk.END) # Auto-scroll to the end

    def flush(self):
        pass # Required for file-like objects

class TerminalWindow:
    def __init__(self, root: tk.Tk, text_area: scrolledtext.ScrolledText, config: configparser.ConfigParser, config_file='moa_webpage.ini', font_family='Consolas', font_size=12, font_style='normal'):
        self.root = root
        self.text_area = text_area
        # 獲取當前檔案 (ui_interface.py) 的目錄
        current_file_dir = os.path.dirname(os.path.abspath(__file__))
        # 將 config_file 與當前檔案目錄結合，形成絕對路徑
        self.config_file_path = os.path.join(current_file_dir, config_file)
        self.config = config # 使用傳入的共享 config 物件

        # Default window settings (不再由 TerminalWindow 創建，但保留用於配置讀取)
        self.x = 100
        self.y = 100
        self.width = 800
        self.height = 600
        self.font_family = font_family
        self.font_size = font_size
        self.font_style = font_style

        self._load_config_from_shared() # 修改為從共享 config 讀取

        # Redirect stdout to the text area
        sys.stdout = StdoutRedirector(self.text_area)

        # Bind the closing event to save configuration
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

    def _load_config_from_shared(self): # 修改函數名稱
        # 不再需要讀取檔案，因為 config 物件已經在 moa_url_fetcher.py 中讀取
        if 'Window' in self.config:
            self.x = self.config.getint('Window', 'x', fallback=self.x)
            self.y = self.config.getint('Window', 'y', fallback=self.y)
            self.width = self.config.getint('Window', 'width', fallback=self.width)
            self.height = self.config.getint('Window', 'height', fallback=self.height)
            self.font_family = self.config.get('Window', 'font_family', fallback=self.font_family)
            self.font_size = self.config.getint('Window', 'font_size', fallback=self.font_size)
            self.font_style = self.config.get('Window', 'font_style', fallback=self.font_style)

    def _save_config(self):
        if 'Window' not in self.config:
            self.config['Window'] = {}
        
        # Get current window geometry
        # format: WIDTHxHEIGHT+X+Y
        geometry = self.root.geometry()
        parts = geometry.split('x')
        width = int(parts[0])
        height_and_pos = parts[1].split('+')
        height = int(height_and_pos[0])
        x = int(height_and_pos[1])
        y = int(height_and_pos[2])

        self.config['Window']['x'] = str(x)
        self.config['Window']['y'] = str(y)
        self.config['Window']['width'] = str(width)
        self.config['Window']['height'] = str(height)
        
        # Get current font settings from the text_area
        current_font_str = self.text_area.cget("font")
        
        # Use tkinter.font.Font to parse the font string reliably
        try:
            font_obj = tkinter.font.Font(font=current_font_str)
            saved_font_family = font_obj.cget("family")
            saved_font_size = font_obj.cget("size")
            saved_font_style = font_obj.cget("weight") # 'normal' or 'bold'
            
            # 檢查並轉換字體名稱，以確保儲存為英文名稱
            if saved_font_family == '微軟正黑體':
                saved_font_family = 'Microsoft JhengHei'

        except Exception as e:
            logger.warning(f"解析字體設定時發生錯誤: {e}，將使用預設值。")
            saved_font_family = self.font_family
            saved_font_size = self.font_size
            saved_font_style = self.font_style

        self.config['Window']['font_family'] = saved_font_family
        self.config['Window']['font_size'] = str(saved_font_size)
        self.config['Window']['font_style'] = saved_font_style

        # 使用儲存的絕對路徑來開啟檔案
        with open(self.config_file_path, 'w', encoding='utf-8') as configfile:
            self.config.write(configfile)

    def _on_closing(self):
        self._save_config()
        self.root.destroy()
        # Restore original stdout to avoid issues after window closes
        sys.stdout = sys.__stdout__


import tkinter as tk
import threading
# import queue # 移除 queue 模組的導入

def get_webpage_url_from_popup(title="輸入資訊", input_configs=None):
    """
    顯示一個 Tkinter 彈出視窗，讓使用者輸入多個資訊。
    Args:
        title (str): 彈出視窗的標題。
        input_configs (list): 包含字典的列表，每個字典定義一個輸入欄位。
                              每個字典應包含 'label' (str), 'default_value' (str),
                              和 'validation_type' (str, 例如 "numeric", "text")。
    Returns:
        dict: 使用者輸入的所有值，以 label 為 key，如果使用者取消則為 None。
    """
    root = tk.Tk()
    root.withdraw()  # 隱藏主視窗

    input_window = tk.Toplevel(root)
    input_window.title(title)
    input_window.attributes('-topmost', True) # 讓視窗先顯示在最上面

    # 儲存所有 entry_var
    entry_vars = {}
    webpage_url_result = None # 修改為 None 表示取消

    # 預設 input_configs
    if input_configs is None:
        input_configs = [
            {"label": "請輸入資料集網頁的 URL：", "default_value": "https://data.moa.gov.tw/open_detail.aspx?id=054", "validation_type": "text"}
        ]

    # 創建輸入欄位容器
    input_frame = tk.Frame(input_window)
    input_frame.pack(pady=10, padx=10, fill='both', expand=True)

    for i, field_config in enumerate(input_configs):
        label_text = field_config.get("label", "")
        default_value = field_config.get("default_value", "")
        validation_type = field_config.get("validation_type", "text")

        row_frame = tk.Frame(input_frame)
        row_frame.grid(row=i, column=0, sticky="ew", pady=5)
        input_frame.grid_columnconfigure(0, weight=1) # 讓欄位可以水平擴展

        label = tk.Label(row_frame, text=label_text, font=("Arial", 12))
        label.pack(side=tk.LEFT, padx=(0, 10)) # 標籤在左側，與輸入框有間距

        entry_var = tk.StringVar(value=default_value)
        entry = tk.Entry(row_frame, textvariable=entry_var, font=("Arial", 12))
        entry.pack(side=tk.LEFT, fill='x', expand=True) # 輸入框水平擴展

        entry_vars[label_text] = entry_var # 使用 label_text 作為 key 儲存 entry_var

        # 設置驗證
        if validation_type == "numeric":
            # 允許空字串（使用者可以清空輸入）或只包含數字
            vcmd = (input_window.register(lambda P: P.isdigit() or P == ""), '%P')
            entry.config(validate="key", validatecommand=vcmd)
        # 其他驗證類型可以在這裡添加，例如 "url"

        if i == 0: # 預設第一個輸入框獲得焦點
            entry.focus_set()
            entry.selection_range(0, tk.END)
            entry.bind("<Return>", lambda event: on_submit()) # 綁定 Enter 鍵
            entry.bind("<Escape>", lambda event: on_cancel()) # 綁定 ESC 鍵

    def on_submit():
        nonlocal webpage_url_result
        webpage_url_result = {label: var.get() for label, var in entry_vars.items()}
        input_window.attributes('-topmost', False)
        input_window.destroy()

    def on_cancel():
        nonlocal webpage_url_result
        webpage_url_result = None # 設定為 None 表示取消
        input_window.attributes('-topmost', False)
        input_window.destroy()

    # 創建一個框架來放置按鈕，使其位於底部中央
    button_container_frame = tk.Frame(input_window)
    button_container_frame.pack(pady=10)

    submit_button = tk.Button(button_container_frame, text="確定", command=on_submit, font=("Arial", 12))
    submit_button.pack(side=tk.LEFT, padx=10)

    cancel_button = tk.Button(button_container_frame, text="取消", command=on_cancel, font=("Arial", 12))
    cancel_button.pack(side=tk.RIGHT, padx=10)

    # 自動調整視窗大小
    input_window.update_idletasks() # 確保所有小部件都已佈局
    # 計算所需的寬度 (取最寬的標籤+輸入框組合)
    max_width = 0
    for i, field_config in enumerate(input_configs):
        row_frame = input_frame.grid_slaves(row=i, column=0)[0] # 獲取該行的 frame
        row_frame.update_idletasks()
        max_width = max(max_width, row_frame.winfo_width())

    # 計算所需的高度 (所有行的高度 + 按鈕框架的高度 + 上下 padding)
    total_height = input_frame.winfo_height() + button_container_frame.winfo_height() + 40 # 額外 padding
    total_width = max_width + 40 # 額外 padding

    input_window.geometry(f"{total_width}x{total_height}")
    input_window.resizable(False, False) # 禁止調整視窗大小

    # 讓視窗模態化並等待使用者輸入
    input_window.grab_set()
    root.wait_window(input_window)

    root.destroy() # 銷毀 Tkinter 視窗
    return webpage_url_result

class ButtonManager:
    def __init__(self):
        self.button_choice = None
        self.button_event = threading.Event()
        self.button_window = None # 新增一個屬性來儲存按鈕視窗

    def _add_tooltip(self, widget, text):
        # 將 tooltip 儲存在一個列表中，以便在閉包中修改
        tooltip_holder = [None]
        def enter(event):
            x, y, cx, cy = widget.bbox("insert")
            # 將 x 座標調整為從按鈕的右邊開始顯示，並向右偏移一些距離
            x = widget.winfo_rootx() + widget.winfo_width() + 10 # 調整這個值以控制向右偏移的距離
            y += widget.winfo_rooty() + 20
            tooltip = tk.Toplevel(widget)
            tooltip.wm_overrideredirect(True)
            tooltip.wm_geometry(f"+{x}+{y}")
            tooltip.wm_attributes("-topmost", True) # 確保工具提示在最上層
            label = tk.Label(tooltip, text=text, background="#FFFFEA", relief="solid", borderwidth=1,
                             font=("Consolas", 10))
            label.pack(ipadx=1)
            tooltip_holder[0] = tooltip # 將 tooltip 實例儲存到列表中
        def leave(event):
            if tooltip_holder[0]:
                tooltip_holder[0].destroy()
            tooltip_holder[0] = None # 清除列表中的 tooltip 實例
        widget.bind("<Enter>", enter)
        widget.bind("<Leave>", leave)

    def _on_button_click(self, choice):
        self.button_choice = choice
        self.button_event.set()
        if self.button_window:
            self.button_window.destroy() # 點擊按鈕後銷毀視窗

    def display_buttons(self, buttons: list[tuple[str, str]], message: str = "", countdown_seconds: int = 0, disabled_buttons: list[str] = None) -> str:
        """
        在 Tkinter 視窗中顯示按鈕並等待使用者選擇。
        Args:
            buttons: 包含 (button_text, tooltip_text) 的元組列表。
            message: 顯示在按鈕上方的提示訊息。
            countdown_seconds: 倒數計時的秒數，如果為 0 則不啟用倒數。
        Returns:
            choice: 使用者選擇的按鈕文字。
        """
        self.button_choice = None
        self.button_event.clear()

        # 創建新的 Toplevel 視窗作為按鈕的父容器
        self.button_window = tk.Toplevel()
        self.button_window.title("請選擇操作")
        self.button_window.attributes('-topmost', True) # 讓視窗保持在最上層
        self.button_window.protocol("WM_DELETE_WINDOW", lambda: self._on_button_click("離開")) # 處理視窗關閉事件

        # 如果有訊息，則顯示訊息
        if message:
            message_label = tk.Label(self.button_window, text=message, font=("Consolas", 12), wraplength=300)
            message_label.pack(padx=10, pady=10)

        # 倒數計時標籤
        countdown_label = None
        if countdown_seconds > 0:
            countdown_label = tk.Label(self.button_window, text=f"自動繼續倒數: {countdown_seconds} 秒", font=("Consolas", 12, "bold"), fg="red")
            countdown_label.pack(padx=10, pady=5)

        # 創建一個框架來放置按鈕
        button_frame = tk.Frame(self.button_window)
        button_frame.pack(padx=10, pady=10)

        if disabled_buttons is None:
            disabled_buttons = []

        for button_text, tooltip_text in buttons:
            button = tk.Button(button_frame, text=button_text,
                                command=lambda b=button_text: self._on_button_click(b),
                                font=("Consolas", 12), padx=10, pady=5)
            if button_text in disabled_buttons:
                button.config(state=tk.DISABLED) # 禁用按鈕
            button.pack(side=tk.LEFT, padx=5, pady=5)
            self._add_tooltip(button, tooltip_text)

        # 倒數計時邏輯
        if countdown_seconds > 0:
            def update_countdown(count):
                if self.button_window and countdown_label:
                    if count > 0:
                        countdown_label.config(text=f"自動繼續倒數: {count} 秒")
                        self.button_window.after(1000, update_countdown, count - 1)
                    else:
                        countdown_label.config(text="自動繼續倒數: 0 秒")
                        self._on_button_click("繼續") # 倒數結束後自動點擊「繼續」

            self.button_window.after(1000, update_countdown, countdown_seconds)

        # 啟動 Tkinter 主迴圈，等待按鈕被點擊
        self.button_window.wait_window() # 等待視窗關閉

        return self.button_choice if self.button_choice is not None else ""
    
def get_user_input_from_popup(title="輸入資訊", input_configs=None) -> dict:
    """
    顯示一個 Tkinter 彈出視窗，讓使用者輸入多個資訊。
    Args:
        title (str): 彈出視窗的標題。
        input_configs (list): 包含字典的列表，每個字典定義一個輸入欄位。
                              每個字典應包含 'label' (str), 'default_value' (str),
                              和 'validation_type' (str, 例如 "numeric", "text")。
    Returns:
        dict: 使用者輸入的所有值，以 label 為 key，如果使用者取消則為 None。
    """
    root = tk.Tk()
    root.withdraw()  # 隱藏主視窗

    input_window = tk.Toplevel(root)
    input_window.title(title)
    input_window.attributes('-topmost', True) # 讓視窗先顯示在最上面
    input_window.deiconify() # 確保視窗被映射
    input_window.lift() # 將視窗帶到最前面

    # 儲存所有 entry_var
    entry_vars = {}
    user_inputs = None # 修改為 None 表示取消

    # 預設 input_configs
    if input_configs is None:
        return None

    # 創建輸入欄位容器
    input_frame = tk.Frame(input_window)
    input_frame.pack(pady=5, padx=10, fill='both', expand=True) # 調整 pady 讓輸入框與按鈕更緊密

    for i, field_config in enumerate(input_configs):
        label_text = field_config.get("label", "")
        default_value = field_config.get("default_value", "")
        validation_type = field_config.get("validation_type", "text")
        field_width = field_config.get("width") # 新增：獲取欄位寬度設定

        row_frame = tk.Frame(input_frame)
        row_frame.grid(row=i, column=0, sticky="ew", pady=2) # 調整 pady 讓行距更緊密
        input_frame.grid_columnconfigure(0, weight=1) # 讓欄位可以水平擴展

        label = tk.Label(row_frame, text=label_text, font=("Arial", 12))
        label.pack(side=tk.LEFT, padx=(0, 10)) # 標籤在左側，與輸入框有間距

        entry_var = tk.StringVar(value=default_value)
        entry = tk.Entry(row_frame, textvariable=entry_var, font=("Arial", 12))
        
        if field_width:
            entry.config(width=field_width) # 設定指定寬度
            entry.pack(side=tk.LEFT, fill='none', expand=False) # 不水平擴展
        else:
            entry.pack(side=tk.LEFT, fill='x', expand=True) # 輸入框水平擴展

        entry_vars[label_text] = entry_var # 使用 label_text 作為 key 儲存 entry_var

        # 設置驗證
        if validation_type == "numeric":
            # 允許空字串（使用者可以清空輸入）或只包含數字
            vcmd = (input_window.register(lambda P: P.isdigit() or P == ""), '%P')
            entry.config(validate="key", validatecommand=vcmd)
        elif validation_type == "url":
            # URL 欄位的寬度現在由 field_width 控制，不再硬編碼
            pass
        # 其他驗證類型可以在這裡添加

        if i == 0: # 預設第一個輸入框獲得焦點
            entry.focus_set()
            entry.selection_range(0, tk.END)
            entry.bind("<Return>", lambda event: on_submit()) # 綁定 Enter 鍵
            entry.bind("<Escape>", lambda event: on_cancel()) # 綁定 ESC 鍵

    def on_submit():
        nonlocal user_inputs
        user_inputs = {label: var.get() for label, var in entry_vars.items()}
        input_window.attributes('-topmost', False)
        input_window.destroy()

    def on_cancel():
        nonlocal user_inputs
        user_inputs = None # 設定為 None 表示取消
        input_window.attributes('-topmost', False)
        input_window.destroy()

    # 創建一個框架來放置按鈕，使其位於底部中央
    button_container_frame = tk.Frame(input_window)
    button_container_frame.pack(pady=5) # 調整 pady 讓按鈕與輸入框更緊密

    submit_button = tk.Button(button_container_frame, text="確定", command=on_submit, font=("Arial", 12))
    submit_button.pack(side=tk.LEFT, padx=10)

    cancel_button = tk.Button(button_container_frame, text="取消", command=on_cancel, font=("Arial", 12))
    cancel_button.pack(side=tk.RIGHT, padx=10)

    # 自動調整視窗大小
    input_window.update_idletasks() # 確保所有小部件都已佈局

    # 計算所需的寬度
    # 獲取 input_frame 的建議寬度，這會考慮所有 grid 佈局的內容
    req_width = input_frame.winfo_reqwidth() + 20 # 額外左右 padding
    
    # 檢查是否存在 URL 類型的輸入欄位，或者計算出的寬度是否過小
    # 計算所需的寬度
    # 獲取 input_frame 的建議寬度，這會考慮所有 grid 佈局的內容
    total_width = input_frame.winfo_reqwidth() + 20 # 額外左右 padding
    
    # 計算所需的高度
    # 獲取 input_frame 和 button_container_frame 的建議高度總和
    req_height = input_frame.winfo_reqheight() + button_container_frame.winfo_reqheight() + 20 # 額外上下 padding
    
    input_window.geometry(f"{total_width}x{req_height}")
    # input_window.resizable(False, False) # 暫時移除，允許調整視窗大小以測試

    # 處理視窗關閉事件
    input_window.protocol("WM_DELETE_WINDOW", on_cancel)

    # 讓視窗模態化並等待使用者輸入
    input_window.grab_set()
    root.wait_window(input_window) # 等待 input_window 關閉

    root.destroy() # 銷毀 Tkinter 主視窗
    return user_inputs

class ButtonManager:
    def __init__(self):
        self.button_choice = None
        self.button_event = threading.Event()
        self.button_window = None # 新增一個屬性來儲存按鈕視窗

    def _add_tooltip(self, widget, text):
        # 將 tooltip 儲存在一個列表中，以便在閉包中修改
        tooltip_holder = [None]
        def enter(event):
            x, y, cx, cy = widget.bbox("insert")
            # 將 x 座標調整為從按鈕的右邊開始顯示，並向右偏移一些距離
            x = widget.winfo_rootx() + widget.winfo_width() + 10 # 調整這個值以控制向右偏移的距離
            y += widget.winfo_rooty() + 20
            tooltip = tk.Toplevel(widget)
            tooltip.wm_overrideredirect(True)
            tooltip.wm_geometry(f"+{x}+{y}")
            tooltip.wm_attributes("-topmost", True) # 確保工具提示在最上層
            label = tk.Label(tooltip, text=text, background="#FFFFEA", relief="solid", borderwidth=1,
                             font=("Consolas", 10))
            label.pack(ipadx=1)
            tooltip_holder[0] = tooltip # 將 tooltip 實例儲存到列表中
        def leave(event):
            if tooltip_holder[0]:
                tooltip_holder[0].destroy()
            tooltip_holder[0] = None # 清除列表中的 tooltip 實例
        widget.bind("<Enter>", enter)
        widget.bind("<Leave>", leave)

    def _on_button_click(self, choice):
        self.button_choice = choice
        self.button_event.set()
        if self.button_window:
            self.button_window.destroy() # 點擊按鈕後銷毀視窗

    def display_buttons(self, buttons: list[tuple[str, str]], message: str = "", countdown_seconds: int = 0, disabled_buttons: list[str] = None) -> str:
        """
        在 Tkinter 視窗中顯示按鈕並等待使用者選擇。
        Args:
            buttons: 包含 (button_text, tooltip_text) 的元組列表。
            message: 顯示在按鈕上方的提示訊息。
            countdown_seconds: 倒數計時的秒數，如果為 0 則不啟用倒數。
        Returns:
            choice: 使用者選擇的按鈕文字。
        """
        self.button_choice = None
        self.button_event.clear()

        # 創建新的 Toplevel 視窗作為按鈕的父容器
        self.button_window = tk.Toplevel()
        self.button_window.title("請選擇操作")
        self.button_window.attributes('-topmost', True) # 讓視窗保持在最上層
        self.button_window.protocol("WM_DELETE_WINDOW", lambda: self._on_button_click("離開")) # 處理視窗關閉事件

        # 如果有訊息，則顯示訊息
        if message:
            message_label = tk.Label(self.button_window, text=message, font=("Consolas", 12), wraplength=300)
            message_label.pack(padx=10, pady=10)

        # 倒數計時標籤
        countdown_label = None
        if countdown_seconds > 0:
            countdown_label = tk.Label(self.button_window, text=f"自動繼續倒數: {countdown_seconds} 秒", font=("Consolas", 12, "bold"), fg="red")
            countdown_label.pack(padx=10, pady=5)

        # 創建一個框架來放置按鈕
        button_frame = tk.Frame(self.button_window)
        button_frame.pack(padx=10, pady=10)

        if disabled_buttons is None:
            disabled_buttons = []

        for button_text, tooltip_text in buttons:
            button = tk.Button(button_frame, text=button_text,
                                command=lambda b=button_text: self._on_button_click(b),
                                font=("Consolas", 12), padx=10, pady=5)
            if button_text in disabled_buttons:
                button.config(state=tk.DISABLED) # 禁用按鈕
            button.pack(side=tk.LEFT, padx=5, pady=5)
            self._add_tooltip(button, tooltip_text)

        # 倒數計時邏輯
        if countdown_seconds > 0:
            def update_countdown(count):
                if self.button_window and countdown_label:
                    if count > 0:
                        countdown_label.config(text=f"自動繼續倒數: {count} 秒")
                        self.button_window.after(1000, update_countdown, count - 1)
                    else:
                        countdown_label.config(text="自動繼續倒數: 0 秒")
                        self._on_button_click("繼續") # 倒數結束後自動點擊「繼續」

            self.button_window.after(1000, update_countdown, countdown_seconds)

        # 啟動 Tkinter 主迴圈，等待按鈕被點擊
        self.button_window.wait_window() # 等待視窗關閉

        return self.button_choice if self.button_choice is not None else ""
if __name__ == "__main__":
    # 測試多個輸入欄位和驗證
    input_configs = [
        {"label": "資料集網頁 URL：", "default_value": "https://data.moa.gov.tw/open_detail.aspx?id=054", "validation_type": "url", "width": 50}, # 設置 URL 欄位寬度
        {"label": "起始頁碼：", "default_value": "1", "validation_type": "numeric", "width": 10}, # 設置起始頁碼寬度
        {"label": "結束頁碼：", "default_value": "10", "validation_type": "numeric", "width": 10}, # 設置結束頁碼寬度
        {"label": "自訂文字：", "default_value": "Hello World", "validation_type": "text"} # 不設定寬度，讓其自動擴展
    ]
    
    results = get_user_input_from_popup(title="多欄位輸入測試", input_configs=input_configs)

    if results:
        logger.notice("使用者輸入的結果：")
        for label, value in results.items():
            logger.notice(f"{label}: {value}")
    else:
        logger.info("使用者取消了輸入。")

    # 測試單一輸入欄位 (與舊版功能類似)
    print("\n--- 單一欄位輸入測試 ---")
    single_input_config = [
        {"label": "請輸入您的姓名：", "default_value": "匿名", "validation_type": "text"}
    ]
    single_result = get_user_input_from_popup(title="單一欄位輸入", input_configs=single_input_config)

    if single_result:
        logger.info(f"您的姓名是: {single_result.get('請輸入您的姓名：')}")
    else:
        logger.info("使用者取消了單一欄位輸入。")