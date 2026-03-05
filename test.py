import sys
import pandas # 或是任何你安裝的第三方套件
import psycopg2
import requests
print(f"Python 執行路徑: {sys.executable}")
print(f"套件載入路徑: {pandas.__file__}")
print(f"套件載入路徑: {psycopg2.__file__}")
print(f"套件載入路徑: {requests.__file__}")