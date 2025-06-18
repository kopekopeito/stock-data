# main.py (最終確定版)
import os
import sys
import time
import requests
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from datetime import date, timedelta

# --- グローバル設定 ---
NIKKEI_CSV_PATH = 'nikkei_225_data.csv'
GROWTH_CSV_PATH = 'growth_core_data.csv'

def get_nikkei_225_tickers():
    """Yahoo!ファイナンスから日経225の構成銘柄コードをスクレイピングする"""
    print("INFO: 日経225の構成銘柄リストを取得開始... (Yahoo!ファイナンス)")
    try:
        tickers = []
        # 1ページに50銘柄、計5ページから取得
        for i in range(1, 6):
            url = f"https://finance.yahoo.co.jp/stocks/index/000001?page={i}"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 銘柄コードが含まれる要素を抽出
            elements = soup.select('a[href*="/stocks/"]')
            for el in elements:
                href = el.get('href', '')
                code = href.replace('/stocks/', '').split('?')[0]
                if code.isdigit() and len(code) == 4:
                    tickers.append(f"{code}.T")
            time.sleep(1) # ページごとに1秒待機
        
        unique_tickers = sorted(list(set(tickers)))
        if len(unique_tickers) < 200: # 取得件数が少なすぎる場合は警告
            print(f"WARNING: 日経225の銘柄リスト取得数が想定より少ないです({len(unique_tickers)}件)。")
        if not unique_tickers:
            print("ERROR: 日経225の銘柄リスト取得に失敗しました(0件)。")
            return []
        
        print(f"SUCCESS: 日経225から {len(unique_tickers)} 銘柄を取得しました。")
        return unique_tickers
    except Exception as e:
        print(f"CRITICAL: 日経225の取得中に致命的なエラーが発生しました: {e}")
        return []

def get_growth_core_tickers():
    """auカブコム証券から東証グロース市場Core指数の構成銘柄をスクレイピングする"""
    print("INFO: 東証グロースCore指数の構成銘柄リストを取得開始... (auカブコム証券)")
    try:
        url = "https://kabu.com/investment/guide/stock/theme/tosho_growth_shijo_core_shisu.html"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        table = soup.find('table', class_='c-table')
        tickers = []
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) > 0 and cells[0].text.strip().isdigit():
                    code = cells[0].text.strip()
                    tickers.append(f"{code}.T")
        
        if not tickers:
            print("ERROR: グロース指数の銘柄リスト取得に失敗しました(0件)。")
            return []

        unique_tickers = sorted(list(set(tickers)))
        print(f"SUCCESS: グロース指数から {len(unique_tickers)} 銘柄を取得しました。")
        return unique_tickers
    except Exception as e:
        print(f"CRITICAL: グロース指数の取得中に致命的なエラーが発生しました: {e}")
        return []

def get_stock_data(tickers, file_path):
    """株価データを取得し、既存のCSVに追記・保存する"""
    if not tickers:
        print(f"WARNING: {file_path} は銘柄リストが空のためスキップします。")
        return False

    print(f"INFO: {file_path} の処理を開始。対象は {len(tickers)} 銘柄。")
    start_date = "2025-01-01"
    existing_df = None
    if os.path.exists(file_path):
        try:
            existing_df = pd.read_csv(file_path)
            if not existing_df.empty:
                last_date_str = existing_df['Date'].dropna().max()
                last_date = pd.to_datetime(last_date_str)
                start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        except Exception as e:
            print(f"WARNING: 既存ファイル {file_path} の読込に失敗({e})。最初から取得します。")

    end_date = date.today().strftime('%Y-%m-%d')
    if start_date > end_date:
        print(f"INFO: データは最新です({start_date})。処理をスキップします。")
        return True

    print(f"INFO: yfinanceで {start_date} から {end_date} のデータをダウンロードします...")
    df_new = yf.download(tickers, start=start_date, end=end_date, auto_adjust=False, group_by='ticker', progress=False)
    
    if df_new.empty:
        print("INFO: 新規の取引データはありませんでした。")
        return True

    all_data_list = []
    for ticker in tickers:
        try:
            # yfinanceのMultiIndex DataFrameから正しくデータを抽出
            df_ticker = df_new.loc[:, (ticker, slice(None))].copy()
            df_ticker.columns = df_ticker.columns.droplevel(0) # 銘柄コードの階層を削除
            if not df_ticker.dropna(how='all').empty:
                df_ticker['Ticker'] = ticker.replace('.T', '')
                all_data_list.append(df_ticker)
        except KeyError:
            continue
            
    if not all_data_list:
        print("INFO: ダウンロードデータから有効な行を抽出できませんでした。")
        return True
    
    new_data_df = pd.concat(all_data_list).reset_index()
    new_data_df['Date'] = pd.to_datetime(new_data_df['Date']).dt.strftime('%Y-%m-%d')
    
    if existing_df is not None and not existing_df.empty:
        combined_df = pd.concat([existing_df, new_data_df])
    else:
        combined_df = new_data_df
        
    combined_df.drop_duplicates(subset=['Date', 'Ticker'], keep='last', inplace=True)
    combined_df.sort_values(by=['Date', 'Ticker'], inplace=True)
    combined_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"SUCCESS: {file_path} を更新しました。合計 {len(combined_df)} 行。")
    return True

if __name__ == "__main__":
    print("======== 株価データ自動更新処理 開始 ========")
    
    nikkei_tickers = get_nikkei_225_tickers()
    time.sleep(2)
    growth_tickers = get_growth_core_tickers()

    if not nikkei_tickers or not growth_tickers:
        print("CRITICAL: 銘柄リストの取得に失敗したため、処理を中断します。")
        sys.exit(1)

    get_stock_data(nikkei_tickers, NIKKEI_CSV_PATH)
    time.sleep(2)
    get_stock_data(growth_tickers, GROWTH_CSV_PATH)
    
    print("======== 株価データ自動更新処理 正常終了 ========")
