# main.py (最終版)
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
    """日経プロファイルから日経225の構成銘柄コードをスクレイピングする"""
    print("INFO: 日経225の構成銘柄リストを取得開始...")
    try:
        url = "https://indexes.nikkei.co.jp/nkave/index/component?idx=nk225"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        tickers = []
        rows = soup.find_all('tr')
        for row in rows:
            code_cell = row.find('div', class_='component-list-item_code')
            if code_cell and code_cell.text.strip().isdigit():
                tickers.append(f"{code_cell.text.strip()}.T")
        
        if not tickers:
            print("ERROR: 日経225の銘柄リスト取得に失敗しました(0件)。サイト構造変更の可能性。")
            return []
        
        print(f"SUCCESS: 日経225から {len(tickers)} 銘柄を取得しました。")
        return tickers
    except Exception as e:
        print(f"CRITICAL: 日経225の取得中に致命的なエラーが発生しました: {e}")
        return []

def get_growth_core_tickers():
    """みんかぶから東証グロース市場Core指数の構成銘柄をスクレイピングする"""
    print("INFO: 東証グロースCore指数の構成銘柄リストを取得開始...")
    try:
        url = "https://minkabu.jp/financial_item/tse_growth_core_index"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.select('td > a[href*="/stock/"]')
        tickers = []
        for link in links:
            href = link.get('href', '')
            code = href.split('/')[-1]
            if code.isdigit() and len(code) == 4:
                tickers.append(f"{code}.T")

        if not tickers:
            print("ERROR: グロース指数の銘柄リスト取得に失敗しました(0件)。サイト構造変更の可能性。")
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
    df_new = yf.download(tickers, start=start_date, end=end_date, auto_adjust=False, group_by='ticker')
    
    if df_new.empty:
        print("INFO: 新規の取引データはありませんでした。")
        return True

    all_data_list = []
    for ticker in tickers:
        try:
            df_ticker = df_new.loc[:, (ticker, slice(None))].copy()
            df_ticker.columns = df_ticker.columns.droplevel(0)
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
    time.sleep(2)  # サーバー負荷軽減
    growth_tickers = get_growth_core_tickers()

    # どちらかのスクレイピングに失敗したら、エラーで処理を終了させる
    if not nikkei_tickers or not growth_tickers:
        print("CRITICAL: 銘柄リストの取得に失敗したため、処理を中断します。")
        sys.exit(1) # エラーとして終了

    get_stock_data(nikkei_tickers, NIKKEI_CSV_PATH)
    time.sleep(2) # yfinanceへの連続アクセスを避ける
    get_stock_data(growth_tickers, GROWTH_CSV_PATH)
    
    print("======== 株価データ自動更新処理 正常終了 ========")
