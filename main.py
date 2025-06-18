# main.py

import os
import yfinance as yf
import pandas as pd
from datetime import date, timedelta

# --- 設定項目 ---
# 取得する銘柄リスト
# ※ご自身のリストに差し替えてください
NIKKEI_225_TICKERS = ['7203.T', '9984.T', '6758.T', '9432.T', '8058.T']
GROWTH_TICKERS = ['4478.T', '4485.T', '4165.T', '7779.T', '2158.T']

# 保存するCSVファイル名
NIKKEI_CSV_PATH = 'nikkei_225_data.csv'
GROWTH_CSV_PATH = 'growth_data.csv'

def get_stock_data(tickers, file_path):
    """
    指定された銘柄リストの株価データを取得し、CSVファイルに追記・保存する関数
    """
    print(f"--- {file_path} の処理を開始 ---")
    
    # 既存のCSVファイルがあれば、最終取得日を確認
    start_fetch_date = "2025-01-01"
    if os.path.exists(file_path):
        try:
            existing_df = pd.read_csv(file_path)
            # 最終取得日の翌日からデータを取得するように設定
            last_date = pd.to_datetime(existing_df['Date']).max()
            start_fetch_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
            print(f"既存ファイルを発見。最終取得日: {last_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"既存ファイルの読み込みに失敗しました: {e}")

    today = date.today().strftime("%Y-%m-%d")
    
    if start_fetch_date > today:
        print(f"データは最新です（{start_fetch_date}）。取得をスキップします。")
        return

    print(f"{len(tickers)} 銘柄のデータを {start_fetch_date} から取得します...")
    
    # yfinanceからデータをダウンロード
    df_new = yf.download(tickers, start=start_fetch_date, end=today, auto_adjust=False, group_by='ticker')

    if df_new.empty:
        print("新規データはありませんでした。")
        return

    # データを整形
    all_data_list = []
    for ticker in tickers:
        try:
            df_ticker = df_new[ticker].copy()
            if not df_ticker.dropna().empty: # データが空でないことを確認
                df_ticker['Ticker'] = ticker.replace('.T', '')
                all_data_list.append(df_ticker)
        except KeyError:
            pass # データがない場合はスキップ

    if not all_data_list:
        print("整形後の有効なデータはありませんでした。")
        return

    new_data_df = pd.concat(all_data_list).reset_index()
    final_columns = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    new_data_df = new_data_df[[col for col in final_columns if col in new_data_df.columns]]
    new_data_df['Date'] = pd.to_datetime(new_data_df['Date']).dt.strftime('%Y-%m-%d')

    # 既存データと結合して保存
    if 'existing_df' in locals():
        combined_df = pd.concat([existing_df, new_data_df]).drop_duplicates(subset=['Date', 'Ticker'], keep='last')
    else:
        combined_df = new_data_df

    combined_df.sort_values(by=['Ticker', 'Date'], inplace=True)
    combined_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"データを {file_path} に保存しました。")


if __name__ == "__main__":
    get_stock_data(NIKKEI_225_TICKERS, NIKKEI_CSV_PATH)
    get_stock_data(GROWTH_TICKERS, GROWTH_CSV_PATH)
    print("\nすべての処理が完了しました。")
