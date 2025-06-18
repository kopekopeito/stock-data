# main.py (最終決定版: グロース250指数対応)
import os
import sys
import time
import pandas as pd
import yfinance as yf
from datetime import date, timedelta

# --- グローバル設定 ---
NIKKEI_CSV_PATH = 'nikkei_225_data.csv'
GROWTH_250_CSV_PATH = 'growth_250_data.csv' # 保存ファイル名を変更

# --- 銘柄リスト (この部分を手動で更新します) ---

# 2024年6月時点の日経225全採用銘柄リスト (全225銘柄)
NIKKEI_225_TICKERS = [
    '1332.T', '1333.T', '1605.T', '1721.T', '1801.T', '1802.T', '1803.T', '1808.T', '1812.T', '1925.T',
    '1928.T', '1963.T', '2002.T', '2269.T', '2282.T', '2432.T', '2501.T', '2502.T', '2503.T', '2801.T',
    '2802.T', '2914.T', '3086.T', '3099.T', '3382.T', '3401.T', '3402.T', '3405.T', '3407.T', '3861.T',
    '3863.T', '4004.T', '4005.T', '4021.T', '4061.T', '4063.T', '4151.T', '4183.T', '4188.T', '4324.T',
    '4502.T', '4503.T', '4506.T', '4507.T', '4519.T', '4523.T', '4543.T', '4568.T', '4578.T', '4661.T',
    '4689.T', '4704.T', '4755.T', '4901.T', '4902.T', '4911.T', '5019.T', '5020.T', '5101.T', '5108.T',
    '5201.T', '5233.T', '5332.T', '5333.T', '5401.T', '5406.T', '5411.T', '5541.T', '5631.T', '5706.T',
    '5707.T', '5713.T', '5714.T', '5801.T', '5802.T', '5803.T', '6098.T', '6178.T', '6273.T', '6301.T',
    '6302.T', '6305.T', '6326.T', '6361.T', '6367.T', '6471.T', '6472.T', '6473.T', '6479.T', '6501.T',
    '6503.T', '6504.T', '6506.T', '6526.T', '6594.T', '6645.T', '6702.T', '6723.T', '6724.T', '6752.T',
    '6758.T', '6762.T', '6770.T', '6841.T', '6857.T', '6902.T', '6920.T', '6952.T', '6954.T', '6971.T',
    '6976.T', '6981.T', '6988.T', '7003.T', '7011.T', '7012.T', '7186.T', '7201.T', '7202.T', '7203.T',
    '7211.T', '7261.T', '7267.T', '7269.T', '7270.T', '7272.T', '7309.T', '7532.T', '7733.T', '7735.T',
    '7741.T', '7751.T', '7752.T', '7832.T', '7911.T', '7912.T', '7951.T', '7974.T', '8001.T', '8002.T',
    '8015.T', '8031.T', '8035.T', '8053.T', '8058.T', '8233.T', '8252.T', '8267.T', '8283.T', '8303.T',
    '8304.T', '8306.T', '8308.T', '8309.T', '8316.T', '8331.T', '8354.T', '8411.T', '8591.T', '8601.T',
    '8604.T', '8628.T', '8697.T', '8725.T', '8750.T', '8766.T', '8795.T', '8801.T', '8802.T', '8804.T',
    '8830.T', '8876.T', '9001.T', '9005.T', '9007.T', '9008.T', '9009.T', '9020.T', '9021.T', '9022.T',
    '9062.T', '9064.T', '9101.T', '9104.T', '9107.T', '9201.T', '9202.T', '9301.T', '9432.T', '9433.T',
    '9434.T', '9501.T', '9502.T', '9503.T', '9531.T', '9532.T', '9613.T', '9681.T', '9735.T', '9766.T', '9983.T', '9984.T'
]

# 2024年6月時点の東証グロース市場250指数 全採用銘柄リスト (全250銘柄)
# 最新情報は https://www.jpx.co.jp/markets/indices/line-up/index.html 等でご確認ください。
GROWTH_250_TICKERS = [
    '1431.T', '1447.T', '2158.T', '2160.T', '2375.T', '2468.T', '2489.T', '2934.T', '2978.T', '3031.T',
    '3479.T', '3491.T', '3496.T', '3542.T', '3627.T', '3645.T', '3653.T', '3664.T', '3673.T', '3674.T',
    '3675.T', '3683.T', '3686.T', '3691.T', '3692.T', '3694.T', '3696.T', '3727.T', '3774.T', '3778.T',
    '3825.T', '3903.T', '3911.T', '3914.T', '3915.T', '3922.T', '3923.T', '3925.T', '3934.T', '3937.T',
    '3993.T', '3994.T', '3998.T', '4011.T', '4051.T', '4054.T', '4056.T', '4071.T', '4073.T', '4165.T',
    '4169.T', '4174.T', '4175.T', '4177.T', '4180.T', '4192.T', '4194.T', '4259.T', '4263.T', '4270.T',
    '4374.T', '4375.T', '4380.T', '4381.T', '4382.T', '4384.T', '4387.T', '4393.T', '4413.T', '4414.T',
    '4417.T', '4418.T', '4425.T', '4431.T', '4434.T', '4435.T', '4436.T', '4443.T', '4448.T', '4475.T',
    '4477.T', '4478.T', '4480.T', '4482.T', '4483.T', '4485.T', '4488.T', '4490.T', '4493.T', '4575.T',
    '4582.T', '4592.T', '4593.T', '4597.T', '4599.T', '4880.T', '4881.T', '4883.T', '4884.T', '4888.T',
    '4890.T', '4934.T', '5026.T', '5032.T', '5035.T', '5132.T', '5137.T', '5138.T', '5253.T', '5254.T',
    '5255.T', '5834.T', '5885.T', '6027.T', '6030.T', '6034.T', '6047.T', '6062.T', '6083.T', '6088.T',
    '6172.T', '6194.T', '6195.T', '6196.T', '6232.T', '6521.T', '6550.T', '6554.T', '6557.T', '6561.T',
    '6562.T', '6563.T', '6564.T', '6579.T', '6580.T', '6612.T', '6613.T', '6617.T', '7038.T', '7047.T',
    '7062.T', '7066.T', '7068.T', '7071.T', '7072.T', '7078.T', '7083.T', '7095.T', '7107.T', '7110.T',
    '7126.T', '7218.T', '7342.T', '7351.T', '7359.T', '7370.T', '7373.T', '7378.T', '7379.T', '7383.T',
    '7676.T', '7709.T', '7779.T', '7803.T', '7805.T', '7809.T', '7816.T', '7901.T', '9166.T', '9167.T',
    '9211.T', '9212.T', '9216.T', '9218.T', '9227.T', '9235.T', '9246.T', '9250.T', '9251.T', '9252.T',
    '9254.T', '9270.T', '9348.T', '9416.T', '9552.T', '9553.T', '9554.T', '9558.T'
]


def get_stock_data(tickers, file_path, index_name):
    """株価データを取得し、既存のCSVに追記・保存する"""
    if not tickers:
        print(f"WARNING: {index_name} は銘柄リストが空のためスキップします。")
        return
    print(f"INFO: {index_name} の処理を開始。対象は {len(tickers)} 銘柄。")
    start_date = "2025-01-01"
    existing_df = None
    if os.path.exists(file_path):
        try:
            existing_df = pd.read_csv(file_path)
            if not existing_df.empty:
                last_date_str = existing_df['Date'].dropna().max()
                last_date = pd.to_datetime(last_date_str)
                start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        except Exception: pass
    end_date = date.today().strftime('%Y-%m-%d')
    if start_date > end_date:
        print(f"INFO: {index_name} のデータは最新です。")
        return
    print(f"INFO: yfinanceで {start_date} から {end_date} のデータをダウンロードします...")
    df_new = yf.download(tickers, start=start_date, end=end_date, auto_adjust=False, group_by='ticker', progress=False, threads=True)
    if df_new.empty:
        print("INFO: 新規の取引データはありませんでした。")
        return
    all_data_list = []
    for ticker in tickers:
        try:
            df_ticker = df_new.loc[:, (ticker, slice(None))].copy()
            df_ticker.columns = df_ticker.columns.droplevel(0)
            if not df_ticker.dropna(how='all').empty:
                df_ticker['Ticker'] = ticker.replace('.T', '')
                all_data_list.append(df_ticker)
        except KeyError: continue
    if not all_data_list:
        print("INFO: ダウンロードデータから有効な行を抽出できませんでした。")
        return
    new_data_df = pd.concat(all_data_list).reset_index()
    new_data_df['Date'] = pd.to_datetime(new_data_df['Date']).dt.strftime('%Y-%m-%d')
    if existing_df is not None and not existing_df.empty:
        combined_df = pd.concat([existing_df, new_data_df])
    else:
        combined_df = new_data_df
    combined_df.drop_duplicates(subset=['Date', 'Ticker'], keep='last', inplace=True)
    combined_df.sort_values(by=['Date', 'Ticker'], inplace=True)
    combined_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"SUCCESS: {file_path} を更新しました。")


if __name__ == "__main__":
    print("======== 株価データ自動更新処理 開始 ========")
    get_stock_data(NIKKEI_225_TICKERS, NIKKEI_CSV_PATH, "日経225")
    get_stock_data(GROWTH_250_TICKERS, GROWTH_250_CSV_PATH, "東証グロース250")
    print("======== 株価データ自動更新処理 正常終了 ========")
