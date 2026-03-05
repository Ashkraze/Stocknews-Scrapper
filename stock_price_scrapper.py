import yfinance as yf
import pandas as pd
from datetime import datetime


def main():

    ticker = "NVDA"
    start_date = "2025-08-19"
    end_date = datetime.today().strftime("%Y-%m-%d")

    df = yf.download(ticker, start=start_date, end=end_date)

    # Flatten multiindex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.reset_index(inplace=True)

    df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume"
    }, inplace=True)

    df["return"] = df["close"].pct_change()

    file_path = "stock_prices.xlsx"

    try:
        existing_df = pd.read_excel(file_path)

        combined = pd.concat([existing_df, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"], keep="last")

        combined.to_excel(file_path, index=False)

    except FileNotFoundError:
        df.to_excel(file_path, index=False)

#    print("Stock price data updated successfully.")


if __name__ == "__main__":
    main()
