import yfinance as yf
import pandas as pd
import os
from datetime import date, timedelta

xlfile = "stock_prices.xlsx"
TICKER = "NVDA"
today = date.today()

import pandas_market_calendars as mcal
nyse = mcal.get_calendar('NYSE')
schedule = nyse.schedule(start_date=today - timedelta(days=7), end_date=today)
last_trading_day = schedule.index[-1].date()

start = last_trading_day
end = last_trading_day + timedelta(days=1)

def fetch_prices(start=start, end=end):
    df = yf.download(TICKER, start=start, end=end, interval="1d")
    if df.empty:
        print("No data fetched. Market may have been closed.")
        return

    df = df[["Open", "Close"]].reset_index()
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

    if os.path.exists(xlfile):
        old = pd.read_excel(xlfile, engine="openpyxl")
        mergeddf = pd.concat([old, df]).drop_duplicates(subset="Date").sort_values("Date")
        mergeddf.to_excel(xlfile, index=False, engine="openpyxl")
    else:
        df.to_excel(xlfile, index=False, engine="openpyxl")

if __name__ == "__main__":
    fetch_prices()
