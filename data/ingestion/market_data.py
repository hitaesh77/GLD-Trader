import yfinance as yf
import pandas as pd

def fetch_data(start, end):
    dat = yf.Ticker("GLD")
    df = dat.history(start=start, end=end)       
    df = df.dropna()
    return df

if __name__ == "__main__":
    start_date = "2020-01-01"
    end_date = "2025-07-10"
    df = fetch_data(start=start_date, end=end_date)
    df.to_csv("data/gld_daily.csv")