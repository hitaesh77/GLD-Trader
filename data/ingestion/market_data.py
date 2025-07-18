import yfinance as yf
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

def fetch_data(start, end):
    dat = yf.Ticker("GLD")
    df = dat.history(start=start, end=end)       
    df = df.dropna()
    return df

def calculate_moving_average(df, window_sizes = [30, 60, 200]):
    for window in window_sizes:
        df[f"EMA_{window}"] = df['Close'].rolling(window=window).mean()
    return df

def calculate_rsi(df, period=14):
    delta = df['Close'].diff()
    upper, lower = delta.clip(lower=0), -delta.clip(upper = 0)
    avg_gain = upper.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = lower.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    df[f"rsi_{period}"] = rsi
    return df

def calculate_macd(df, fast=12, slow=26, signal=9):
    ema_fast = df['Close'].ewm(span=fast).mean()
    ema_slow = df['Close'].ewm(span=slow).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal).mean()
    histogram = macd_line - signal_line
    
    df['MACD'] = macd_line
    df['MACD_Signal'] = signal_line
    df['MACD_Histogram'] = histogram
    return df

def compute_bollinger_bands(df, period=20, k=2):
    sma = df['Close'].rolling(window=period).mean()
    std = df['Close'].rolling(window=period).std()

    df['BB_Middle'] = sma
    df['BB_Upper'] = sma + k * std
    df['BB_Lower'] = sma - k * std
    return df

def compute_momentum(df, period=10):
    df[f'Momentum_{period}'] = df['Close'] / df['Close'].shift(period) - 1
    return df

#TO DO: IMPLEMENT HYPER PARAMETERS TUNING FOR WINDOW SIZE, RSI PERIODS
def get_splits(df, train_size, test_size, step):
    for start in range(0, len(df) - train_size - test_size + 1, step):
        train = df[start : start + train_size]
        test = df[start + train_size : start + train_size + test_size]
        yield train, test


if __name__ == "__main__":
    start_date = "2020-01-01"
    end_date = "2025-07-10"
    df = fetch_data(start=start_date, end=end_date)
    df = calculate_moving_average(df, [30, 60, 200])
    df = calculate_rsi(df, 14)
    df = calculate_macd(df, 12, 26, 9)
    df = compute_bollinger_bands(df, 20, 2)
    df = compute_momentum(df, 10)
    df.to_csv("data/gld_daily.csv")
    df.dropna()