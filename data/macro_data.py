import pandas as pd
from fredapi import Fred
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
import yfinance as yf

load_dotenv()

start_date="2020-01-01" 
end_date="2025-07-10"

# Setup logging
logging.basicConfig(
    filename='logs/macro_data.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Initialize FRED client with your API key
fred = Fred(api_key=os.getenv("FRED_API_KEY"))

def fetch_macro_series(series_id, start=start_date, end=end_date):
    if end is None:
        end = datetime.today().strftime('%Y-%m-%d')
    logging.info(f"Fetching {series_id} data from {start} to {end}")
    data = fred.get_series(series_id, observation_start=start, observation_end=end)
    df = pd.DataFrame(data, columns=[series_id])
    df.index.name = 'date'
    df.reset_index(inplace=True)
    logging.info(f"Fetched {len(df)} rows for {series_id}")
    return df

def fetch_oil_prices(start=start_date, end=end_date):
    if end is None:
        end = datetime.today().strftime('%Y-%m-%d')
    logging.info(f"Fetching oil prices (CL=F) from {start} to {end}")
    
    oil = yf.download("CL=F", start=start, end=end, progress=False, auto_adjust=False)

    if isinstance(oil.columns, pd.MultiIndex):
        oil.columns = oil.columns.droplevel(1)  # Remove the second level (ticker symbol)

    # Fix: flatten index if needed
    oil.reset_index(inplace=True)

    # Rename and clean columns
    oil = oil[['Date', 'Close']].rename(columns={'Date': 'date', 'Close': 'OIL_PRICE'})
    oil['date'] = pd.to_datetime(oil['date'])
    
    logging.info(f"Fetched {len(oil)} rows of oil prices")
    return oil

def merge_macro_data(start=start_date, end=end_date):
    dxy = fetch_macro_series('DTWEXBGS', start, end)  # Trade Weighted Dollar Index
    cpi = fetch_macro_series('CPIAUCSL', start, end)  # Consumer Price Index (for inflation)
    fed_funds = fetch_macro_series('FEDFUNDS', start, end)  # Fed Funds Rate
    gdp = fetch_macro_series('GDP', start, end)  # GDP (for economic growth)
    oil = fetch_oil_prices(start, end)

    # Merge on date
    df = dxy.merge(cpi, on='date', how='outer')
    df = df.merge(fed_funds, on='date', how='outer')
    df = df.merge(gdp, on='date', how='outer')
    df = df.merge(oil, on='date', how='outer')

    # Forward-fill missing values (some series update monthly/quarterly)
    df.ffill()
    # df.fillna(method='ffill', inplace=True)

    return df

def clean_data(df):
    """
    Clean macro data by forward-filling monthly series and handling missing values.
    
    Args:
        df: DataFrame with macro data including daily and monthly series
    
    Returns:
        DataFrame with cleaned data
    """
    df_clean = df.copy()

    df_clean['date'] = pd.to_datetime(df_clean['date'])
    
    df_clean = df_clean.sort_values('date').reset_index(drop=True)

    not_daily_columns = ['CPIAUCSL', 'FEDFUNDS']
    
    for col in not_daily_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].ffill()
            logging.info(f"Forward-filled {col} - monthly series")

    if 'GDP' in df_clean.columns:
        df_clean['GDP'] = df_clean['GDP'].ffill()
        logging.info("Forward-filled GDP - quarterly series")
    
    daily_columns = ['DTWEXBGS', 'OIL_PRICE']
    
    for col in daily_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].ffill()
            logging.info(f"Forward-filled {col} - daily series")
    
    logging.info("Data cleaning summary:")
    for col in df_clean.columns:
        if col != 'date':
            null_count = df_clean[col].isnull().sum()
            total_count = len(df_clean)
            logging.info(f"{col}: {null_count}/{total_count} null values remaining")
    
    return df_clean

if __name__ == "__main__":
    try:
        macro_df = merge_macro_data()
        macro_df = clean_data(macro_df)
        macro_df.to_csv('data/macro_data.csv', index=False)
        logging.info("Saved macro data to data/macro_data.csv")
    except Exception as e:
        logging.error(f"Macro data ingestion failed: {e}")
        raise
