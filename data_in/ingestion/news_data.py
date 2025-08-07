#!/usr/bin/env python3
import csv
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import argparse
import requests
import os
from dotenv import load_dotenv
import time
import json

from transformers import BertTokenizer, BertForSequenceClassification

# Load environment variables
load_dotenv()

def clean_final_gold_data(input_file: str, output_file: str):
    """
    Clean final_gold_data.csv to only include data from 2020 onwards
    with just date, currency, and headlines columns.
    """
    try:
        print(f"Reading data from {input_file}...")
        
        # Read the CSV file with semicolon delimiter
        df = pd.read_csv(input_file, delimiter=';')
        
        print(f"Original data: {len(df)} rows")
        print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filter for 2020 and after
        df_filtered = df[df['timestamp'].dt.year >= 2020].copy()
        
        print(f"After 2020+ filter: {len(df_filtered)} rows")
        
        # Select only the required columns: date, currency, headlines
        cleaned_df = df_filtered[['timestamp', 'currency', 'headlines']].copy()
        
        # Rename columns for clarity
        cleaned_df.columns = ['date', 'currency', 'headlines']
        
        # Format date as YYYY-MM-DD
        cleaned_df['date'] = cleaned_df['date'].dt.strftime('%Y-%m-%d')
        
        # Remove rows where headlines are empty or NaN
        cleaned_df = cleaned_df.dropna(subset=['headlines'])
        cleaned_df = cleaned_df[cleaned_df['headlines'].str.strip() != '']
        
        # Sort by date
        cleaned_df = cleaned_df.sort_values('date')
        
        # Reset index
        cleaned_df = cleaned_df.reset_index(drop=True)
        
        # Save to CSV
        cleaned_df.to_csv(output_file, index=False)
        
        print(f"Successfully cleaned gold data!")
        print(f"Final entries: {len(cleaned_df)}")
        print(f"Date range: {cleaned_df['date'].min()} to {cleaned_df['date'].max()}")
        print(f"Currencies: {cleaned_df['currency'].unique()}")
        print(f"Saved to: {output_file}")
        
        # Show first few entries as preview
        print("\nPreview of cleaned data:")
        for idx, row in cleaned_df.head(5).iterrows():
            print(f"  {row['date']} | {row['currency']} | {row['headlines'][:80]}...")
            
        # Show statistics by year
        cleaned_df['year'] = pd.to_datetime(cleaned_df['date']).dt.year
        year_counts = cleaned_df['year'].value_counts().sort_index()
        print(f"\nData by year:")
        for year, count in year_counts.items():
            print(f"  {year}: {count} entries")
        
        return cleaned_df
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        return None
    except Exception as e:
        print(f"Error processing file: {e}")
        return None

def process_temp_file():
    """
    Original function to process temp.txt file (kept for compatibility)
    """
    try:
        # Read lines from temp.txt
        with open("/Users/akhilkagithapu/Downloads/GLD-Trader/data/temp.txt", "r", encoding="utf-8") as file:
            lines = [line.strip() for line in file.readlines() if line.strip()]
        entries = [lines[i:i+3] for i in range(0, len(lines), 3)]

        # Convert to (date, outlet, headline)
        rows = []
        for entry in entries:
            if len(entry) != 3:
                continue
            headline, outlet, date_str = entry
            try:
                # Try parsing with year first, if fails, assume year 2025
                try:
                    parsed_date = datetime.strptime(date_str.replace(".", ""), "%a, %b %d, %Y")
                except ValueError:
                    parsed_date = datetime.strptime(date_str.replace(".", ""), "%a, %b %d")
                    parsed_date = parsed_date.replace(year=2025)
                formatted_date = parsed_date.strftime("%m/%d/%Y")
                rows.append([formatted_date, outlet, headline])
            except Exception as e:
                print(f"Skipping entry due to date parsing error: {entry}")

        # Output CSV
        output_file = "gld_headlines.csv"
        with open(output_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        print(f"CSV written to {output_file}")
        
    except FileNotFoundError:
        print("temp.txt file not found - skipping temp file processing")

def fetch_recent_news_data(start_date='2024-04-01', end_date=None):
    """
    Fetch news data from April 2024 to present using multiple sources
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"Fetching news data from {start_date} to {end_date}...")
    
    all_headlines = []
    
    # Try Alpha Vantage first (best for GLD-specific news)
    alpha_headlines = fetch_alpha_vantage_news(start_date, end_date)
    all_headlines.extend(alpha_headlines)
    
    # Try NewsAPI as backup
    newsapi_headlines = fetch_newsapi_data(start_date, end_date)
    all_headlines.extend(newsapi_headlines)
    
    # Try Yahoo Finance for supplementary data
    yahoo_headlines = fetch_yahoo_finance_data()
    all_headlines.extend(yahoo_headlines)
    
    return all_headlines

def fetch_alpha_vantage_news(start_date, end_date):
    """
    Fetch GLD-specific news from Alpha Vantage
    """
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        print("Alpha Vantage API key not found. Skipping...")
        return []
    
    url = "https://www.alphavantage.co/query"
    params = {
        'function': 'NEWS_SENTIMENT',
        'tickers': 'GLD',
        'limit': 1000,
        'apikey': api_key
    }
    
    try:
        print("Fetching from Alpha Vantage...")
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        headlines = []
        if 'feed' in data:
            for item in data['feed']:
                pub_date = item.get('time_published', '')[:10]  # YYYY-MM-DD
                
                # Filter by date range
                if start_date <= pub_date <= end_date:
                    # Check if specifically about GLD
                    title = item.get('title', '').lower()
                    summary = item.get('summary', '').lower()
                    
                    if any(term in title or term in summary for term in ['gld', 'spdr gold', 'gold etf']):
                        headlines.append({
                            'date': pub_date,
                            'currency': 'USD',  # GLD is USD-denominated
                            'headlines': item.get('title', '')
                        })
        
        print(f"Found {len(headlines)} headlines from Alpha Vantage")
        return headlines
        
    except Exception as e:
        print(f"Error fetching Alpha Vantage data: {e}")
        return []

def fetch_newsapi_data(start_date, end_date):
    """
    Fetch GLD news from NewsAPI
    """
    api_key = os.getenv('NEWS_API_KEY')
    if not api_key:
        print("NewsAPI key not found. Skipping...")
        return []
    
    url = "https://newsapi.org/v2/everything"
    
    headlines = []
    
    # NewsAPI free tier only allows 30 days back, so we need to chunk the requests
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    current_date = end_dt
    
    while current_date >= start_dt:
        from_date = current_date.strftime('%Y-%m-%d')
        to_date = (current_date + timedelta(days=29)).strftime('%Y-%m-%d')
        
        params = {
            'q': 'GLD OR "SPDR Gold Trust" OR "SPDR Gold ETF"',
            'from': from_date,
            'to': min(to_date, end_date),
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': 100,
            'apiKey': api_key
        }
        
        try:
            print(f"Fetching NewsAPI data for {from_date} to {min(to_date, end_date)}...")
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            
            if data.get('status') == 'ok':
                for article in data.get('articles', []):
                    pub_date = article.get('publishedAt', '')[:10]
                    headlines.append({
                        'date': pub_date,
                        'currency': 'USD',
                        'headlines': article.get('title', '')
                    })
            
            time.sleep(1)  # Rate limiting
            current_date -= timedelta(days=30)
            
        except Exception as e:
            print(f"Error fetching NewsAPI data: {e}")
            break
    
    print(f"Found {len(headlines)} headlines from NewsAPI")
    return headlines

def fetch_yahoo_finance_data():
    """
    Fetch recent GLD news from Yahoo Finance
    """
    url = "https://finance.yahoo.com/quote/GLD/news"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    try:
        print("Fetching from Yahoo Finance...")
        response = requests.get(url, headers=headers, timeout=30)
        
        # This would need BeautifulSoup parsing - simplified for now
        # Return empty for now, can implement scraping later
        headlines = []
        
        print(f"Found {len(headlines)} headlines from Yahoo Finance")
        return headlines
        
    except Exception as e:
        print(f"Error fetching Yahoo Finance data: {e}")
        return []

def setup_daily_data_collection():
    """
    Set up automated daily data collection
    This function can be called by a cron job or scheduler
    """
    print("Setting up daily data collection...")
    
    # Get yesterday's date (assuming we run this daily)
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Fetch new headlines
    new_headlines = fetch_recent_news_data(yesterday, today)
    
    if new_headlines:
        # Convert to DataFrame
        df = pd.DataFrame(new_headlines)
        
        # Append to existing file or create new one
        output_file = "data/daily_headlines.csv"
        
        if os.path.exists(output_file):
            # Append to existing file
            existing_df = pd.read_csv(output_file)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            # Remove duplicates
            combined_df = combined_df.drop_duplicates(subset=['date', 'headlines'])
            combined_df.to_csv(output_file, index=False)
            print(f"Appended {len(new_headlines)} new headlines to {output_file}")
        else:
            # Create new file
            df.to_csv(output_file, index=False)
            print(f"Created new file {output_file} with {len(new_headlines)} headlines")
    else:
        print("No new headlines found")

def backfill_historical_data(start_date='2024-04-01'):
    """
    Backfill historical data from April 2024 to present
    """
    print(f"Backfilling historical data from {start_date}...")
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    headlines = fetch_recent_news_data(start_date, end_date)
    
    if headlines:
        # Convert to DataFrame
        df = pd.DataFrame(headlines)
        
        # Clean and deduplicate
        df = df.dropna(subset=['headlines'])
        df = df[df['headlines'].str.strip() != '']
        df = df.drop_duplicates(subset=['date', 'headlines'])
        df = df.sort_values('date')
        
        # Save to file
        output_file = "data/historical_headlines_2024.csv"
        df.to_csv(output_file, index=False)
        
        print(f"Saved {len(df)} historical headlines to {output_file}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        
        return df
    else:
        print("No historical headlines found")
        return pd.DataFrame()
    
def finbert_sentiment_analysis(csv_path: str):
    finbert = BertForSequenceClassification.from_pretrained('yiyanghkust/finbert-tone', num_labels=3)
    tokenizer = BertTokenizer.from_pretrained('yiyanghkust/finbert-tone')
    labels_map = {0: 'negative', 1: 'neutral', 2: 'positive'}

    df = pd.read_csv(csv_path, encoding='utf-8')
    sentiment_results = []

    for _, row in df.iterrows():
        headline_texts = [h.strip() for h in str(row['headlines']).split('/') if h.strip()]
        sentiments = []

        for text in headline_texts:
            inputs = tokenizer(text, return_tensors='pt', truncation=True, max_length=512)
            outputs = finbert(**inputs)
            pred = outputs.logits.argmax(dim=1).item()
            sentiments.append(labels_map[pred])

        sentiment_results.append(" / ".join(sentiments))

    df['sentiments'] = sentiment_results

    # Generate output file path
    base_name = os.path.basename(csv_path)
    name_part = os.path.splitext(base_name)[0]
    output_file = f"finbert_{name_part}.csv"

    df.to_csv(output_file, index=False)
    print(f"FinBERT sentiment output written to {output_file}")


# New function to compute average sentiment score only
def compute_avg_sentiment_only(csv_path: str):
    df = pd.read_csv(csv_path, encoding='utf-8')
    avg_sentiment_scores = []

    sentiment_to_score = {'negative': -1, 'neutral': 0, 'positive': 1}

    for sentiments in df['sentiments']:
        labels = [s.strip() for s in str(sentiments).split('/') if s.strip()]
        scores = [sentiment_to_score.get(s, 0) for s in labels]
        avg_score = sum(scores) / len(scores) if scores else 0
        avg_sentiment_scores.append(avg_score)

    df['avg_sentiment_score'] = avg_sentiment_scores
    df.to_csv(csv_path, index=False)
    print(f"Appended avg_sentiment_score to {csv_path}")



def main():
    parser = argparse.ArgumentParser(description="Clean gold data files and fetch recent news")
    parser.add_argument("--input", "-i", 
                       default="data/final_gold_data.csv", 
                       help="Input CSV file path")
    parser.add_argument("--output", "-o", 
                       default="data/cleaned_gold_data_2020_plus.csv", 
                       help="Output CSV file path")
    parser.add_argument("--process-temp", action="store_true",
                       help="Also process temp.txt file")
    parser.add_argument("--backfill", action="store_true",
                       help="Backfill historical data from April 2024 to now")
    parser.add_argument("--daily", action="store_true",
                       help="Run daily data collection (yesterday to today)")
    parser.add_argument("--start-date", 
                       default="2024-04-01",
                       help="Start date for historical data collection (YYYY-MM-DD)")
    
    
    args = parser.parse_args()
    # finbert_sentiment_analysis("/Users/akhilkagithapu/Downloads/GLD-Trader/data/gold_news.csv")
    compute_avg_sentiment_only("data/finbert_gold_news.csv")
    return
    # Clean the final_gold_data.csv file (default action)
    if not args.backfill and not args.daily:
        print("Starting gold data cleaning...")
        cleaned_df = clean_final_gold_data(args.input, args.output)
        
        # Optionally process temp file
        if args.process_temp:
            print("\nProcessing temp.txt file...")
            process_temp_file()
    
    # Backfill historical data
    if args.backfill:
        print(f"\nBackfilling historical data from {args.start_date}...")
        historical_df = backfill_historical_data(args.start_date)
        
    # Run daily collection
    if args.daily:
        print("\nRunning daily data collection...")
        setup_daily_data_collection()

if __name__ == "__main__":
    main()