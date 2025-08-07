import csv
import numpy as np
import pandas as pd
from newsapi import NewsApiClient
from datetime import datetime, timedelta
import argparse
import requests
import os
from dotenv import load_dotenv
# from dateutil import parser as dateparser
import feedparser
import dateparser
import logging
import time

from transformers import BertTokenizer, BertForSequenceClassification

# load_dotenv()

# logging.basicConfig(
#     filename='logs/news_data.log',
#     level=logging.INFO,
#     format='%(asctime)s [%(levelname)s] %(message)s'
# )

# newsapi = NewsApiClient(api_key=os.getenv("NEWS_API_KEY"))

# QUERY = '("gold" OR "GLD" OR "gold price" OR "gold ETF" OR "safe haven" OR inflation) AND NOT "video"'

# # Fetch articles for a single day
# def fetch_articles_for_day(date_str: str, query: str, page_size: int = 10) -> list:
#     try:
#         from_str = date_str
#         to_str = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime('%Y-%m-%d')

#         logging.info(f"Fetching articles for {from_str}")

#         articles = newsapi.get_everything(
#             q=query,
#             language='en',
#             from_param=from_str,
#             to=to_str,
#             sort_by='relevancy',
#             page_size=page_size,
#         )

#         fetched = articles.get('articles', [])
#         logging.info(f"Fetched {len(fetched)} articles for {from_str}")

#         results = []
#         for article in fetched:
#             results.append({
#                 'date': from_str,
#                 'title': article['title'],
#                 'source': article['source']['name'],
#                 'url': article['url'],
#                 'description': article['description'],
#                 'content': article['content']
#             })

#         return results

#     except Exception as e:
#         logging.error(f"Failed to fetch articles for {date_str}: {e}")
#         return []

# # Fetch articles for a range of dates
# def fetch_articles_range(start_date: str, end_date: str, query: str) -> pd.DataFrame:
#     all_articles = []
#     current_date = datetime.strptime(start_date, "%Y-%m-%d")
#     end_dt = datetime.strptime(end_date, "%Y-%m-%d")

#     while current_date <= end_dt:
#         date_str = current_date.strftime('%Y-%m-%d')
#         articles = fetch_articles_for_day(date_str, query)
#         all_articles.extend(articles)
#         current_date += timedelta(days=1)
#         time.sleep(1)  # Respect rate limits

#     return pd.DataFrame(all_articles)

# # Save articles to Excel file
# def save_articles_to_excel(df: pd.DataFrame, filepath: str):
#     try:
#         df.to_excel(filepath, index=False)
#         logging.info(f"Saved {len(df)} articles to {filepath}")
#     except Exception as e:
#         logging.error(f"Failed to save articles to Excel: {e}")
#         raise


# def main(start_date: str, end_date: str, output_file: str):
#     logging.info(f"Starting news fetch from {start_date} to {end_date}")
#     df = fetch_articles_range(start_date, end_date, QUERY)
#     save_articles_to_excel(df, output_file)
#     logging.info("News fetch completed successfully.")

# if __name__ == "__main__":
#     start_date = '2020-01-01'
#     end_date = '2025-07-10'
#     output_file = "data/news_data.xlsx"
#     main(start_date, end_date, output_file)

# Load environment variables and setup logging
load_dotenv()
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")  # Set this in your .env

BASE_URL = "https://gnews.io/api/v4/search"

def fetch_news(query, from_date, to_date, max_pages=100):
    all_articles = []
    page = 1

    while page <= max_pages:
        params = {
            "q": query,
            "from": from_date,
            "to": to_date,
            "lang": "en",
            "country": "us",
            "token": GNEWS_API_KEY,
            "max": 100,  # max articles per request
            "page": page
        }

        response = requests.get(BASE_URL, params=params)
        if response.status_code != 200:
            print(f"Error (Page {page}): {response.status_code} - {response.text}")
            break

        data = response.json()
        articles = data.get("articles", [])

        if not articles:
            break

        all_articles.extend(articles)
        print(f"Fetched page {page}, total articles: {len(all_articles)}")

        page += 1
        time.sleep(1.2)  # Avoid rate limit (free plan = 1 request/sec)

    return all_articles

def save_to_csv(articles, filename):
    if not articles:
        print("No articles to save.")
        return

    df = pd.DataFrame([{
        "title": a["title"],
        "description": a["description"],
        "content": a["content"],
        "publishedAt": a["publishedAt"],
        "source": a["source"]["name"],
        "url": a["url"]
    } for a in articles])

    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} articles to {filename}")

def run(query, start_date, end_date, output_file):
    all_articles = []

    # GNews only supports a ~1-month range per query, so chunk by month
    current_start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current_start <= end:
        current_end = min(current_start + timedelta(days=29), end)
        print(f"Fetching {current_start.date()} to {current_end.date()}")
        chunk = fetch_news(query, current_start.date(), current_end.date())
        all_articles.extend(chunk)
        current_start = current_end + timedelta(days=1)
        time.sleep(1)  # extra safety

    save_to_csv(all_articles, output_file)

# Example usage:
if __name__ == "__main__":
    run(
        query="gold OR gold price OR GLD OR Gold ETF OR inflation OR recession OR interest rates OR safe haven OR gold market OR gold futures OR gold investing OR market",
        start_date="2020-01-01",
        end_date="2025-07-10",
        output_file="gold_news_gnews.csv"
    )