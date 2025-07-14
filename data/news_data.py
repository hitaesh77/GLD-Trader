#!/usr/bin/env python3
import os
import argparse
from dotenv import load_dotenv
from newsapi import NewsApiClient
from newsapi.newsapi_exception import NewsAPIException
from datetime import datetime, timedelta
import pandas as pd

import requests
from bs4 import BeautifulSoup

# NewsAPI free plan only allows up to 30 days of historical data
MAX_NEWSAPI_DAYS = 30

def fetch_gold_news(api_key: str, days: int = 100, page_size: int = 100) -> list:
    """
    Fetch gold-related news articles from NewsAPI over the past `days` days.
    """
    client = NewsApiClient(api_key=api_key)
    # Clamp days to NewsAPI free plan limit
    if days > MAX_NEWSAPI_DAYS:
        print(f"⚠️  Requested {days} days, but NewsAPI free plan only supports last {MAX_NEWSAPI_DAYS} days. Fetching {MAX_NEWSAPI_DAYS} days instead.")
        days = MAX_NEWSAPI_DAYS
    from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    all_articles = []
    page = 1
    while True:
        try:
            resp = client.get_everything(
                q="GLD OR SPDR Gold",
                from_param=from_date,
                language="en",
                sort_by="publishedAt",
                page_size=page_size,
                page=page
            )
        except NewsAPIException as e:
            print(f"Error fetching from NewsAPI: {e}")
            break
        articles = resp.get("articles", [])
        if not articles:
            break
        all_articles.extend(articles)
        if len(articles) < page_size:
            break
        page += 1
    return all_articles


# Yahoo Finance news fetcher
def fetch_yahoo_finance_news(symbol: str = "GLD") -> list:
    """
    Fetch latest news headlines for a ticker from Yahoo Finance.
    """
    url = f"https://finance.yahoo.com/quote/{symbol}/news"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(resp.text, "html.parser")
    articles = []
    # Each news item container in Yahoo Finance stream
    for item in soup.select("li.stream-item.story-item"):
        title_tag = item.select_one("h3.clamp.yf-10mgn4g")
        desc_tag = item.select_one("p.clamp.yf-10mgn4g")
        if not title_tag or not desc_tag:
            continue
        title = title_tag.get_text().strip()
        description = desc_tag.get_text().strip()

        # Extract URL from the parent <a> of the headline
        link_parent = title_tag.find_parent("a")
        if not link_parent or not link_parent.get("href"):
            continue
        href = link_parent["href"]
        if href.startswith("/"):
            href = "https://finance.yahoo.com" + href

        date_tag = item.select_one("time")
        publishedAt = date_tag.get_text().strip() if date_tag else None

        articles.append({
            "source": "YahooFinance",
            "author": None,
            "title": title,
            "description": description,
            "url": href,
            "publishedAt": publishedAt,
            "content": None
        })
    return articles

def build_dataframe(articles: list) -> pd.DataFrame:
    """
    Convert raw articles list to a pandas DataFrame.
    """
    rows = []
    for art in articles:
        src = art.get("source")
        if isinstance(src, dict):
            src_name = src.get("name")
        else:
            src_name = src
        pub = art.get("publishedAt")
        # derive date-only (YYYY-MM-DD) if available
        if isinstance(pub, str) and "T" in pub:
            date_only = pub.split("T")[0]
        else:
            date_only = pub
        rows.append({
            "source": src_name,
            "author": art.get("author"),
            "title": art.get("title"),
            "description": art.get("description"),
            "url": art.get("url"),
            "publishedAt": pub,
            "date": date_only,
            "content": art.get("content")
        })
    return pd.DataFrame(rows)

def main(output_csv: str, days: int):
    load_dotenv()
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        raise RuntimeError("Please set NEWS_API_KEY in your environment or .env file.")
    articles = fetch_gold_news(api_key, days=days)
    # Supplement with Yahoo Finance news
    yahoo_articles = fetch_yahoo_finance_news(symbol="GLD")
    if not yahoo_articles:
        print("No Yahoo Finance articles found, using NewsAPI articles only.")
    articles.extend(yahoo_articles)
    # if not articles:
    #     print("No articles found for the specified period.")
    #     return
    df = build_dataframe(articles)
    # Reorder columns to have 'date' as the first column
    cols = ['date'] + [c for c in df.columns if c != 'date']
    df = df[cols]
    df.to_csv(output_csv, index=False)
    print(f"Wrote {len(df)} articles to {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch gold-related news for the past N days and save to CSV.")
    parser.add_argument("--days", "-d", type=int, default=100, help="Number of days in the past to fetch news for.")
    parser.add_argument("--output", "-o", default="data/gold_news.csv", help="Output CSV file path.")
    args = parser.parse_args()
    main(args.output, args.days)