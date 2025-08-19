# Author: Vikas Lamba
# Date: 2025-08-19
# Description: This module provides functions for fetching financial news.

import feedparser

# Using Yahoo Finance RSS feed as it's more accessible than Reuters.
YAHOO_FINANCE_RSS_URL = "https://www.yahoo.com/news/rss/finance"

def get_news_headlines(top_n=10):
    """
    Fetches the top N financial news headlines from a public RSS feed.

    Args:
        top_n (int): The number of headlines to return.

    Returns:
        list: A list of dictionaries, where each dictionary contains a 'title' and 'link'.
              Returns an empty list if the feed cannot be parsed.
    """
    print("Fetching news headlines...")
    feed = feedparser.parse(YAHOO_FINANCE_RSS_URL)

    # The 'bozo' flag is set to 1 if the feed is not well-formed.
    # We also check if there are any entries.
    if feed.bozo or not feed.entries:
        print(f"Error: Could not parse the RSS feed from {YAHOO_FINANCE_RSS_URL}")
        print(f"Bozo exception type: {feed.bozo_exception}")
        return []

    headlines = []
    for entry in feed.entries[:top_n]:
        headlines.append({
            'title': entry.title,
            'link': entry.link
        })

    print(f"Successfully fetched {len(headlines)} headlines.")
    return headlines

if __name__ == '__main__':
    # Test the function
    news = get_news_headlines()
    if news:
        print("\n--- Top 10 Financial News Headlines ---")
        for i, item in enumerate(news, 1):
            print(f"{i}. {item['title']}")
            print(f"   Link: {item['link']}\n")
    else:
        print("Could not retrieve any news headlines.")
