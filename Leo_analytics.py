# Author: Vikas Lamba
# Date: 2025-08-19
# Description: This module provides functions for analyzing stock data.

import pandas as pd
import pandas_market_calendars as mcal
from datetime import date, timedelta
import Leo_connection

def get_top_movers(top_n=10):
    """
    Calculates the top N winning and losing stocks over the last 5 trading days.

    Args:
        top_n (int): The number of top movers to return.

    Returns:
        tuple: A tuple containing two lists of dictionaries: (winners, losers).
               Returns (None, None) if an error occurs.
    """
    print("Calculating top movers...")
    engine = Leo_connection.get_db_engine()
    if not engine:
        print("Database connection failed.")
        return None, None

    # 1. Get the last 5 trading days from the NYSE calendar
    nyse = mcal.get_calendar('NYSE')
    # Get a schedule for a recent period to ensure we have the latest dates
    schedule = nyse.schedule(start_date=date.today() - timedelta(days=30), end_date=date.today())
    if schedule.empty:
        print("Could not fetch market calendar. There might be no recent trading days.")
        return None, None

    last_5_days = schedule.index.date.tolist()[-5:]
    start_date = last_5_days[0]
    end_date = last_5_days[-1]

    print(f"Using trading days from {start_date} to {end_date} for calculation.")

    try:
        # 2. Get start and end prices for all stocks in one query
        query = f"""
        SELECT
            s.ticker,
            sp.price_date,
            sp.adj_close_price
        FROM stocks s
        JOIN stock_prices sp ON s.id = sp.stock_id
        WHERE sp.price_date IN ('{start_date}', '{end_date}')
        """
        all_prices_df = pd.read_sql(query, engine)

        if all_prices_df.empty:
            print("No price data found for the required dates.")
            return [], []

        # 3. Pivot the data and calculate percentage change
        pivot_df = all_prices_df.pivot(index='ticker', columns='price_date', values='adj_close_price')

        # Ensure both start and end date columns exist after pivot
        if start_date not in pivot_df.columns or end_date not in pivot_df.columns:
             print("Could not find data for both start and end dates for all stocks.")
             # Fill missing values to avoid errors, though this may exclude some stocks
             pivot_df = pivot_df.reindex(columns=[start_date, end_date])

        pivot_df.dropna(inplace=True) # Drop stocks that don't have data for both dates

        pivot_df['change_pct'] = ((pivot_df[end_date] - pivot_df[start_date]) / pivot_df[start_date]) * 100

        # 4. Get the top N winners and losers
        top_winners_df = pivot_df.nlargest(top_n, 'change_pct')
        top_losers_df = pivot_df.nsmallest(top_n, 'change_pct')

        top_movers_tickers = list(top_winners_df.index) + list(top_losers_df.index)

        if not top_movers_tickers:
            print("No top movers found.")
            return [], []

        # 5. Get the 5-day price series for the top movers for the sparklines
        sparkline_query = f"""
        SELECT
            s.ticker,
            sp.price_date,
            sp.adj_close_price
        FROM stocks s
        JOIN stock_prices sp ON s.id = sp.stock_id
        WHERE s.ticker IN ({','.join([f"'{t}'" for t in top_movers_tickers])})
        AND sp.price_date >= '{start_date}'
        ORDER BY s.ticker, sp.price_date
        """
        sparkline_df = pd.read_sql(sparkline_query, engine)

        # 6. Structure the final data
        winners_list = []
        for ticker, row in top_winners_df.iterrows():
            prices = sparkline_df[sparkline_df['ticker'] == ticker]['adj_close_price'].tolist()
            winners_list.append({
                'ticker': ticker,
                'change_pct': round(row['change_pct'], 2),
                'last_5_days_prices': prices
            })

        losers_list = []
        for ticker, row in top_losers_df.iterrows():
            prices = sparkline_df[sparkline_df['ticker'] == ticker]['adj_close_price'].tolist()
            losers_list.append({
                'ticker': ticker,
                'change_pct': round(row['change_pct'], 2),
                'last_5_days_prices': prices
            })

        print(f"Successfully identified {len(winners_list)} winners and {len(losers_list)} losers.")
        return winners_list, losers_list

    except Exception as e:
        print(f"An error occurred during top movers calculation: {e}")
        return None, None

if __name__ == '__main__':
    # Test the function
    winners, losers = get_top_movers()
    if winners is not None and losers is not None:
        print("\n--- Top 10 Winners ---")
        for w in winners:
            print(f"{w['ticker']}: {w['change_pct']}% | Prices: {w['last_5_days_prices']}")

        print("\n--- Top 10 Losers ---")
        for l in losers:
            print(f"{l['ticker']}: {l['change_pct']}% | Prices: {l['last_5_days_prices']}")
