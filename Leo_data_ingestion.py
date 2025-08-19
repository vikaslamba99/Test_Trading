# Author: Vikas Lamba
# Date: 2025-08-14
# Description: This module fetches historical stock prices and stores them in the database.

import yfinance as yf
import pandas as pd
from datetime import datetime
from sqlalchemy import text
import Leo_connection

def get_tickers_from_db():
    """
    Retrieves the list of stock tickers from the 'stocks' table.

    Returns:
        list: A list of tuples, where each tuple contains (stock_id, ticker).
              Returns an empty list if fetching fails.
    """
    print("Retrieving tickers from the database...")
    conn = Leo_connection.get_db_connection()
    if not conn:
        print("Database connection failed.")
        return []

    tickers = []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, ticker FROM stocks")
        tickers = cursor.fetchall()
        print(f"Found {len(tickers)} tickers in the database.")
    except Exception as e:
        print(f"An error occurred while fetching tickers: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
    return tickers

def fetch_historical_data(ticker, start_date, end_date):
    """
    Fetches historical OHLCV data for a given ticker from Yahoo Finance.

    Args:
        ticker (str): The stock ticker symbol.
        start_date (str): The start date in 'YYYY-MM-DD' format.
        end_date (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        pandas.DataFrame: A DataFrame containing the historical data, or None if fetching fails.
    """
    print(f"Fetching historical data for {ticker} from {start_date} to {end_date}...")
    try:
        # Set auto_adjust=False to ensure the 'Adj Close' column is returned,
        # which is expected by the rest of the script.
        data = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)
        if data.empty:
            print(f"No data found for ticker {ticker}. It might be delisted or the ticker is incorrect.")
            return None
        return data
    except Exception as e:
        print(f"An error occurred while fetching data for {ticker}: {e}")
        return None

def prepare_data_for_db(df, stock_id):
    """
    Prepares the DataFrame for insertion into the 'stock_prices' table.

    Args:
        df (pandas.DataFrame): The DataFrame with historical data.
        stock_id (int): The ID of the stock.

    Returns:
        pandas.DataFrame: The formatted DataFrame.
    """
    df = df.copy()

    # yfinance returns a multi-level column index for single-ticker downloads.
    # This checks for that and flattens the columns to a single level.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    df.reset_index(inplace=True)
    df['stock_id'] = stock_id

    # Rename columns to match the database schema
    df.rename(columns={
        'Date': 'price_date',
        'Open': 'open_price',
        'High': 'high_price',
        'Low': 'low_price',
        'Close': 'close_price',
        'Adj Close': 'adj_close_price',
        'Volume': 'volume'
    }, inplace=True)

    # Ensure date format is correct
    df['price_date'] = pd.to_datetime(df['price_date']).dt.date

    # Select only the required columns in the correct order
    required_columns = [
        'stock_id', 'price_date', 'open_price', 'high_price',
        'low_price', 'close_price', 'adj_close_price', 'volume'
    ]
    return df[required_columns]

def store_prices_to_db(df):
    """
    Stores the historical price data into the 'stock_prices' table.
    Uses a temporary table to handle potential duplicates efficiently.

    Args:
        df (pandas.DataFrame): A DataFrame with the formatted price data.
    """
    if df.empty:
        return

    engine = Leo_connection.get_db_engine()
    if not engine:
        print("Could not get database engine. Aborting storage.")
        return

    print(f"Storing {len(df)} price records...")

    try:
        with engine.connect() as conn:
            with conn.begin(): # Start a transaction
                # Create a temporary table
                temp_table_name = "temp_stock_prices"
                df.to_sql(temp_table_name, conn, if_exists='replace', index=False)

                # Use INSERT IGNORE to skip duplicates based on the unique key (stock_id, price_date)
                insert_sql = f"""
                INSERT IGNORE INTO stock_prices (stock_id, price_date, open_price, high_price, low_price, close_price, adj_close_price, volume)
                SELECT stock_id, price_date, open_price, high_price, low_price, close_price, adj_close_price, volume
                FROM {temp_table_name};
                """
                result = conn.execute(text(insert_sql))
                print(f"Successfully inserted {result.rowcount} new price records.")

                # Drop the temporary table, ensuring it's also a text object
                conn.execute(text(f"DROP TABLE {temp_table_name}"))

    except Exception as e:
        print(f"An error occurred during database insertion: {e}")


if __name__ == '__main__':
    START_DATE = "2018-07-01"
    # Set end date far in the future to get all available data up to today
    END_DATE = "2025-07-31"

    tickers_to_process = get_tickers_from_db()

    for stock_id, ticker_symbol in tickers_to_process:
        historical_data = fetch_historical_data(ticker_symbol, START_DATE, END_DATE)

        if historical_data is not None and not historical_data.empty:
            prepared_df = prepare_data_for_db(historical_data, stock_id)
            store_prices_to_db(prepared_df)

    print("\nData ingestion process complete.")
