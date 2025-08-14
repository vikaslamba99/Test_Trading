# Author: Vikas Lamba
# Date: 2025-08-14
# Description: This module calculates technical indicators for the stored stock prices.

import pandas as pd
import pandas_ta as ta
import Leo_connection
from sqlalchemy.orm import sessionmaker

def get_stocks_to_process():
    """
    Retrieves the list of stock IDs from the 'stocks' table.

    Returns:
        list: A list of stock IDs.
    """
    print("Retrieving list of stocks to process...")
    conn = Leo_connection.get_db_connection()
    if not conn:
        print("Database connection failed.")
        return []

    stock_ids = []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM stocks")
        # fetchall returns a list of tuples, e.g., [(1,), (2,)]
        stock_ids = [item[0] for item in cursor.fetchall()]
        print(f"Found {len(stock_ids)} stocks to process.")
    except Exception as e:
        print(f"An error occurred while fetching stock list: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
    return stock_ids

def get_price_data_for_stock(stock_id):
    """
    Fetches price data for a specific stock from the database.

    Args:
        stock_id (int): The ID of the stock.

    Returns:
        pandas.DataFrame: A DataFrame with the price data, sorted by date.
    """
    engine = Leo_connection.get_db_engine()
    if not engine:
        print("Could not get database engine.")
        return pd.DataFrame()

    query = f"""
    SELECT id, price_date, open_price, high_price, low_price, close_price, volume
    FROM stock_prices
    WHERE stock_id = {stock_id}
    ORDER BY price_date ASC
    """
    try:
        df = pd.read_sql(query, engine)
        df.set_index('price_date', inplace=True)
        # pandas_ta expects lower case column names
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        print(f"Error fetching price data for stock ID {stock_id}: {e}")
        return pd.DataFrame()

def calculate_indicators(price_df):
    """
    Calculates technical indicators using pandas_ta.

    Args:
        price_df (pandas.DataFrame): DataFrame with OHLCV data.

    Returns:
        pandas.DataFrame: DataFrame with calculated indicators and stock_price_id.
    """
    if price_df.empty:
        return pd.DataFrame()

    # Calculate indicators using the pandas_ta extension
    price_df.ta.ema(length=21, append=True)
    price_df.ta.ema(length=50, append=True)
    price_df.ta.ema(length=200, append=True)
    price_df.ta.rsi(append=True)
    # MACD calculation returns a DataFrame with MACD, histogram, and signal line
    macd = price_df.ta.macd(append=False)

    # Prepare the DataFrame for database insertion
    indicators_df = pd.DataFrame(index=price_df.index)
    indicators_df['stock_price_id'] = price_df['id']
    indicators_df['ema_21'] = price_df.get('EMA_21')
    indicators_df['ema_50'] = price_df.get('EMA_50')
    indicators_df['ema_200'] = price_df.get('EMA_200')
    indicators_df['rsi'] = price_df.get('RSI_14')
    if macd is not None and not macd.empty:
        indicators_df['macd'] = macd.get('MACD_12_26_9')
        indicators_df['signal_line'] = macd.get('MACDs_12_26_9')

    # Drop rows with any NaN values (typically at the start of the data)
    indicators_df.dropna(inplace=True)

    return indicators_df

def store_indicators_to_db(df):
    """
    Stores the calculated indicators into the 'technical_indicators' table.
    Deletes existing indicators for the given stock_price_ids before inserting.

    Args:
        df (pandas.DataFrame): A DataFrame with the formatted indicator data.
    """
    if df.empty:
        return

    engine = Leo_connection.get_db_engine()
    if not engine:
        print("Could not get database engine. Aborting storage.")
        return

    stock_price_ids = df['stock_price_id'].tolist()

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Start a transaction
        session.begin()

        # Delete existing records to prevent duplicates and ensure fresh data
        if stock_price_ids:
            delete_query = f"DELETE FROM technical_indicators WHERE stock_price_id IN ({','.join(map(str, stock_price_ids))})"
            session.execute(delete_query)

        # Insert new records
        df.to_sql('technical_indicators', engine, if_exists='append', index=False)

        session.commit()
        print(f"Stored {len(df)} indicator records.")
    except Exception as e:
        print(f"An error occurred during indicator storage: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == '__main__':
    stock_id_list = get_stocks_to_process()

    for i, stock_id in enumerate(stock_id_list):
        print(f"\nProcessing stock {i+1}/{len(stock_id_list)} (ID: {stock_id})...")
        prices = get_price_data_for_stock(stock_id)

        if not prices.empty:
            indicators = calculate_indicators(prices)
            if not indicators.empty:
                store_indicators_to_db(indicators)
            else:
                print(f"Not enough data to calculate indicators for stock ID {stock_id}.")
        else:
            print(f"No price data found for stock ID {stock_id}.")

    print("\nTechnical indicator calculation process complete.")
