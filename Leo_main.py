# Author: Vikas Lamba
# Date: 2025-08-14
# Description: Main orchestration script to run the entire data pipeline for the Leo Trading Platform.

import Leo_ticker_sourcing
import Leo_data_ingestion
import Leo_indicator_calculation
import Leo_connection
import mysql.connector

def execute_sql_from_file(filepath):
    """
    Executes a SQL script file. It handles the initial database creation.
    """
    print(f"Executing SQL script from {filepath}...")

    try:
        # Read the SQL file
        with open(filepath, 'r') as f:
            sql_script = f.read()
    except FileNotFoundError:
        print(f"Error: The file {filepath} was not found.")
        return

    # Separate connection for creating the database, as it might not exist yet
    db_config = Leo_connection.DB_CONFIG.copy()
    db_name = db_config.pop('database')

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' created or already exists.")
        conn.close()
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL server to create database: {e}")
        return

    # Now connect to the specific database to create tables
    conn = Leo_connection.get_db_connection()
    if not conn:
        print("Failed to connect to the database. Aborting schema creation.")
        return

    try:
        cursor = conn.cursor()
        # Split the script into individual statements and execute them
        for statement in sql_script.split(';'):
            if statement.strip():
                # Skip the USE database line as we are already connected
                if 'USE ' in statement.upper() or 'CREATE DATABASE' in statement.upper():
                    continue
                cursor.execute(statement)
        conn.commit()
        print("Tables created successfully.")
    except mysql.connector.Error as e:
        print(f"Error executing SQL script: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def run_data_ingestion_pipeline():
    """
    Runs the data ingestion part of the pipeline.
    """
    print("\n--- Running Data Ingestion ---")
    START_DATE = "2018-07-01"
    END_DATE = "2025-07-31"

    tickers_to_process = Leo_data_ingestion.get_tickers_from_db()

    if not tickers_to_process:
        print("No tickers found in the database. Please run the ticker sourcing step first.")
        return

    for i, (stock_id, ticker_symbol) in enumerate(tickers_to_process):
        print(f"\nProcessing {ticker_symbol} ({i+1}/{len(tickers_to_process)})...")
        historical_data = Leo_data_ingestion.fetch_historical_data(ticker_symbol, START_DATE, END_DATE)

        if historical_data is not None and not historical_data.empty:
            prepared_df = Leo_data_ingestion.prepare_data_for_db(historical_data, stock_id)
            Leo_data_ingestion.store_prices_to_db(prepared_df)

def run_indicator_calculation_pipeline():
    """
    Runs the technical indicator calculation part of the pipeline.
    """
    print("\n--- Running Indicator Calculation ---")
    stock_id_list = Leo_indicator_calculation.get_stocks_to_process()

    if not stock_id_list:
        print("No stocks found for indicator calculation.")
        return

    for i, stock_id in enumerate(stock_id_list):
        print(f"\nCalculating for stock ID: {stock_id} ({i+1}/{len(stock_id_list)})...")
        prices = Leo_indicator_calculation.get_price_data_for_stock(stock_id)

        if not prices.empty:
            indicators = Leo_indicator_calculation.calculate_indicators(prices)
            if not indicators.empty:
                Leo_indicator_calculation.store_indicators_to_db(indicators)
            else:
                print(f"Not enough data to calculate indicators for stock ID {stock_id}.")
        else:
            print(f"No price data found for stock ID {stock_id}.")


def main():
    """
    Main function to orchestrate the entire data pipeline.
    """
    print("--- Starting Leo Trading Platform Data Pipeline ---")

    # Step 1: Initialize Database Schema
    print("\n[STEP 1/4] Initializing Database Schema...")
    execute_sql_from_file('Leo_schema.sql')
    print("Database schema initialization complete.")

    # Step 2: Sourcing S&P 500 Tickers
    print("\n[STEP 2/4] Sourcing S&P 500 Tickers...")
    tickers = Leo_ticker_sourcing.get_sp500_tickers()
    if tickers:
        Leo_ticker_sourcing.store_tickers_to_db(tickers)
    print("Ticker sourcing complete.")

    # Step 3: Ingesting Historical Price Data
    print("\n[STEP 3/4] Ingesting Historical Price Data...")
    run_data_ingestion_pipeline()
    print("Historical data ingestion complete.")

    # Step 4: Calculating Technical Indicators
    print("\n[STEP 4/4] Calculating Technical Indicators...")
    run_indicator_calculation_pipeline()
    print("Technical indicator calculation complete.")

    print("\n--- Leo Trading Platform Data Pipeline Finished Successfully ---")


if __name__ == '__main__':
    main()
