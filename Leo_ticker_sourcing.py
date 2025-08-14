# Author: Vikas Lamba
# Date: 2025-08-14
# Description: This module fetches S&P 500 tickers and stores them in the database.

import bs4 as bs
import requests
import Leo_connection

def get_sp500_tickers():
    """
    Scrapes the list of S&P 500 companies from Wikipedia.

    Returns:
        list: A list of dictionaries, where each dictionary contains information about a stock.
              Returns an empty list if scraping fails.
    """
    print("Fetching S&P 500 tickers from Wikipedia...")
    try:
        resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        resp.raise_for_status()  # Raise an exception for bad status codes
        soup = bs.BeautifulSoup(resp.text, 'lxml')
        table = soup.find('table', {'class': 'wikitable sortable'})

        tickers = []
        for row in table.findAll('tr')[1:]:
            cells = row.findAll('td')
            # Ensure the row has enough columns to parse the main info
            if len(cells) >= 4:
                ticker = cells[0].text.strip().replace('.', '-')
                name = cells[1].text.strip()
                sector = cells[2].text.strip()
                industry = cells[3].text.strip()

                # A final check to ensure the ticker is not empty, which might indicate a header row
                if ticker:
                    stock_info = {
                        'ticker': ticker,
                        'name': name,
                        'sector': sector,
                        'industry': industry
                    }
                    tickers.append(stock_info)
                else:
                    print(f"Skipping row with empty ticker: {row.text.strip()}")
            else:
                # Add logging for skipped rows to help with debugging
                print(f"Skipping row with insufficient columns ({len(cells)}): {row.text.strip()}")

        print(f"Successfully fetched {len(tickers)} tickers.")
        return tickers

    except requests.exceptions.RequestException as e:
        print(f"Error fetching the Wikipedia page: {e}")
        return []
    except Exception as e:
        print(f"An error occurred during scraping: {e}")
        return []

def store_tickers_to_db(tickers):
    """
    Stores a list of tickers into the 'stocks' table in the database.
    It inserts new tickers and ignores duplicates.

    Args:
        tickers (list): A list of stock information dictionaries.
    """
    if not tickers:
        print("No tickers to store.")
        return

    print("Storing tickers to the database...")
    conn = Leo_connection.get_db_connection()
    if not conn:
        print("Database connection failed. Cannot store tickers.")
        return

    try:
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO stocks (ticker, name, sector, industry)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE name=VALUES(name), sector=VALUES(sector), industry=VALUES(industry);
        """

        data_to_insert = [
            (t['ticker'], t['name'], t['sector'], t['industry'])
            for t in tickers
        ]

        cursor.executemany(insert_query, data_to_insert)
        conn.commit()

        print(f"{cursor.rowcount} records were inserted or updated in the 'stocks' table.")

    except Exception as e:
        print(f"An error occurred while storing tickers to the database: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == '__main__':
    # This block is for testing the module.
    tickers_list = get_sp500_tickers()
    store_tickers_to_db(tickers_list)
