#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 19 10:32:24 2024

@author: vikaslamba
"""
import pickle
import requests
import yfinance as yf
import bs4 as bs
import mysql.connector as mysql
import os
from pandas_datareader import data as pdr
import datetime as dt
import math
import numpy as np
from connection_pool import get_con_alchemy
from date_qualifier import qualify_date
import pandas as pd
import quantstats as qs

initial_investment = 1000000.00
portfolio_value = 0
benchmark_units = 0

def save_sp500_tickers():
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text.replace('.', '-')
        ticker = ticker[:-1]
        tickers.append(ticker)
    with open("sp500tickers.pickle", "wb") as f:
        pickle.dump(tickers, f)
    return tickers


def save_all_US_tickers():
    df = pd.read_csv("stocks_and_commodities/leftover_us_stocks_clean.csv", index_col=False)
    US_tickers = df['Symbol']
    with open("all_us_tickers.pickle", "wb") as f:
        pickle.dump(US_tickers, f)
    return US_tickers


def get_ticker_list(reload_sp500=False):
    with open("sp500tickers.pickle", "rb") as f:
        tickers = pickle.load(f)
    return tickers


def get_all_us_ticker_list(reload_all_us=False):
    with open("all_us_tickers.pickle", "rb") as f:
        tickers = pickle.load(f)
    return tickers
"""
    reload_sp500 is set to false in the below code because the SP500 stocks are already loaded.
"""

"""
    The below code plucks the S&P 500 stock prices from Yahoo Finance and 
    saves them to  the database with the name of the stock ticker and to the MySQL database table daily_price. 
    The prices such as Open, High, Low & Close are stored in the file
    for each of the trading dates. Window length for moving average
"""
def get_data_from_yahoo(reload_sp500=False):
    db = get_con_alchemy()

    headers = {
    'Content-Type': 'application/json'
    }
    window_length = 14
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')
    start_d = dt.datetime(1999, 1, 1)
    end_d = dt.datetime.now()
    for ticker in tickers:
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
            print('The ticker we have come inside for is, : ', ticker)
            df = pdr.get_data_yahoo(ticker, start_d, end_d)
            df = df.rename(columns = {'Adj Close': 'Adj_Close'})

            delta = df['Adj_Close'].diff()
            delta = delta[1:]
            
            # Make the positive gains (up) and negative gains (down) Series
            up, down = delta.copy(), delta.copy()
            up[up < 0] = 0
            down[down > 0] = 0
            
            # Calculate the EWMA
            roll_up1 = up.ewm(span=window_length).mean()
            roll_down1 = down.abs().ewm(span=window_length).mean()
            
            # Calculate the RSI based on EWMA
            RS1 = roll_up1 / roll_down1
            RSI1 = 100.0 - (100.0 / (1.0 + RS1))      
            # Derive daily return percentage  
            df['Daily_Return'] = df['Adj_Close'].pct_change()
            df['Monthly_Return'] = df['Adj_Close'].resample('M').ffill().pct_change()
            
            # Derive the Exponential Moving Averages
            df['EMA21'] = df['Adj_Close'].ewm(span=21, adjust=False).mean().astype('float')
            df['EMA50'] = df['Adj_Close'].ewm(span=50, adjust=False).mean().astype('float')
            df['EMA200'] = df['Adj_Close'].ewm(span=200, adjust=False).mean().astype('float')
            df['RS'] = RS1.astype('float')
            df['RS'] = round(df['RS'].replace([np.inf, -np.inf], 0), 5)
            df['RSI'] = RSI1.astype('float')
            df['ticker'] = ticker
            df['data_vendor_id'] = 100
            
            pfe_length = 10
            pfe_period = pfe_length - 1
            pfe_tmp_values = []
            
            for k in range(0, 9):
                pfe_tmp_values.append(0)
            for i in range(9, len(df)):
                diff = df['Adj_Close'].iloc[i] - df['Adj_Close'].iloc[i-pfe_period]
                pfetmp1 = 100 * math.sqrt(pow(diff,2) + pow(pfe_period,2))
                pfetmp2 = 0
                for j in range(0, pfe_period-1):
                    pfetmp2 += math.sqrt(1 + pow(df['Adj_Close'].iloc[i-j] - df['Adj_Close'].iloc[i-j-1],2))
                pfe_tmp = pfetmp1 / pfetmp2
                if diff < 0:
                    pfe_tmp = -pfe_tmp
                pfe_tmp_values.append(pfe_tmp)          
            df['PFE'] = pfe_tmp_values
            df['PFE'] = df['PFE'].ewm(span=5, adjust=False).mean()
            change = df['Adj_Close'].pct_change()
            slope = change/df['Adj_Close']
            ema_slope_f = slope.ewm(span=9, adjust=False).mean()
            ema_slope_m = slope.ewm(span=21, adjust=False).mean()
            ema_slope_s = slope.ewm(span=200, adjust=False).mean()
            df['Slope_Fast'] = ema_slope_f
            df['Slope_Medium'] = ema_slope_m
            df['Slope_Slow'] = ema_slope_s
            df.to_sql(con=db, name='daily_price', if_exists='append')
            
            db.commit()
            # db.dispose()
        else:
            pass
#            print('Already have {}'.format(ticker))
    if (db.is_connected()):
#        cursor.close()
        db.close()
    return tickers



"""
    The below code plucks the all US stock prices from Yahoo Finance and 
    saves them with the name of the stock ticker and to the MySQL database table daily_price. 
    The prices such as Open, High, Low & Close are stored in the file
    for each of the trading dates. Window length for moving average
"""
def get_all_us_data_from_yahoo(reload_all_us=False):
    db = get_con_alchemy()
    yf.pdr_override()
    headers = {
    'Content-Type': 'application/json'
    }
    window_length = 14
    if reload_all_us:
        tickers = save_all_US_tickers()
    else:
        with open("all_us_tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')
    start_d = dt.datetime(1998, 1, 1)
    end_d = dt.datetime.now()
    for ticker in tickers:
        print('******************* The ticker to start is =========== : ', ticker)
        df = pdr.get_data_yahoo(ticker, start_d, end_d)
        # if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
        if len(df) >126:
            print('******************* The ticker we have come inside for is =========== : ', ticker)
            # df = pdr.get_data_yahoo(ticker, start_d, end_d)
            df = df.rename(columns = {'Adj Close': 'Adj_Close'})
            df['Open'] = df['Open'].round(4)
            df['High'] = df['High'].round(4)
            df['Low'] = df['Low'].round(4)
            df['Close'] = df['Close'].round(4)
            df['Adj_Close'] = df['Adj_Close'].round(4)
            delta = df['Adj_Close'].diff()
            delta = delta[1:]
            # Make the positive gains (up) and negative gains (down) Series
            up, down = delta.copy(), delta.copy()
            up[up < 0] = 0
            down[down > 0] = 0
            # Calculate the EWMA
            roll_up1 = up.ewm(span=window_length).mean().round(4)
            roll_down1 = down.abs().ewm(span=window_length).mean().round(4)
            # Calculate the RSI based on EWMA
            RS1 = (roll_up1 / roll_down1).round(4)
            RSI1 = 100.0 - (100.0 / (1.0 + RS1)).round(4)  
            # Derive daily return percentage
            df['Daily_Return'] = df['Adj_Close'].pct_change().round(4)
            df['Monthly_Return'] = df['Adj_Close'].resample('M').ffill().pct_change().round(4)
            # Derive the Exponential Moving Averages
            df['EMA21'] = df['Adj_Close'].ewm(span=21, adjust=False).mean().astype('float').round(4)
            df['EMA50'] = df['Adj_Close'].ewm(span=50, adjust=False).mean().astype('float').round(4)
            df['EMA200'] = df['Adj_Close'].ewm(span=200, adjust=False).mean().astype('float').round(4)
            df['RS'] = RS1.astype('float')
            df['RS'] = round(df['RS'].replace([np.inf, -np.inf], 0), 5)
            # print('***************************** The highest RS is : ', ticker, df['RS'].max())
            df['RSI'] = RSI1.astype('float').round(4)
            df['ticker'] = ticker
            df['data_vendor_id'] = 7300
            pfe_length = 10
            pfe_period = pfe_length - 1
            pfe_tmp_values = []
            for k in range(0, 9):
                pfe_tmp_values.append(0)
            for i in range(9, len(df)):
                diff = df['Adj_Close'].iloc[i] - df['Adj_Close'].iloc[i-pfe_period]
                pfetmp1 = 100 * math.sqrt(pow(diff,2) + pow(pfe_period,2))
                pfetmp2 = 0
                for j in range(0, pfe_period-1):
                    pfetmp2 += math.sqrt(1 + pow(df['Adj_Close'].iloc[i-j] - df['Adj_Close'].iloc[i-j-1],2))
                pfe_tmp = pfetmp1 / pfetmp2
                if diff < 0:
                    pfe_tmp = -pfe_tmp
                pfe_tmp_values.append(pfe_tmp)          
            df['PFE'] = pfe_tmp_values
            df['PFE'] = df['PFE'].ewm(span=5, adjust=False).mean().round(4)
            change = df['Adj_Close'].pct_change()
            slope = change/df['Adj_Close']
            ema_slope_f = slope.ewm(span=9, adjust=False).mean()
            ema_slope_m = slope.ewm(span=21, adjust=False).mean()
            ema_slope_s = slope.ewm(span=200, adjust=False).mean()
            df['Slope_Fast'] = ema_slope_f.round(4)
            df['Slope_Medium'] = ema_slope_m.round(4)
            df['Slope_Slow'] = ema_slope_s.round(4)
            df.replace([np.inf, -np.inf], 0.0, inplace=True)
            df.to_sql(con=db, name='daily_price', if_exists='append')
            

def getsp100(start_date, end_date):
    db = get_con_alchemy()
    conn = db.connect()
    ticker = '^OEX'
    start_d = dt.datetime(1999, 1, 1)
    end_d = dt.datetime.now()
    yf.pdr_override()

    df = pd.DataFrame(pdr.get_data_yahoo(ticker, start_d, end_d))

    all_dates = (df.index).to_frame()
    df['ticker_id'] = ticker
    df['price_date'] = (df.index).to_frame()
    df.rename(columns = {'Adj Close': 'adj_close_price', 'Open': 'open_price', 'High': 'high_price', 'Low': 'low_price', 'Close': 'close_price'}, inplace=True)

    # dfp.drop('a', axis=1, inplace=True)
    df.to_sql(con=conn, name='sp100', if_exists='append', index=False)


def get_commodities_data():
    db = get_con_alchemy()
    conn = db.connect()
    window_length = 14
    df = pd.read_csv("stocks_and_commodities/commodity_2000_2022.csv", index_col=False)
    # df.reset_index(drop=True)
    df = df.rename(columns = {'Symbol': 'symbol'})
    df = df.rename(columns = {'Date': 'price_date'})
    df = df.rename(columns = {'Close': 'close_price'})
    df = df.rename(columns = {'Open': 'open_price'})
    df = df.rename(columns = {'High': 'high_price'})
    df = df.rename(columns = {'Low': 'low_price'})
    df = df.rename(columns = {'Volume': 'volume'})
    df.to_sql(con=conn, name='commodity_prices', if_exists='append', index=False)

"""
    delta = df['Close'].diff()
    delta = delta[1:]
    
    # Make the positive gains (up) and negative gains (down) Series
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    
    # Calculate the EWMA
    roll_up1 = up.ewm(span=window_length).mean()
    roll_down1 = down.abs().ewm(span=window_length).mean()
    
    # Calculate the RSI based on EWMA
    RS1 = roll_up1 / roll_down1
    RSI1 = 100.0 - (100.0 / (1.0 + RS1))   
      
    # Derive daily return percentage  
    df['Daily_Return'] = df['Adj_Close'].pct_change()
    df['Monthly_Return'] = df['Adj_Close'].resample('M').ffill().pct_change()
    
    # Derive the Exponential Moving Averages
    df['EMA21'] = df['Close'].ewm(span=21, adjust=False).mean().astype('float')
    df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean().astype('float')
    df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean().astype('float')
    df['RS'] = RS1.astype('float')
    df['RS'] = round(df['RS'].replace([np.inf, -np.inf], 0), 5)
    df['RSI'] = RSI1.astype('float')
    df['ticker'] = ticker

    pfe_length = 10
    pfe_period = pfe_length - 1
    pfe_tmp_values = []
    
    for k in range(0, 9):
        pfe_tmp_values.append(0)
    for i in range(9, len(df)):
        diff = df['Adj_Close'].iloc[i] - df['Adj_Close'].iloc[i-pfe_period]
        pfetmp1 = 100 * math.sqrt(pow(diff,2) + pow(pfe_period,2))
        pfetmp2 = 0
        for j in range(0, pfe_period-1):
            pfetmp2 += math.sqrt(1 + pow(df['Adj_Close'].iloc[i-j] - df['Adj_Close'].iloc[i-j-1],2))
        pfe_tmp = pfetmp1 / pfetmp2
        if diff < 0:
            pfe_tmp = -pfe_tmp
        pfe_tmp_values.append(pfe_tmp)          
    df['PFE'] = pfe_tmp_values
    df['PFE'] = df['PFE'].ewm(span=5, adjust=False).mean()
"""
    


"""
    The below code reads the price data from the DB for all stocks in the S&P 500 list
    and calculates the montly returns using the last day of each month's close 
    price. It also calculates the quarterly, semi-annual and annual returns. 
    The monthly returns are then used to calculate the net annual return
    which is then sorted in a descending order and saved to the period_returns table.
"""
def prepare_returns(f_date, reload_sp500=False):
    if reload_sp500:
        tickers = save_all_US_tickers()
    else:
        with open("all_us_tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
            
    db = get_con_alchemy()
    conn = db.connect()
    # rem_per_ret_sql = "Delete FROM period_returns"
    # conn.execute(rem_per_ret_sql)
    short_past_date = f_date - dt.timedelta(days=(30))
    short_past_date = qualify_date(short_past_date)
    
    ninety_days_past_date = f_date - dt.timedelta(days=(90))
    ninety_days_past_date = qualify_date(ninety_days_past_date)
    
    semi_annual_past_date = f_date - dt.timedelta(days=(180))
    semi_annual_past_date = qualify_date(semi_annual_past_date)
    
    annual_past_date = f_date - dt.timedelta(days=(365))
    annual_past_date = qualify_date(annual_past_date)
    
    i = 0
    while i <len(tickers):    #    chnaged this to the number of tickers
        price_sql = "SELECT Adj_Close FROM daily_price WHERE ticker = %s AND Date = %s"
        rows1 = conn.execute(price_sql, (tickers[i], short_past_date,))
        price_short_past = rows1.fetchall()
        if len(price_short_past) == 0:
            i += 1
            continue
        rows2 = conn.execute(price_sql, (tickers[i], f_date,))
        price_start = rows2.fetchall()
        rows3 = conn.execute(price_sql, (tickers[i], ninety_days_past_date,))
        price_long_past = rows3.fetchall()  
        if len(price_long_past) == 0:
            i += 1
            continue
        rows4 = conn.execute(price_sql, (tickers[i], semi_annual_past_date,))
        semi_annual_past_price = rows4.fetchall() 
        if len(semi_annual_past_price) == 0:
            i += 1
            continue
        rows5 = conn.execute(price_sql, (tickers[i], annual_past_date,))
        annual_past_price = rows5.fetchall() 
        if len(annual_past_price) == 0:
            i += 1
            continue
        # try:
        short_return = round(((float(price_start[0][0])/float(price_short_past[0][0])) -1), 6)
        long_return = round(((float(price_start[0][0])/float(price_long_past[0][0])) -1), 6)
        semi_annual_return = round(((float(price_start[0][0])/float(semi_annual_past_price[0][0])) -1), 6)
        annual_return = round(((float(price_start[0][0])/float(annual_past_price[0][0])) -1), 6)
        perf_sql = "insert into period_returns (ticker, return_date, price, monthly_return, qtr_return, semi_annual_return, annual_return) values(%s, %s, %s, %s,  %s, %s,  %s)"
        
        conn.execute(perf_sql, (tickers[i], f_date, price_start[0][0], short_return, long_return, semi_annual_return, annual_return),)
        # print('************************************** Just finished: ', i, tickers[i])
            
        # except:
        #     # Add a step to handle the exception
        #     pass
        
        i += 1
    if not(conn.closed):
        conn.close()
        db.dispose()


"""
    The below code reads the price data from the DB for the requested stock and the date
    and returns it.
"""
def retrieve_px_values(stock, for_date):
    db = get_con_alchemy()
    conn = db.connect()
    
    price_sql = "SELECT Close, Daily_Return, EMA21, EMA200, RSI, PFE, Slope_Fast, Slope_Medium FROM daily_price WHERE ticker = %s AND Date = %s"
    c_fetch = conn.execute(price_sql, (stock, for_date,))
    t_series = c_fetch.fetchall()    
    if not(conn.closed):
        conn.close()
        db.dispose()  
    return t_series


"""
    The below code reads the quarterly returns data from the DB (table period_returns) for all stocks in the S&P 500 list
    and fetches the top 10 as well as bottom 10 performers to form the 1st decile.
    The results are then stored in the table top_bottom_deciles.
    ======================== The period to sort is mentioned in the 2 queries below =======================================
    =========================and also in the function go_long_short while fetching the data twice =========================
"""
def create_return_deciles(f_date, return_type, reload_sp500=False):     
    db = get_con_alchemy()
    conn = db.connect()
    today = dt.datetime.now()
    min_price = 5.0
    # return_type = "monthly_return"

    # Top Decile - only choose the stocks where the latest adjusted close price is over the minimum price defined.
    """
    ==============================
    We decide in the top decile and the bottom decile formation, whether we need monthly or quarterly trend period.
    ==============================
    """
    price_sql = "select * from period_returns where return_date = %s AND ticker IS NOT NULL AND price > %s ORDER BY %s DESC LIMIT 10;"
    rows1 = conn.execute(price_sql, (f_date), min_price, return_type)
    top_decile = rows1.fetchall()

    all_returns = []
    all_returns_1 = np.array([0, 0, 0, 0, 0, 0, 0, 0])
    all_returns_set = all_returns_1.reshape((1, 8))
    
    for j in range(len(top_decile)):
        date = top_decile[j][2]
        decile = 'top decile'
        ticker = top_decile[j][1]
        price = top_decile[j][5]
        monthly_return = top_decile[j][6]
        quarterly_return = top_decile[j][7]
        semi_annual_return = top_decile[j][8]
        annual_return = top_decile[j][9]

        all_returns = np.array([date, ticker, decile, price, monthly_return, quarterly_return, semi_annual_return, annual_return])
        all_returns_1 = all_returns.reshape((1, 8))
        all_returns_set = np.append(all_returns_set, all_returns_1, axis=0)

    # Bottom Decile - only choose the stocks where the latest adjusted close price is over the minimum price defined.
    price_sql = "select * from period_returns where return_date = %s AND ticker IS NOT NULL AND price > %s ORDER BY %s ASC LIMIT 10;"
    rows1 = conn.execute(price_sql, (f_date), min_price, return_type)
    bottom_decile = rows1.fetchall()

    all_returns_bottom = []
    all_returns_1_bottom = np.array([0, 0, 0, 0, 0, 0, 0, 0])
    # all_returns_set = all_returns_1.reshape((1, 8))
    
    for j in range(len(bottom_decile)):
        date = bottom_decile[j][2]
        decile = 'bottom decile'
        ticker = bottom_decile[j][1]
        price = round(bottom_decile[j][5], 6)
        monthly_return = round(bottom_decile[j][6], 6)
        quarterly_return = round(bottom_decile[j][7], 6)
        semi_annual_return = round(bottom_decile[j][8], 6)
        annual_return = round(bottom_decile[j][9], 6)

        all_returns_bottom = np.array([date, ticker, decile, price, monthly_return, quarterly_return, semi_annual_return, annual_return])
        all_returns_1_bottom = all_returns_bottom.reshape((1, 8))
        all_returns_set = np.append(all_returns_set, all_returns_1_bottom, axis=0)

        # Remove the rows with values as 0 in the below line
        all_returns_set = all_returns_set[~np.all(all_returns_set == 0, axis=1)]

    df = pd.DataFrame(all_returns_set, columns=['return_date', 'ticker', 'decile', 'price', 'monthly_return', 'quarterly_return', 'semi_annual_return', 'annual_return'])

    df.to_sql(con=db, name='top_bottom_deciles', if_exists='append', index=False)
            
    # conn.commit()


def build_order_book():
    # latest_price = retrieve_px_values(ticker, today)
    # go_long_short(ticker, latest_price, today, funds_available, momentum_positive, EMA20_signal)
    pass


def go_long_short(return_date, previous_date, qtr_count, return_type):
    db = get_con_alchemy()
    conn = db.connect()
    today = dt.datetime.now()
    # return_type = "monthly_return"
    global initial_investment
    global benchmark_units
    if qtr_count == 0:
        # Insert the following values into portfolio_performance to set the starting portfolio value.
        dfp = pd.DataFrame([1], columns = ['a'])
        dfp['position_date'] = return_date
        dfp['value_date'] = return_date

        dfp['portfolio_value'] = initial_investment
        dfp['portfolio_return'] = 0.00

        chosen_security_sql = "SELECT * FROM sp100 WHERE price_date = %s AND ticker_id IS NOT NULL"
        rows1 = conn.execute(chosen_security_sql, (return_date))
        chosen_security = pd.DataFrame(rows1.fetchall())
        benchmark_units = math.floor(initial_investment/chosen_security['close_price'].astype(float))
        dfp['benchmark_value'] = chosen_security['close_price'] * benchmark_units
        dfp['benchmark_units'] = benchmark_units
        dfp['benchmark_return'] = 0.00

        dfp.drop('a', axis=1, inplace=True)
        dfp.to_sql(con=conn, name='portfolio_performance', if_exists='append', index=False)


    chosen_security_sql = "SELECT * FROM top_bottom_deciles WHERE return_date = %s AND ticker IS NOT NULL"
    rows1 = conn.execute(chosen_security_sql, (return_date))
    chosen_security = pd.DataFrame(rows1.fetchall())
    chosen_security['ticker'] = chosen_security['ticker'].astype('string')
    chosen_security['decile'] = chosen_security['decile'].astype('string')
    chosen_security['price'] = chosen_security['price'].astype(float)
    chosen_security['monthly_return'] = chosen_security['monthly_return'].astype(float)
    chosen_security['quarterly_return'] = chosen_security['quarterly_return'].astype(float)
    chosen_security['semi_annual_return'] = chosen_security['semi_annual_return'].astype(float)
    chosen_security['annual_return'] = chosen_security['annual_return'].astype(float)

    # Choose the longs and invest
    included_longs = chosen_security.nlargest(5, return_type)
    """""
    ==================== LOGIC TO BE ADDED  ====================
    Add the logic to get the invested portfolio value and the available cash from portfolio_value table. 
    OR get the total_portfolio_value from the same table.
    This value, when available, should be used insatead of the amount below for funds_available.
    The below hardcoded value should only be used the 1st time.
    """""
    
    if qtr_count == 0:
        funds_available = initial_investment
    else:
        cash_sql = "SELECT * FROM trade_portfolio WHERE ticker IS NOT NULL AND txn_date = %s"
        rows1 = conn.execute(cash_sql, previous_date)
        cash_amounts = pd.DataFrame(rows1.fetchall())
        cash_amounts['remaining_cash'] = cash_amounts['remaining_cash'].astype(float)
        funds_available = cash_amounts['remaining_cash'].iloc[-1]
        # print("************ Original Funds available are :", funds_available)
        # Perform the buy and sell of previous constituents and update the cash register
        for p_constituents in range(len(cash_amounts)):
            ticker = cash_amounts.iloc[p_constituents]['ticker']
            long_short = cash_amounts.iloc[p_constituents]['long_short']
            quantity = float(cash_amounts.iloc[p_constituents]['quantity'])
            sell_security_sql = "SELECT Adj_Close FROM daily_price WHERE Date = %s AND ticker = %s"
            price_row = conn.execute(sell_security_sql, (return_date, ticker))
            price = pd.DataFrame(price_row.fetchall())
            unit_price = float(price.Adj_Close.iloc[0])
            buy_sell_price = unit_price * quantity
            if long_short == 'long':
                funds_available += buy_sell_price
                # print("After sale, Funds available are :", ticker, funds_available)
            elif long_short == 'short':
                funds_available -= buy_sell_price
                # print("After covering short, Funds available are :", ticker, funds_available)
            # print("Selling the stock, bought on, and selling on :", ticker, previous_date, return_date)
            # print("After sale, Funds available are :", ticker, funds_available)

    # Split the funds across 10 buckets, although 5 securities are being bought or sold at a time
    bucket_size = initial_investment/10
    trade_date = today
    # Taking a conservative approach, defining a price uplift of 1%
    price_bump = 0.00
    # Taking a conservative approach, adding a commission of half percent to the costs
    commission = 0.00
    next_day_date = qualify_date(return_date + dt.timedelta(days=(1)))
    # price_fetch_date = qualify_date(price_fetch_date)
    # dfp = pd.DataFrame([1], columns = ['a'])
    for j in range(len(included_longs)):
        ticker = included_longs.iloc[j]['ticker']
        # Taking a conservative approach, use the highest price from the previous day
        trade_security_sql = "SELECT Adj_Close FROM daily_price WHERE Date = %s AND ticker = %s"
        price_row = conn.execute(trade_security_sql, (next_day_date, ticker))
        price = pd.DataFrame(price_row.fetchall())
        s_price = float(price.Adj_Close.iloc[0])
        # Taking a conservative approach, applying a price uplift of 1% to the highest price of previous day
        trade_price = (s_price) * (1+price_bump)
        # Split the funds across 10 buckets, although 5 securities are being bought for now
        # funds_to_trade = funds_available/10
        long_qty = math.floor((bucket_size * (1-commission))/ trade_price)
        # print('****************** Funds to be invested in the ticker :', ticker, bucket_size, long_qty)
        funds_used = (long_qty * trade_price) * (1 + commission)
        funds_available = funds_available - funds_used
        dfp = pd.DataFrame([1], columns = ['a'])
        dfp['txn_date'] = return_date
        dfp['ticker'] = ticker
        dfp['long_short'] = 'long'
        dfp['quantity'] = long_qty
        dfp['trade_price'] = trade_price
        # Value is calculated and stored. The return amount on the security will be calculated against this.
        dfp['value'] = funds_used
        dfp['remaining_cash'] = funds_available
        dfp.drop('a', axis=1, inplace=True)
        dfp.to_sql(con=conn, name='trade_portfolio', if_exists='append', index=False)
        # db.commit()
        # conn.dispose()       

# =============================== COMMENTING OUT THE SHORTS TEMPORARILY ==================================
# ============= FIX REMAINING_CASH, IT IS REACHING CLOSE TO NEGATIVE 10 MILLION IF SHORTS ARE REMOVED
    # Choose the shorts and invest
    included_shorts = chosen_security.nsmallest(5, return_type)
    # funds_available = 1000000.00
    # funds_remaining = funds_available
    trade_date = today
    # Taking a conservative approach, defining a price uplift of 1%
    price_decline = 0.00
    # Taking a conservative approach, adding a commission of two percent to the costs for shorts, 4 times higher than longs
    short_commission = 0.00
    for j in range(len(included_shorts)):
        ticker = included_shorts.iloc[j]['ticker']
        # Taking a conservative approach, use the lowest price from the previous day
        trade_security_sql = "SELECT Adj_Close FROM daily_price WHERE Date = %s AND ticker = %s"
        price_row = conn.execute(trade_security_sql, (next_day_date, ticker))
        price = pd.DataFrame(price_row.fetchall())
        s_price = float(price.Adj_Close.iloc[0])
        # Taking a conservative approach, applying a price uplift of 1% to the highest price of previous day
        sh_trade_price = (s_price) * (1-price_decline)
        # Although, short position is not totally dependent on funds but using this approach to determine the quantity
        short_qty = math.floor((bucket_size * (1-short_commission))/ sh_trade_price)
        # short_qty = math.floor(s_amount)
        # print('****************** Funds for the short position in the ticker :', ticker, bucket_size, short_qty)
        funds_recieved = (short_qty * sh_trade_price) * (1 - short_commission)
        # print('££££££££££££££ Funds available while in shorts are :', funds_available)
        funds_available = funds_available + funds_recieved

        dfp = pd.DataFrame([1], columns = ['a'])
        dfp['txn_date'] = return_date
        dfp['ticker'] = ticker
        dfp['long_short'] = 'short'
        dfp['quantity'] = short_qty
        dfp['trade_price'] = round(sh_trade_price, 6)
        # Value is calculated and stored. The return amount on the security will be calculated against this.
        dfp['value'] = round(funds_recieved, 6)
        dfp['remaining_cash'] = round(funds_available, 6)
        dfp.drop('a', axis=1, inplace=True)
        dfp.to_sql(con=conn, name='trade_portfolio', if_exists='append', index=False)
        # conn.dispose()


"""
    The below code reads the quarterly returns data for all long as well as short positions in the portfolio
    and builds the portfolio performance accordingly.
"""
def portfolio_valuation(position_date, performance_date, qtr_count):
    db = get_con_alchemy()
    conn = db.connect()
    # today = dt.datetime.now()

    # Taking a conservative approach, defining a price uplift of 1%
    price_decline = 0.00
    # Taking a conservative approach, adding a commission of two percent to the costs for shorts, 4 times higher than longs
    sell_commission = 0.00

    chosen_security_sql = "SELECT * FROM trade_portfolio WHERE txn_date = %s AND ticker IS NOT NULL"
    rows1 = conn.execute(chosen_security_sql, (position_date))
    chosen_security = pd.DataFrame(rows1.fetchall())
    chosen_security['ticker'] = chosen_security['ticker'].astype('string')
    chosen_security['long_short'] = chosen_security['long_short'].astype('string')
    chosen_security['quantity'] = chosen_security['quantity'].astype(float)
    chosen_security['trade_price'] = chosen_security['trade_price'].astype(float)
    chosen_security['value'] = chosen_security['value'].astype(float)
    chosen_security['txn_date'] = chosen_security['txn_date']
    chosen_security['remaining_cash'] = chosen_security['remaining_cash'].astype(float)

    original_portfolio_value = 0
    # current_portfolio_value = 0
    portfolio_return = 0
    remaining_cash = 0

    global portfolio_value
    current_portfolio_value = portfolio_value

    if qtr_count == 0:
        dfp = pd.DataFrame([1], columns = ['a'])
        dfp['ticker'] = 'start_val'
        dfp['position_date'] = position_date
        dfp['price_date'] = position_date
        dfp['entry_price'] = '0.00'
        dfp['exit_price'] = '0.00'
        dfp['t_direction'] = ''
        dfp['open_close'] = ''
        dfp['position_return'] = '0.00'
        dfp['portfolio_return'] = '0.00'
        dfp['benchmark_value'] = '0.00'
        dfp['orig_port_val'] = '0.00'
        dfp['inv_portfolio_value'] = '0.00'
        dfp['cash'] = 1000000.00
        dfp['total_portfolio_value'] = 1000000.00

        chosen_benchmark_sql = "SELECT * FROM sp100 WHERE price_date = %s AND ticker_id IS NOT NULL"
        rows2 = conn.execute(chosen_benchmark_sql, (position_date))
        benchmark_vals = pd.DataFrame(rows2.fetchall())  
        benchmark_price = benchmark_vals['close_price'].iloc[-1]
        dfp['benchmark_value'] = benchmark_price
        dfp.drop('a', axis=1, inplace=True)
        dfp.to_sql(con=conn, name='portfolio_value', if_exists='append', index=False)
    
    for j in range(len(chosen_security)):
        if (chosen_security.iloc[j]['long_short'] == 'long'):
            ticker = chosen_security.iloc[j]['ticker']
            qty = chosen_security.iloc[j]['quantity']
            original_value = chosen_security.iloc[j]['value']
            remaining_cash = chosen_security.iloc[j]['remaining_cash']
            trade_security_sql = "SELECT Adj_Close FROM daily_price WHERE Date = %s AND ticker = %s"
            price_row = conn.execute(trade_security_sql, (performance_date, ticker))
            price = pd.DataFrame(price_row.fetchall())
            # print('************* Here the values are: ', performance_date, ticker)
            exit_price = float(price.Adj_Close.iloc[0]) * (1 - price_decline) * (1-sell_commission)
            sell_value = (qty * (exit_price ))
            return_amount = sell_value - original_value
            trade_price = chosen_security.iloc[j]['trade_price']
            original_portfolio_value += original_value
            current_portfolio_value += sell_value
            portfolio_return += return_amount

            chosen_benchmark_sql = "SELECT * FROM sp100 WHERE price_date = %s AND ticker_id IS NOT NULL"
            rows2 = conn.execute(chosen_benchmark_sql, (performance_date))
            benchmark_vals = pd.DataFrame(rows2.fetchall())  
            benchmark_price = benchmark_vals['close_price'].iloc[-1]
            """
            ============   The definition of inv_portfolio_value and current_portfolio_value:   ==================
            At the beginning of each period, the existing positions are liquidated and new positions are taken.
            So, the inv_portfolio_value (same as current_portfolio_value) is nothing but the net value of the 
            invested portfolio positions at the time of entering those positions (All positions are assumed to be entered at the same time).
            So this value will always be 0 at the beginning of each period as all positions are dissolved and we re-start the cycle.
            As a long position is entered, the current_portfolio_value rises by the value of that position and as a short 
            position is entered, current_portfolio_value drops by the value of the position.
            At the end, the sum of 5 longs (+ve) and 5 shorts (-ve) gives the net value of the invested portfolio.
            """

        if (chosen_security.iloc[j]['long_short'] == 'short'):
            ticker = chosen_security.iloc[j]['ticker']
            qty = chosen_security.iloc[j]['quantity']
            original_value = chosen_security.iloc[j]['value']
            remaining_cash = chosen_security.iloc[j]['remaining_cash']
            trade_security_sql = "SELECT Adj_Close FROM daily_price WHERE Date = %s AND ticker = %s"
            price_row = conn.execute(trade_security_sql, (performance_date, ticker))
            price = pd.DataFrame(price_row.fetchall())
            exit_price = float(price.Adj_Close.iloc[0]) * (1 - price_decline) * (1-sell_commission)
            buy_value = (qty * (exit_price))
            return_amount = original_value - buy_value
            trade_price = chosen_security.iloc[j]['trade_price']
            # ************************************
            # Ignoring the signage temporarily for short positions. It will need to be updated at some stage to store -ve values.
            # ************************************
            original_portfolio_value += original_value
            current_portfolio_value -= buy_value
            portfolio_return += return_amount
            # print('************** Buy back value and the Current portfolio value are :', buy_value, current_portfolio_value)

            chosen_benchmark_sql = "SELECT * FROM sp100 WHERE price_date = %s AND ticker_id IS NOT NULL"
            rows2 = conn.execute(chosen_benchmark_sql, (performance_date))
            benchmark_vals = pd.DataFrame(rows2.fetchall())  
            benchmark_price = benchmark_vals['close_price'].iloc[-1]

        # Insert the following values into portfolio_value for each period.
        dfp = pd.DataFrame([1], columns = ['a'])
        dfp['ticker'] = ticker
        dfp['position_date'] = position_date
        dfp['price_date'] = performance_date
        dfp['entry_price'] = round(trade_price, 6)
        dfp['exit_price'] = round(exit_price, 6)
        dfp['t_direction'] = chosen_security.iloc[j]['long_short']
        dfp['open_close'] = 'close'
        dfp['position_return'] = round(return_amount, 6)
        dfp['portfolio_return'] = round(portfolio_return, 6)
        dfp['benchmark_value'] = '0.00'
        dfp['orig_port_val'] = round(original_portfolio_value, 6)
        dfp['inv_portfolio_value'] = round(current_portfolio_value, 6)
        dfp['cash'] = round(remaining_cash, 6)
        dfp['total_portfolio_value'] = round((current_portfolio_value + remaining_cash), 6)
        dfp['benchmark_value'] = round(benchmark_price, 6)
        dfp.drop('a', axis=1, inplace=True)
        dfp.to_sql(con=conn, name='portfolio_value', if_exists='append', index=False)
        # conn.dispose()
        

"""
    The below code reads the quarterly returns data for all long as well as short positions in the portfolio
    and builds the portfolio performance accordingly.
"""
def portfolio_performance(position_date, performance_date, qtr_count):
    db = get_con_alchemy()
    conn = db.connect()

    # Taking a conservative approach, defining a price uplift of 1%
    price_decline = 0.00
    # Taking a conservative approach, adding a commission of two percent to the costs for shorts, 4 times higher than longs
    sell_commission = 0.00

    chosen_security_sql = "SELECT * FROM portfolio_value WHERE price_date = %s AND ticker IS NOT NULL"
    rows1 = conn.execute(chosen_security_sql, (performance_date))
    chosen_security = pd.DataFrame(rows1.fetchall())
    chosen_security['position_date'] = chosen_security['position_date']
    chosen_security['price_date'] = chosen_security['price_date']
    chosen_security['total_portfolio_value'] = chosen_security['total_portfolio_value'].astype(float)
    chosen_security['portfolio_return'] = chosen_security['portfolio_return'].astype(float)

    pos_date = chosen_security['position_date'].iloc[-1]
    value_date = chosen_security['price_date'].iloc[-1]
    portfolio_value = chosen_security['total_portfolio_value'].iloc[-1]
    portfolio_return = chosen_security['portfolio_return'].iloc[-1]
    # benchmark_units = chosen_security['benchmark_units'].iloc[0]
    # print('*********** Before creating new df, dfp')
    # Insert the following values into portfolio_value for each period.
    dfp = pd.DataFrame([1], columns = ['a'])
    dfp['position_date'] = pos_date
    dfp['value_date'] = value_date

    dfp['portfolio_value'] = portfolio_value
    dfp['portfolio_return'] = portfolio_return
    dfp['benchmark_units'] = benchmark_units
    dfp['benchmark_value'] = chosen_security['benchmark_value'].iloc[-1] * benchmark_units
    # dfp['benchmark_value'] = chosen_security['close_price'] * benchmark_units
    dfp['benchmark_return'] = 0.00

    dfp.drop('a', axis=1, inplace=True)
    dfp.to_sql(con=conn, name='portfolio_performance', if_exists='append', index=False)
    print('*********** This is the end..........................')
    # qs.reports.html(dfp['portfolio_return'], "SPY", output='/portfolio_tearsheet.html')
    

"""
    The below code reads the portfolio performance data for the entire duration in the portfolio
    and builds the percentage returns.
    It deletes all records and then saves everything again along with the percentage returns.
"""
def portfolio_returns(position_date, performance_date, qtr_count):
    db = get_con_alchemy()
    conn = db.connect()

    chosen_security_sql = "SELECT * FROM portfolio_performance"
    rows1 = conn.execute(chosen_security_sql,)
    chosen_security = pd.DataFrame(rows1.fetchall())
    # dfp = pd.DataFrame([1], columns = ['a'])
    chosen_security['position_date'] = chosen_security['position_date']
    chosen_security['value_date'] = chosen_security['value_date']
    chosen_security['benchmark_units'] = chosen_security['benchmark_units']
    chosen_security['portfolio_value'] = chosen_security['portfolio_value'].astype(float)
    chosen_security['portfolio_return'] = chosen_security['portfolio_return'].astype(float)
    chosen_security['returns_pct'] = chosen_security['portfolio_value'].astype(float).pct_change()
    chosen_security['benchmark_value'] = chosen_security['benchmark_value'].astype(float)
    chosen_security['benchmark_return'] = chosen_security['benchmark_value'].astype(float).pct_change()
    # print('=============================== ', chosen_security['returns_pct'], chosen_security['benchmark_return'])
    # Insert the following values into portfolio_value for each period.

    # dfp.drop('a', axis=1, inplace=True)

    conn.execute("TRUNCATE TABLE portfolio_performance")
    chosen_security.to_sql(con=conn, name='portfolio_performance', if_exists='append', index=False)
    print('*********** This is the end..........................')
    # qs.reports.html(dfp['portfolio_return'], "SPY", output='/portfolio_tearsheet.html')
        # conn.dispose()