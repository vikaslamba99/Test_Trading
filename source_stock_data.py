#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 12:01:52 2020

@author: vikaslamba
"""

import bs4 as bs
import datetime as dt
import pandas as pd
import mysql.connector as mysql
import sqlalchemy
import os
#from mysql.connector import Error
import math
from pandas_datareader import data as pdr
# from yahoofinancials import YahooFinancials
# from alpha_vantage.timeseries import TimeSeries
# from alpha_vantage.techindicators import TechIndicators
# from alpha_vantage.fundamentaldata import FundamentalData
import pickle
import requests
import yfinance as yf
import numpy as np
import io
from urllib.request import urlopen
import json
#from scipy.stats import linregress
#from matplotlib.pyplot import figure
#from date_qualifier import qualify_date
# import csv
# import quandl
#from pandas import DataFrame
#from alphavantage.price_history import (
#  AdjustedPriceHistory, get_results, PriceHistory, IntradayPriceHistory,
#  filter_dividends
#)
# quandl.ApiConfig.api_key = 'ZnkP4vphXzU3d-Qq9fD7'
# alphavantage_api_key = 'FBSOGLCBZNKFTN46'


yf.pdr_override()

def get_most_active_stocks():
    symbols=[]
    names=[]
    prices=[]
    changes=[]
    percentChanges=[]
    marketCaps=[]
    totalVolumes=[]
    circulatingSupplys=[]
    
    for i in range(0,11):
        CryptoCurrenciesUrl = "https://finance.yahoo.com/most-active?offset="+str(i)+"&amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;amp;count=100"
        r= requests.get(CryptoCurrenciesUrl)
        data=r.text
        soup=bs.BeautifulSoup(data, 'lxml')
        
    for listing in soup.find_all('tr', attrs={'class':'simpTblRow'}):
        for symbol in listing.find_all('td', attrs={'aria-label':'Symbol'}):
            symbols.append(symbol.text)
        for name in listing.find_all('td', attrs={'aria-label':'Name'}):
            names.append(name.text)
        for price in listing.find_all('td', attrs={'aria-label':'Price (intraday)'}):
            prices.append(price.find('span').text)
        for change in listing.find_all('td', attrs={'aria-label':'Change'}):
            changes.append(change.text)
        for percentChange in listing.find_all('td', attrs={'aria-label':'% change'}):
            percentChanges.append(percentChange.text)
        for marketCap in listing.find_all('td', attrs={'aria-label':'Market cap'}):
            marketCaps.append(marketCap.text)
        for totalVolume in listing.find_all('td', attrs={'aria-label':'Avg vol (3-month)'}):
            totalVolumes.append(totalVolume.text)
        for circulatingSupply in listing.find_all('td', attrs={'aria-label':'Volume'}):
            circulatingSupplys.append(circulatingSupply.text)

    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')
    start = dt.datetime(2007, 1, 1)
    end = dt.datetime.now()
    for ticker in symbols:
        # Make a list with all tickers  -- // TO DO  //
        # 
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
#            df = pdr.get_data_yahoo(ticker, start, end)
            df = pdr.DataReader(ticker, 'yahoo', start, end)
            df.reset_index(inplace=True)
            df.set_index("Date", inplace=True)
            short_rolling = df['Close'].ewm(span=21, adjust=False).mean()
            medium_rolling = df['Close'].ewm(span=50, adjust=False).mean()
            long_rolling = df['Close'].ewm(span=200, adjust=False).mean()
            df['EMA21'] = short_rolling
            df['EMA50'] = medium_rolling
            df['EMA200'] = long_rolling
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
        else:
            pass
    return symbols


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

def get_ticker_list(reload_sp500=False):
    with open("sp500tickers.pickle", "rb") as f:
        tickers = pickle.load(f)
#        print('......... First tickers are : ', tickers)
    return tickers
"""
    reload_sp500 is set to false in the below code because the SP500 stocks are already loaded.
"""
def get_data_from_yahoo(reload_sp500=False):
#        The below code plucks the S&P 500 stock prices from Yahoo Finance and 
#        saves them to  CSV file with the name of the stock ticker and to the MySQL database table daily_price. 
#        The prices such as Open, High, Low & Close are stored in the file
#        for each of the trading dates.
    # Window length for moving average
    db = mysql.connect(
        database = "Time_Series",
        host = "localhost",
        user = "root",
        passwd = "V!shnuPurana36")
    
    database_username = 'root'
    database_password = 'V!shnuPurana36'
    database_ip       = 'localhost'
    database_name     = 'Time_Series'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
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
            # df = pdr.DataReader(ticker, 'yahoo', start=start_d, end=end_d)
            # short_rolling = df['Close'].ewm(span=21, adjust=False).mean().astype('float')
            # medium_rolling = df['Close'].ewm(span=50, adjust=False).mean().astype('float')
            # long_rolling = df['Close'].ewm(span=200, adjust=False).mean().astype('float')
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
            
#            requestResponse = requests.get("https://api.tiingo.com/tiingo/fundamentals/aapl/daily?startDate>=2014-01-02&token=f5441a6ec32402886b650c44cc8aef6a49f140b8", headers=headers)
#            print(requestResponse.json())
#            funda_df = pd.DataFrame(requestResponse)
#            print(funda_df[0])
            df.to_sql(con=database_connection, name='daily_price', if_exists='append')
            
            db.commit()
            database_connection.dispose()
        else:
            pass
#            print('Already have {}'.format(ticker))
    if (db.is_connected()):
#        cursor.close()
        db.close()
    return tickers

'''            
def get_data_from_alphavantage(reload_sp500=False):
#        The below code plucks the S&P 500 stock prices from AlphaVantage and 
#        saves them to  CSV file with the name of the stock ticker. 
#        The prices such as Open, High, Low & Close are stored in the file
#        for each of the trading dates.
    # Chose your output format, or default to JSON (python dict)
    ts = TimeSeries(alphavantage_api_key, output_format='pandas')
    ti = TechIndicators(alphavantage_api_key, output_format='pandas')
    tf = FundamentalData(alphavantage_api_key, output_format='pandas')

    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
        
    if not os.path.exists('AV_Time_Series'):
        os.makedirs('AV_Time_Series')
    for ticker in tickers:
        if not os.path.exists('AV_Time_Series/{}.csv'.format(ticker)):
#            ticker_prices, meta_prices = ts.get_daily(symbol=ticker, outputsize='full')
            
            ticker_funda, meta_funda = tf.get_company_overview(symbol=ticker)
            
#            ticker_ema, meta_ema = ti.get_ema(symbol=ticker, time_period=21)
#            ticker_mom, meta_mom = ti.get_mom(symbol=ticker, interval='daily', time_period=21, series_type='close')
#            ticker_rsi, meta_rsi = ti.get_rsi(symbol=ticker, interval='daily', time_period=21, series_type='close')
#            frames = [ticker_prices, ticker_ema, ticker_mom, ticker_rsi]
#            frames = [ticker_prices, ticker_funda]
#            ticker_data = pd.concat(frames, axis=1, join='outer', sort=False) 
#            ticker_data.to_csv('AV_Time_Series/{}.csv'.format(ticker))
            
            ticker_funda.to_csv('AV_Time_Series/{}.csv'.format(ticker))
        else:
            pass
#            print('Already have {}'.format(ticker))
    return tickers
'''    

def get_data_from_fmp(reload_sp500=False):
#        The below code plucks the S&P 500 stock fundamentals from financialmodelingprep.com and 
#        saves them to  CSV file with the name of the stock ticker. 
    
    db = mysql.connect(
    database = "Time_Series",
    host = "localhost",
    user = "root",
    passwd = "V!shnuPurana36")
    
    database_username = 'root'
    database_password = 'V!shnuPurana36'
    database_ip       = 'localhost'
    database_name     = 'Time_Series'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
        
    if not os.path.exists('FMP_Fundamentals'):
        os.makedirs('FMP_Fundamentals')
    for ticker in tickers:
        ticker = ticker.replace('.', '-')
        if not os.path.exists('FMP_Fundamentals/{}.csv'.format(ticker)):
            url = ("https://financialmodelingprep.com/api/v3/key-metrics/"+ticker+"?period=quarter&limit=130&apikey=5184cf46dee614422e4dbcf1242bcb87")
            js = get_jsonparsed_data(url)
            funda_df = pd.DataFrame(js)
#            print('The ticker is: ', ticker)
            df = pd.DataFrame()
            try:
                funda_df = funda_df.fillna(0.0)
                df['calc_date'] = funda_df.date
                df['ticker'] = funda_df.symbol
                df['graham_number'] = round(funda_df.grahamNumber, 6)
                df['ps_ratio'] = round(funda_df.priceToSalesRatio, 6)
#                if ticker == 'AMCR':
#                    print('The value of grahamNumber is : ............... ', funda_df.grahamNumber)
#                    print('The value of pb_ratio is : ............... ', funda_df.pbRatio)
#                    print('The value of dividendYield is : ............... ', funda_df.dividendYield)
#                    print('The value of priceToSalesRatio is : ............... ', funda_df.priceToSalesRatio)
#                    print('The value of peRatio is : ............... ', funda_df.peRatio)
#                    print('The value of pocfratio is : ............... ', funda_df.pocfratio)
#                    print('The value of enterpriseValueOverEBITDA is : ............... ', funda_df.enterpriseValueOverEBITDA)
#                    print('The value of researchAndDdevelopementToRevenue is : ............... ', funda_df.researchAndDdevelopementToRevenue)
                df['pb_ratio'] = round(funda_df.pbRatio, 6)
                df['pe_ratio'] = round(funda_df.peRatio, 6)
                df['pcf_ratio'] = round(funda_df.pocfratio, 6)         
                df['ev_ebitda'] = round(funda_df.enterpriseValueOverEBITDA, 6)
                df['rd_revenue'] = round(funda_df.researchAndDdevelopementToRevenue, 6)  
                df['div_yield'] = round(funda_df.dividendYield, 6)
                
#                print("Inside try: ------ ", ticker) 
                df.to_sql(con=database_connection, name='fin_ratios', if_exists='append', index=False)
#                print("DB submission worked...... it seems.")
            except:
                print("Error with data for the stock: ------ ", ticker)

            db.commit()
            database_connection.dispose()
#            
#            with open('test.txt', 'w') as json_file:
#                json.dump(js, json_file)
#  
#            pd.set_option('display.max_columns', 50)
#            df = pd.read_json('test.txt')
#            df.to_csv('FMP_Fundamentals/{}.csv'.format(ticker))
        else:
            pass
    return tickers

def get_jsonparsed_data(url):
    """
    Receive the content of ``url``, parse it as JSON and return the object.

    Parameters
    ----------
    url : str

    Returns
    -------
    dict
    """
    response = urlopen(url)
    data = response.read().decode("utf-8")
    return json.loads(data)

def retrieve_values(stock, for_date):
    connection = mysql.connect(
                database = "Time_Series",
                host = "localhost",
                user = "root",
                passwd = "V!shnuPurana36")
    cursor = connection.cursor(named_tuple=True)
    record = cursor.fetchone()
    price_sql = "SELECT Close, Daily_Return, EMA21, EMA200, RSI, PFE, Slope_Fast, Slope_Medium FROM daily_price WHERE ticker = %s AND Date = %s"
    cursor.execute(price_sql, (stock, for_date,))
    t_series = cursor.fetchall()    
    if (connection.is_connected()):
        cursor.close()
        connection.close()    
    return t_series



#def get_data_from_quandl():
#    data = quandl.get_table('WIKI/PRICES', ticker = ['AAPL'], 
#                        qopts = { 'columns': ['ticker', 'date', 'adj_close'] }, 
#                        date = { 'gte': '2018-01-31', 'lte': '2020-03-30' }, 
#                        paginate=True)
#
#    data.head(10)
#    print('Here is what we have :', data['date'][1])
#    print('Here is what we have :', data['adj_close'][1])
#    
#get_data_from_quandl()

#get_data_from_alphavantage()
    
#get_data_from_yahoo()
            
#update_data_values()

#pfe_calc()
#    
#slope_calc()

#just_testing()