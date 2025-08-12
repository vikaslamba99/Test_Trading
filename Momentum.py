#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 18:17:58 2020

@author: vikaslamba
"""

import datetime as dt
from connection_pool import get_connection
import os
# import pyfolio as pf
# from scipy import stats
# import empyrical as ep
# import numpy as np
#import quandl
# from tabulate import tabulate
import pandas as pd
# import seaborn as sns
from pandas_datareader import data as pdr
from source_stock_data import get_data_from_yahoo
#from source_stock_data import get_ticker_list
#from choose_high_mom_stocks import prepare_returns
#from choose_high_mom_stocks import predict_the_returns
#from choose_high_mom_stocks import regression_returns
from choose_high_mom_stocks import regression_mean, piotroski_screen, calc_weighted_z_score, find_monthly_return, find_the_signal
#from choose_high_mom_stocks import move_fundamentals
from date_qualifier import qualify_date
#from portfolio_performance import lets_trade
#from portfolio_performance import snapshot
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates
# import matplotlib.animation as animation
#from source_stock_data import get_data_from_alphavantage
#from source_stock_data import get_data_from_fmp


start_date = dt.datetime.strptime('01-03-2018', '%d-%m-%Y')
"""
The below line of code needs revision. It should be changed to shaving off 1 day
from today and then in the date qualifier function, it should be sorted to step
1 day backward in case of a holiday and not 1 day forward.
"""
end_date = dt.datetime.strptime('22-06-2020', '%d-%m-%Y')
cycle = ''
#global allocation_period
allocation_period = 60
#if cycle == 'up':
#    allocation_period = 30
#elif cycle == 'down':
#    allocation_period = 120
"""    
 --------------------------------------------------------------------        
 ADD A NEW FUNCTION TO ROTATE THE PORTFOLIO BASED ON 1ST PERIOD PERFORMANCE.
 MAKE THE STOCK ALLOCATION FUNCTION COMMON SO THAT PARAMETERS CAN BE PASSED
 FROM OUTSIDE.
 ALSO, SPLIT THE CODE INTO 3 SEPARATE FILES -by different types of functions.
 --------------------------------------------------------------------   
""" 
def cockpit_engine_start():
    global start_date
    global end_date
    global allocation_period
    global f_date
    
    f_date = qualify_date(start_date + dt.timedelta(days=(1)*allocation_period))    #    first date of the period
    s_date = qualify_date(f_date + dt.timedelta(days=(1)*allocation_period))        #    second date taking into account allocation period
    t_date = qualify_date(s_date + dt.timedelta(days=(1)*allocation_period))        #    third date

    db = get_connection()
    cursor = db.cursor(named_tuple=True)
#    first_date = start_date
    i = 0  
    """
    The below 2 deletions need to be removed/commented as the price and fundamental details 
    has been saved up to May 2021 after the run on 22nd June, 2022 - although the 
    end date specified above for the strategy is 30th March, 2021.
    """
#    cursor.execute("Delete FROM daily_price")
#    db.commit()
#
#    cursor.execute("Delete FROM fin_ratios")
#    db.commit()
#    rem_per_ret_sql = "Delete FROM trade_portfolio" 
    cursor.execute("Delete FROM trade_portfolio")
    db.commit()
#    rem_action_history_sql = "Delete FROM action_funds"
    cursor.execute("Delete FROM action_funds")
    db.commit()
#    rem_p_funds_sql = "Delete FROM portfolio_value"
    cursor.execute("Delete FROM portfolio_value")
    db.commit()

    
    """
    uncomment the below line to re-load data from yahoo OR other sources?
    """
#    tickers = get_data_from_yahoo()
#    tickers = get_data_from_fmp()
    
#    pre_start_date = '2018-04-27'
#    pre_end_date = '2019-04-28'
#    
#    tickers = predict_the_returns(pre_start_date, pre_end_date)
#    print("----- The model stocks are : ------------------", model_stocks)
#    move_fundamentals()

    """
    The benchmark data is called to be loaded below.
    """
    getsp100()
    global cycle
    cycle = 'up'
#    while end_date >= s_date:

#        f_date = qualify_date(start_date + dt.timedelta(days=(i*allocation_period)))
#        s_date = qualify_date(start_date + dt.timedelta(days=(i+1)*allocation_period))
#        third_date = qualify_date(start_date + dt.timedelta(days=(i+2)*allocation_period))
#    n = 0
#    print(';;;;;;;;;;; Just before regression...........')
#    regression_returns(start_date, end_date)
#    regression_mean()
#    get_data_from_fmp()
#    piotroski_screen(end_date)
#    prepare_returns(end_date)
#    calc_weighted_z_score(end_date)

    """
    The below function generates and provides the monthly return of each stock by using the daily return values 
    for the given date range.
    """
#    find_monthly_return(start_date, end_date)
    # print('About to call findthesignal')
    tickers = get_data_from_yahoo()
#    find_the_signal(start_date, end_date)
'''
Commenting out the main functionality
    p_sec_date = f_date
    while s_date <= end_date and t_date <= end_date:
        print("The allocation period is : ", allocation_period, f_date, s_date)
#        tickers = predict_the_returns(f_date, s_date)
        tickers = get_ticker_list()
#        print('Here are the tickers:  ', tickers)
        prepare_returns(f_date, tickers)
        flag, cycle, allocation_period = lets_trade(s_date, cycle)
        snapshot(p_sec_date, s_date, flag)
#        
        p_sec_date = s_date
        f_date = qualify_date(f_date + dt.timedelta(days=(allocation_period)))
        s_date = qualify_date(s_date + dt.timedelta(days=(1)*allocation_period))
        t_date = qualify_date(t_date + dt.timedelta(days=(1)*allocation_period))
#
    i += 1
    plottheperformance()
    riskmetrics()
    if (db.is_connected()):
        cursor.close()
        db.close()

'''
'''
Plot the individual security performance in the function below
'''

def plotstockperformance():
    db = get_connection()
    cursor = db.cursor(named_tuple=True)
    port_val_sql = "SELECT Date, Adj_Close, Daily_Return  FFROM daily_price WHERE ticker = %s AND Date >= %s AND Date <= %s"
    cursor.execute(port_val_sql, ('AAPL', start_date, end_date,))
    port_vals = cursor.fetchall()
    portfolio_val_total = []
    price_date =[]
    benchmark_value = []
    i = 0
    while i < len(port_vals):
        price_date.append(port_vals[i][0])
        benchmark_value.append(port_vals[i][1])
        portfolio_val_total.append(port_vals[i][2])
        i += 1

    sns.set(font_scale=1.5, style="whitegrid")
    #set date as index    
    fig, ax = plt.subplots(figsize=(15,7))
    #set ticks every week
    ax.xaxis.set_major_locator(mdates.YearLocator())
    #set major ticks format
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    ax.set(title="Portfolio vs Benchmark")
    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Value", color="blue")
    ax.set_ylim(top=5000000)
    
    ax2=ax.twinx()
    ax2.set_ylabel("Benchmark Value", color="grey")
    ax2.set_ylim(top=5000000)    
#    
#    ax.clear()
#    ax2.clear()
    
    ax.plot(price_date, portfolio_val_total, color = 'blue')
    ax2.plot(price_date, benchmark_value, color='grey')
    plt.show()
    fig.savefig('portfolio_value.png')

'''
Plot the portfolio performance in the function below
'''
    
def plottheperformance():
    db = get_connection()
    cursor = db.cursor(named_tuple=True)
    port_val_sql = "SELECT price_date, benchmark_value, total_portfolio_value  FROM portfolio_value"
    cursor.execute(port_val_sql)
    port_vals = cursor.fetchall()
    portfolio_val_total = []
    price_date =[]
    benchmark_value = []
    i = 0
    while i < len(port_vals):
        price_date.append(port_vals[i][0])
        benchmark_value.append(port_vals[i][1])
        portfolio_val_total.append(port_vals[i][2])
        i += 1

    sns.set(font_scale=1.5, style="whitegrid")
    #set date as index    
    fig, ax = plt.subplots(figsize=(15,7))
    #set ticks every week
    ax.xaxis.set_major_locator(mdates.YearLocator())
    #set major ticks format
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    ax.set(title="Portfolio vs Benchmark")
    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Value", color="blue")
    ax.set_ylim(top=5000000)
    
    ax2=ax.twinx()
    ax2.set_ylabel("Benchmark Value", color="grey")
    ax2.set_ylim(top=5000000)    
#    
#    ax.clear()
#    ax2.clear()
    
    ax.plot(price_date, portfolio_val_total, color = 'blue')
    ax2.plot(price_date, benchmark_value, color='grey')
    plt.show()
    fig.savefig('portfolio_value.png')

def startanimating():
    ani = animation.FuncAnimation(fig, plottheperformance, interval=10000)
    plt.show()

def getsp100():
    db = get_connection()
    ticker = '^OEX'

    cursor = db.cursor(prepared=True)
    """
    The below csv file needs to be deleted to ensure that the SP100 data is loaded fresh.
    Also, the dependency on the csv needs to be removed.
    """
    if not os.path.exists('SP100/SP100.csv'):
        df = pdr.get_data_yahoo(ticker, start_date, end_date)
        df.reset_index(inplace=True)
        df.to_csv('SP100/SP100.csv')
        ticker_id = "SP100"
        sql = "insert into sp100 (ticker_id, price_date, open_price, high_price, low_price, close_price, adj_close_price, volume) values(%s, %s, %s, %s, %s, %s, %s, %s)"
        i = 0
        while i< len(df):
            cursor.execute(sql, (ticker_id, df[ "Date"][i], df["Open"][i], df["High"][i], df["Low"][i], df["Close"][i], df["Adj Close"][i], df["Volume"][i].astype('float')),)
            i += 1
        db.commit()
        if (db.is_connected()):
            cursor.close()
            db.close()
    else:
        pass


def progress_bar(iterable, size=100):

    count = len(iterable)  
    print('|', end = '')  
    print((size) * "-", end = '')  
    print('|')

    print(' ', end = '')

    def _update(progress, bar):  
        new_bar = int(size * progress / count)  
        for _ in range(bar, new_bar):  
            print("*", end = '')  
        return new_bar

    bar = 0  
    for i, item in enumerate(iterable):  
        yield item  
        bar = _update(progress=i+1, bar=bar)  
    print('')

def sentimentData():

    db = get_connection()
    cursor = db.cursor(prepared=True)
    df = quandl.get("AAII/AAII_SENTIMENT")
    df.reset_index(inplace=True)
    
    sql = "insert into sentiment (sentiment_date, bullish, neutral, bearish, total,\
    bullish_8_week_MA, bull_bear_spread, bullish_average, bullish_ave_plus_std,\
    bullish_ave_minus_std, sp500_weekly_high, sp500_weekly_low, sp500_weekly_close)\
    values(%s, %s, %s, %s, %s,  %s, %s, %s,  %s, %s, %s,  %s, %s)"
    i = 0
    while i< len(df):
        cursor.execute(sql, (df[ "Date"][i], df["Bullish"][i], df["Neutral"][i],\
                             df["Bearish"][i], df["Total"][i], df["Bullish 8-Week Mov Avg"][i],\
                             df["Bull-Bear Spread"][i], df["Bullish Average"][i],\
                             df["Bullish Average + St. Dev"][i], df["Bullish Average - St. Dev"][i],\
                             df["S&P 500 Weekly High"][i], df["S&P 500 Weekly Low"][i],\
                             df["S&P 500 Weekly Close"][i]),)
        i += 1
    db.commit()
    if (db.is_connected()):
        cursor.close()
        db.close()

def cum_returns(returns, starting_value=0):
    """
    Compute cumulative returns from simple returns.
    Parameters
    ----------
    returns : pd.Series
        Daily returns of the strategy, noncumulative.
         - See full explanation in tears.create_full_tear_sheet.
    starting_value : float, optional
       The starting returns (default 1).
    Returns
    -------
    pandas.Series
        Series of cumulative returns.
    Notes
    -----
    For increased numerical accuracy, convert input to log returns
    where it is possible to sum instead of multiplying.
    """

    return ep.cum_returns(returns, starting_value=starting_value)


def get_max_drawdown_underwater(underwater):
    """
    Determines peak, valley, and recovery dates given an 'underwater'
    DataFrame.
    An underwater DataFrame is a DataFrame that has precomputed
    rolling drawdown.
    Parameters
    ----------
    underwater : pd.Series
       Underwater returns (rolling drawdown) of a strategy.
    Returns
    -------
    peak : datetime
        The maximum drawdown's peak.
    valley : datetime
        The maximum drawdown's valley.
    recovery : datetime
        The maximum drawdown's recovery.
    """

    valley = underwater.idxmin()  # end of the period
    # Find first 0
    peak = underwater[:valley][underwater[:valley] == 0].index[-1]
    # Find last 0
    try:
        recovery = underwater[valley:][underwater[valley:] == 0].index[0]
    except IndexError:
        recovery = np.nan  # drawdown not recovered
    return peak, valley, recovery

def get_max_drawdown(returns):
    """
    Determines the maximum drawdown of a strategy.
    Parameters
    ----------
    returns : pd.Series
        Daily returns of the strategy, noncumulative.
        - See full explanation in :func:`~pyfolio.timeseries.cum_returns`.
    Returns
    -------
    float
        Maximum drawdown.
    Note
    -----
    See https://en.wikipedia.org/wiki/Drawdown_(economics) for more details.
    """

    returns = returns.copy()
    df_cum = cum_returns(returns, 1.0)
    running_max = np.maximum.accumulate(df_cum)
    underwater = df_cum / running_max - 1
    return get_max_drawdown_underwater(underwater)

def riskmetrics():
    db = get_connection()
    cursor = db.cursor(named_tuple=True)
    port_val_sql = "SELECT total_portfolio_value, price_date  FROM portfolio_value"
    cursor.execute(port_val_sql)
    port_vals = cursor.fetchall()
    num_vals = len(port_vals)
    periods = 365.25/allocation_period
    return_series = []
    value_series = []
    period_returns = []
    tear_returns = []
    tear_dates = []
    table = []
    i = 0
    
    while i < (num_vals-1):
        port_returns = (port_vals[i+1][0])/(port_vals[i][0])
        ann_returns = np.power([(float(port_returns))], periods)
        period_returns.append(port_returns)
        tear_returns.append(float(port_returns-1))
        tear_dates.append(port_vals[i+1][1])
        return_series.append(ann_returns)
        value_series.append(float(port_vals[i][0]))
        full_return = (((port_vals[i+1][0])/1000000)-1)*100
        i += 1

    tr_returns = pd.Series(tear_returns, index=tear_dates)
    sh_ratio = ((np.mean(return_series)) - 0.01) / np.std(return_series)
    mean = np.mean(return_series)
    std_dev = np.std(return_series)

    ret_mode = stats.mode(return_series)
    p_mean = np.mean(value_series)
    P_std_dev = np.std(value_series)

    pf.create_simple_tear_sheet(tr_returns)
    max_draw_down = get_max_drawdown(tr_returns)
    max_dd_start = max_draw_down[0]
    max_dd_end = max_draw_down[1]
    tport_val_sql = "SELECT total_portfolio_value  FROM portfolio_value WHERE price_date = %s"
    cursor.execute(tport_val_sql, (max_dd_start,))
    port_val_t = cursor.fetchall()
    peak_port_val = port_val_t[0][0]
    cursor.execute(tport_val_sql, (max_dd_end,))
    port_val_b = cursor.fetchall()
    valley_port_val = port_val_b[0][0]
    max_dd = '{}%'.format(round(((valley_port_val/peak_port_val)-1)*100, 2))
    
    header_vals = ["Return Statistics ", "Value"]
    table = (["Period (in years)", '{} years'.format(10)], ["Return (%)", '{}%'.format(round(full_return, 2))], ["Sharpe Ratio", sh_ratio],\
              ["Max Drawdown", max_dd],["Mean of Returns", mean], ["Std. Deviation of Returns", std_dev],\
              ["Return Mode", ret_mode[0]], ["Portfolio Mean", p_mean],\
              ["Portfolio Std. Deviation", P_std_dev])
    print(tabulate(table, headers = header_vals, tablefmt = "fancy_grid", numalign="right", floatfmt=".4f"))
    if (db.is_connected()):
        cursor.close()
        db.close()


cockpit_engine_start()
    
#plottheperformance()

#riskmetrics()

"""
#riskmetrics()

#plottheperformance()

#        
#getsp100()
    
#riskmetrics()
    
#sentimentData()

#get_sentiment('08-01-2019')
"""