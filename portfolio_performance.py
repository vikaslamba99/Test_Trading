#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 15:04:49 2020

@author: vikaslamba
"""

import pandas as pd
#import Momentum.allocation_period
#from pandas import DataFrame
from date_qualifier import qualify_date
import datetime as dt
from datetime import datetime
#from datetime import date
from mysql.connector import Error
#from os import path
import mysql.connector as mysql
from connection_pool import get_connection
from source_stock_data import retrieve_values
import sqlalchemy
num_counter = 0
"""
This is the investment amount at the beginning of the trading cycle.
"""
initial_investment = 1000000.00
current_investment = initial_investment
"""
This is the amounts of the funds available to invest. At the beginning it is  
set to the value of the initial investment and then changes with each 
transaction to the money left in the pot. It can go up and down after each transaction.
"""
funds_available = initial_investment
trade_portfolio_flag = 0
reserve_funds = int()
port_rise = int()
current_portfolio_value = int()
highest_port_value = int()
flight_safety = 2
#global allocation_period
#cycle = 'up'

db = mysql.connect(
    database = "Time_Series",
    host = "localhost",
    user = "root",
    passwd = "V!shnuPurana36")

def get_sentiment(a_date):
    global current_investment
    global funds_available   

    try:
#        connection = mysql.connect(§
#        database = "Time_Series",
#        host = "localhost",
#        user = "root",
#        passwd = "V!shnuPurana36")
        connection = get_connection()
        if connection.is_connected():
            cursor = connection.cursor(named_tuple=True)
#            record = cursor.fetchone()
            a_date = pd.to_datetime(a_date, dayfirst=True)
            sql = "SELECT sentiment_date FROM sentiment"
            cursor.execute(sql,)
            senti_dates = [item[0] for item in cursor.fetchall()]
            past_dates = [date
                  for date in senti_dates
                  if date <= a_date]
            next_closest_date = max(past_dates)
            """
            ============= Comment ===================
            Should not qualify and get a different date after finding the required date.
            The date found above has to be used as it will exist in the sentiment dates.
            final_date = qualify_date(next_closest_date)
            """
            senti_sql = "SELECT bullish, bullish_8_week_MA FROM sentiment WHERE sentiment_date = %s"
            cursor.execute(senti_sql, (next_closest_date,))
            bullish_val = cursor.fetchall()
            return bullish_val
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()

def lets_trade(a_date, cycle):
        global current_investment
        global funds_available
        global trade_portfolio_flag
        global reserve_funds
        global port_rise
        global highest_port_value
        global current_portfolio_value
        global flight_safety
        all_period = 30
#        global cycle
        fall = 1
#        db = mysql.connect(
#            database = "Time_Series",
#            host = "localhost",
#            user = "root",
#            passwd = "V!shnuPurana36")
        db = get_connection()
        cursor = db.cursor(named_tuple=True)
        record = cursor.fetchone()
#        return2 = int()
        first_date = qualify_date(a_date) 
        past_date = qualify_date(first_date - dt.timedelta(days=7))         # start date for medium term portfoilio building
        one_month_past_date = qualify_date(first_date - dt.timedelta(days=33)) 
        two_months_past_date = qualify_date(first_date - dt.timedelta(days=62)) 
        one_year_past_date = qualify_date(first_date - dt.timedelta(days=180))
        i = 0
        """
        The below code is used to reduce the investment quota and add to 
        reserves in cases of sharp fall in the portfolio value.
        """
        port_rise = 1
        flight_safety += 1
        port_val_sql = "SELECT total_portfolio_value  FROM portfolio_value"
        cursor.execute(port_val_sql)
        port_vals = cursor.fetchall()
        num_vals = len(port_vals)
#        cycle = 1
        if (num_vals > 1):
            k = 0
            while k < (num_vals-1):
                port_val_1 = port_vals[k+1][0]
                port_val_2 = port_vals[k][0]
                current_portfolio_value = port_val_1
                if (port_val_2 > port_val_1) and (port_val_2 > highest_port_value):
                    highest_port_value = port_val_2
                elif (port_val_1 > port_val_2) and (port_val_1 > highest_port_value):
                    highest_port_value = port_val_1
                else:
                    highest_port_value = highest_port_value
                port_rise = port_val_1/port_val_2
                k += 1 
#            print("The highest port value is : ", highest_port_value, current_portfolio_value, a_date)
            fall = float(current_portfolio_value/highest_port_value)
            """
            The below rule for the cycles being marked as up and down needs to be reviewed and upgraded.
            """
            if (fall <=0.9):
                cycle = 'down'
                all_period = 30
            elif(fall >= 0.98):
                cycle = 'up'
                all_period = 30
            else:
                fall = fall
#            if ((0.91 > port_rise >= 0.82) and (reserve_funds == 0)) :
#                reserve_funds = funds_available * 0.36
#                funds_available = funds_available * 0.64
#            elif (port_rise < 0.82) and (reserve_funds == 0) :
#                reserve_funds = funds_available * 0.72
#                funds_available = funds_available * 0.28
#            elif (port_rise > 1) and (reserve_funds > 0):
#                funds_available += reserve_funds
#                reserve_funds = 0
#            elif (port_rise == 1):
#                funds_available = funds_available
#        print('**********8 The highest portfolio value is : ', highest_port_value)
#        print('**********8 The reserve_funds value is : ', reserve_funds)
#        print('**********8 The funds_available value is : ', funds_available)
#        return2 = 1
#        fall = float(current_portfolio_value/highest_port_value)
        
#        s_indicator = get_sentiment(first_date)
#        if (flight_safety >= 5):
#            if (fall <= 0.85) and (s_indicator[0][0] < 0.37):
#                flight_safety = 0
#                flight_to_safety(fall, first_date)
            
        else:
            pass
        count_sql = "SELECT ticker, quantity, price, txn_date FROM trade_portfolio"
        cursor.execute(count_sql)
        current_stocks = cursor.fetchall()        
#            return t_series
        if(len(current_stocks) > 0):         
            j = 0
            while j>=0 and j<len(current_stocks):
                ticker_held = current_stocks[j][0]
                ticker_quantity_held = current_stocks[j][1]
                ticker_held_price = current_stocks[j][2]
                ticker_buy_date = current_stocks[j][3]

                time_series_current_s = retrieve_values(ticker_held, first_date)
#                print("Here is ticker and date :", ticker_held, first_date, a_date)
#                print("Here is current price :", time_series_current_s[0][0])
#                print("Here is held price :", ticker_held_price)

#                time_series_past = retrieve_values(ticker, past_date)
                return_2 = float(time_series_current_s[0][0])/float(ticker_held_price)
                latest_price = time_series_current_s[0][0]
                latest_daily_return = time_series_current_s[0][1]
                latest_ema21 = time_series_current_s[0][2]
                latest_ema200 = time_series_current_s[0][3]
                latest_rsi = time_series_current_s[0][4]
                latest_pfe = time_series_current_s[0][5]
                latest_slope_fast = time_series_current_s[0][6]
                latest_slope_medium = time_series_current_s[0][7]
#                return2 = (ticker_prices['Close'][first_date]/ticker_held_price)
                previous_date =  qualify_date(first_date - dt.timedelta(days=14))
                time_series_previous_s = retrieve_values(ticker_held, previous_date)
                previous_price = time_series_previous_s[0][0]
                return_since_previous = (latest_price/previous_price)
#                not using return_since_previous for now - replaced with return_since_highest
                txn_date_sql = "SELECT txn_date FROM trade_portfolio WHERE ticker = %s"
                cursor.execute(txn_date_sql, (ticker_held,))
                txn_date = cursor.fetchall()
                txn_date = txn_date[0][0]
        
                price_s_sql = "SELECT Close FROM daily_price WHERE ticker = %s AND Date <= %s AND Date > %s"
                cursor.execute(price_s_sql, (ticker_held, first_date, txn_date,))
                prices = cursor.fetchall() 
                max_since_bought_price = pd.DataFrame(prices).max()
#                print('.........................................  ', first_date, ticker_held, ticker_quantity_held, latest_pfe, latest_rsi)
#                if (latest_pfe < -1):
#                    print('......................................... Negative  ', first_date, ticker_held, ticker_quantity_held, latest_pfe, latest_rsi)
#                return_since_highest = float(latest_price)/float(max_since_bought_price)
#                if (return_since_highest < 0.83) or (return_2 > 1.17) or (latest_price  < latest_ema21):   # Momentum not established   or (current_portfolio_value < (0.95 * highest_port_value))
                
#                    if (latest_price  < latest_ema21) or (return_since_highest < 0.8) or (return_2 > 1.21) or (latest_slope_fast > 0.00036) or (latest_slope_fast < latest_slope_medium) or (latest_pfe < 30) or (latest_rsi < 35) or (latest_daily_return < 0.0):
#                if (return_since_highest  < 0.8) or (return_2 > 1.29)\
#                or ((first_date.date() - ticker_buy_date).days > 85 and (0.9 < return_2 < 1.025)): 
                if (return_2  < 0.79) or ((return_2 > 1.3) and (return_2 < 1.39))\
                or ((return_2 > 1.5) and (return_2 < 1.63))\
                or ((return_2 > 1.8)):
#                        or (ticker_prices['RSI1'][first_date]  < 42) or (ticker_prices['RSI1'][first_date]  > 62):
#                        Again, keeping RSI out of the game for now.
#                    or (ticker_prices['RSI1'][first_date]  < 31) or (ticker_prices['RSI1'][first_date]  > 69): # MA established
                    momentum_positive = False
                    EMA20_signal = False
                    """
                    The actual date, the next business day, on which the trade will occur.
                    """
                    trade_date = qualify_date(first_date + dt.timedelta(days=1))
                    quantity = ticker_quantity_held
                    if (quantity > 0):
                        sell(ticker_held, quantity, trade_date, momentum_positive, EMA20_signal)
                    else:
                        pass
                else:
                    pass
#                else:
#                    pass
                    
                j += 1
        print('The current cycle is : ', cycle)
        if (cycle == 'up'):    
            top_stocks_sql = "select ticker from period_returns WHERE short_return > 1 AND long_return > 1 AND semi_annual_return > 1 ORDER BY semi_annual_return DESC"
        elif (cycle == 'down'):
#            top_stocks_sql = "select ticker from period_returns WHERE short_return > 0.75  AND short_return < 0.9 AND semi_annual_return > 1.15 AND annual_return > 1.5 ORDER BY short_return ASC"
            top_stocks_sql = "select ticker from period_returns WHERE short_return > 1 AND long_return > 1 AND semi_annual_return > 1 ORDER BY semi_annual_return DESC"
        cursor.execute(top_stocks_sql)
        top_stocks = cursor.fetchall()
        
        while i<(len(top_stocks)):
            counter = 0
            ticker = top_stocks[i][0]
            momentum_positive = False
            EMA20_signal = False
            if (ticker == 'TT') or(ticker == 'AMCR'):
                pass
            else:    
                time_series_current = retrieve_values(ticker, first_date)
#                print('The ticker is :', ticker, first_date, past_date, two_months_past_date)
#                print('The current time series is: ', time_series_current)
                time_series_past = retrieve_values(ticker, past_date)
                time_series_2_months = retrieve_values(ticker, two_months_past_date)
#                print('The 2 months time series is: ', time_series_2_months)
                return_1 = time_series_current[0][0]/time_series_past[0][0]
                return_2_months = time_series_current[0][0]/time_series_2_months[0][0]
                latest_price = time_series_current[0][0]
                latest_daily_return = time_series_current[0][1]
                latest_ema21 = time_series_current[0][2]
                latest_ema200 = time_series_current[0][3]
                latest_rsi = time_series_current[0][4]
                latest_pfe = time_series_current[0][5]
                latest_slope_fast = time_series_current[0][6]
                latest_slope_medium = time_series_current[0][7]
                """
                Check that its not a penny stock and rising Momentum has been established
                """
                if (return_1 > 1) and (return_2_months > 1.01) and (ticker != 'TT'):  #    and (return2 > return3 > 1.01)
#                        Edited this from below line - latest_price > latest_ema21 > latest_ema200
                    if (latest_price > latest_ema21 > latest_ema200) and (0 < ((latest_price/latest_ema21)-1)< 0.1) and (latest_daily_return > 0):
#                    if (latest_price > latest_ema21 > latest_ema200)\
#                    and (0 < ((latest_price/latest_ema21)-1)< 0.1)\
#                    and (35 < latest_rsi < 69) and (latest_slope_fast > 0)\
#                    and (latest_daily_return > 0):
                        momentum_positive = True
                        EMA20_signal = True
                        count_sql = "SELECT * FROM trade_portfolio"
                        cursor.execute(count_sql)
                        current_stocks = cursor.fetchall()
                        """
                        The actual date, the next business day, on which the trade will occur.
                        """
                        trade_date = qualify_date(first_date + dt.timedelta(days=1))
                        if(len(current_stocks) > 0):
                            try:
                                qty_sql = "SELECT ticker, quantity FROM trade_portfolio WHERE ticker = %s"
                                cursor.execute(qty_sql, (ticker,))
                                qty_stock = cursor.fetchall()
                                quantity = qty_stock[0][1]
                            except:
                                quantity = 0
                            if (quantity == 0) and funds_available>(6000):
                                s_indicator = get_sentiment(first_date)
#                                if ((s_indicator[0][0] > s_indicator[0][1]) ) or ((s_indicator[0][0] > 0.37)):
#                                if ((s_indicator[0][0] > 0.37) ):
                                go_long(ticker, latest_price, trade_date, funds_available, momentum_positive, EMA20_signal)
                                counter += 1
                        else:
                            s_indicator = get_sentiment(first_date)
#                            if ((s_indicator[0][0] > s_indicator[0][1])) or ((s_indicator[0][0] > 0.37)):
#                            if ((s_indicator[0][0] > 0.37)):
                            go_long(ticker, latest_price, trade_date, funds_available, momentum_positive, EMA20_signal)
                            counter += 1
                                
                    else:
                        pass
                else:
                    pass
            i += 1        
        if (db.is_connected()):
            cursor.close()
            db.close()
        return trade_portfolio_flag, cycle, all_period

def flight_to_safety(fall, curr_date):
    """
    Find the currently active stocks and their quantity in the portfolio.
    If level 1 safety required, sell 25% of stocks.
    If level 2 safety required, sell 50% of stocks.
    If severe safety required, sell 100% of stocks. 
    """
    global funds_available
    global reserve_funds
    action_history = []
    price_bump = 0.03
#    connection = mysql.connect(
#            database = "Time_Series",
#            host = "localhost",
#            user = "root",
#            passwd = "V!shnuPurana36")
    connection = get_connection()
    cursor = connection.cursor(named_tuple=True)
    record = cursor.fetchone()
    active_stocks_sql = "SELECT ticker, quantity  FROM trade_portfolio"
    cursor.execute(active_stocks_sql)
    active_stocks = cursor.fetchall()
#    fall = float(current_portfolio_value/highest_port_value)
    i = 0
    print('......... The fall value is : ', fall)
    while i < len(active_stocks):
        stock = active_stocks[i][0]
        quantity = active_stocks[i][1]
        if (0.94 > fall > 0.69):
            sell_qty = float(quantity//3)
#        elif (0.89 >= fall):
#            sell_qty = float(quantity//2)
        remaining_qty = float(quantity) - sell_qty
        price_sql = "SELECT Close FROM daily_price WHERE ticker = %s AND Date = %s"
        cursor.execute(price_sql, (stock, curr_date,))
        st_price = cursor.fetchall()   
        price = float(st_price[0][0]) * (1-price_bump)
        commission = 5.0
        sale_funds = (sell_qty * price)
        new_funds = sale_funds - commission
#        if (sell_qty > 0):
#        print('............... Prior Funds available ', funds_available)    
        update_qty_sql = "Update trade_portfolio SET quantity = %s WHERE ticker = %s"
        cursor.execute(update_qty_sql, (remaining_qty, stock,))
#        up_qty = cursor.fetchall()
        connection.commit()                    
#        funds_available = funds_available + new_funds
        reserve_funds = reserve_funds + new_funds
        action_history.append([curr_date, stock, sell_qty, price, new_funds, 'sell', funds_available])
#        print('..............................', funds_available, new_funds, stock, curr_date)
        sell_fraction = str(round(sell_qty, 2)) + 'SELL'
        sell_action_sql = "Update action_funds SET sell_date = %s, mkt_price = %s, sell_action = %s, funds_available = %s, sale_funds = %s WHERE ticker = %s AND buy_sell_indicator = %s"
        cursor.execute(sell_action_sql, (curr_date, price, sell_fraction, funds_available, new_funds, stock, 'BUY',))
#        up_sell = cursor.fetchall()
        connection.commit()
#        print('..............  sell_fraction ', sell_fraction)
        i += 1
    
def go_long(ticker, stock_price, t_date, funds, momentum_positive, EMA20_signal):
    database_username = 'root'
    database_password = 'V!shnuPurana36'
    database_ip       = 'localhost'
    database_name     = 'Time_Series'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
#    db = mysql.connect(
#        database = "Time_Series",
#        host = "localhost",
#        user = "root",
#        passwd = "V!shnuPurana36")
    db = get_connection()
    cursor = db.cursor(named_tuple=True)
    record = cursor.fetchone()
    constituents = []
    action_history = []
    global trade_portfolio_flag
    price_bump = 0.02
    stop_buying = False
    global funds_available
    if momentum_positive == True:
        if EMA20_signal == True:
            price = float(stock_price) * (1+price_bump)
            commission = 5.0
            funds_to_trade = funds_available
            count_sql = "SELECT * FROM trade_portfolio"
            cursor.execute(count_sql)
            current_stock_count = cursor.fetchall()        
            if(len(current_stock_count) > 0):

                quantity = len(current_stock_count)
                if (quantity < 10):
                    funds_to_trade = funds / (10 - quantity)
                else:
                    stop_buying = True
            else:
                funds_to_trade = funds / (10)
            if (stop_buying == False):
                no_of_stocks = ((funds_to_trade - commission)//price)
                
                """                
                save the ticker, no_of_stocks, price at which bought to the inventory file
                update the available_funds value by deducting the amount spend on stocks + commission
                update the portfolio size with the market price of the stock and leftover cash.
                """
                invested_amount = price * no_of_stocks
                funds_available = funds_available - invested_amount - commission
                value = price * no_of_stocks
                constituents.append([t_date, ticker, no_of_stocks, price, value])
                action_history.append([t_date, ticker, no_of_stocks, price, value, 'buy', funds_available])
                dfp = pd.DataFrame([1], columns = ['a'])
                
                dfp['txn_date'] = t_date
                dfp['ticker'] = ticker
                dfp['quantity'] = round(no_of_stocks, 2)
                dfp['price'] = round(price, 4)
                dfp['value'] = round(value, 4)
                dfp.drop('a', axis=1, inplace=True)
                dfp.to_sql(con=database_connection, name='trade_portfolio', if_exists='append', index=False)
                db.commit()
                database_connection.dispose()
                
                dfaction = pd.DataFrame([1], columns = ['temp'])
                dfaction['ticker'] = ticker
                dfaction['quantity'] = round(no_of_stocks, 2)
                dfaction['buy_date'] = t_date
                dfaction['sell_date'] = t_date
                dfaction['buy_sell_indicator'] = 'BUY'
                dfaction['sell_action'] = ''
                dfaction['funds_available'] = funds_available
                dfaction['txn_price'] = round(price, 4)
                dfaction['bought_value'] = round(value, 4)
                dfaction.drop('temp', axis=1, inplace=True)
                dfaction.to_sql(con=database_connection, name='action_funds', if_exists='append', index=False)
                db.commit()
                database_connection.dispose()
                
            else:
                pass
            
    if (db.is_connected()):
        cursor.close()
        db.close()
            
def snapshot(first_date, second_date, n):
    port_constituents = []
    portfolio_value = 0
    port_return = 0
    total_p_return = 0
    total_funds = 0
    global funds_available
    global trade_portfolio_flag
#    db = mysql.connect(
#        database = "Time_Series",
#        host = "localhost",
#        user = "root",
#        passwd = "V!shnuPurana36")
    db = get_connection()
    cursor = db.cursor(named_tuple=True)
    record = cursor.fetchone()
    
    database_username = 'root'
    database_password = 'V!shnuPurana36'
    database_ip       = 'localhost'
    database_name     = 'Time_Series'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    
    count_sql = "SELECT ticker, quantity FROM trade_portfolio"
    cursor.execute(count_sql)
    current_stocks = cursor.fetchall()        
    if(len(current_stocks) > 0):
        j = 0
        while j<len(current_stocks):
            ticker_held = current_stocks[j][0]
            ticker_quantity_held = current_stocks[j][1]
            
            price_sql = "SELECT Close FROM daily_price WHERE ticker = %s AND Date = %s"
            cursor.execute(price_sql, (ticker_held, second_date,))
            market_price = cursor.fetchall()

            market_value = float(ticker_quantity_held) * float(market_price[0][0])

            portfolio_value += market_value
            j += 1
        if n<2:
            benchmark_value = 1 * 1000000
            port_return = 0
            total_p_return = 0
            total_funds = funds_available + portfolio_value + reserve_funds
        else:
            
            price_p_sql = "SELECT portfolio_value, benchmark_value FROM portfolio_value"
            cursor.execute(price_p_sql)
            port_vals = cursor.fetchall()
            no_of_records = len(port_vals)
            port_return = float(portfolio_value)/float(port_vals[no_of_records-1][0])
            total_p_return = float(portfolio_value)/float(port_vals[0][0])
            price_second_sql = "SELECT close_price FROM sp100 WHERE price_date = %s"
            cursor.execute(price_second_sql, (second_date,))
            sp100_second_price = cursor.fetchall()
            price_first_sql = "SELECT close_price FROM sp100 WHERE price_date = %s"
            cursor.execute(price_first_sql, (first_date,))
            sp100_first_price = cursor.fetchall()
            benchm_val = port_vals[no_of_records-1][1]
            sp_return = sp100_second_price[0][0]/sp100_first_price[0][0]
            benchmark_value = float(sp_return) * float(benchm_val)
            total_funds = funds_available + portfolio_value + reserve_funds
        port_constituents.append([second_date, portfolio_value, port_return, total_p_return, benchmark_value, total_funds])
        
        dfport = pd.DataFrame([1], columns = ['temp'])
        dfport['price_date'] = second_date
        dfport['portfolio_value'] = round(portfolio_value, 4)
        dfport['portfolio_return'] = round(port_return, 4)
        dfport['total_return'] = total_p_return
        dfport['benchmark_value'] = round(benchmark_value, 4)
        dfport['total_portfolio_value'] = round(total_funds, 4)
        dfport.drop('temp', axis=1, inplace=True)
        dfport.to_sql(con=database_connection, name='portfolio_value', if_exists='append', index=False)
        db.commit()
        database_connection.dispose()
        trade_portfolio_flag += 1
        
    else:
        pass
    if (db.is_connected()):
        cursor.close()
        db.close()

def sell(ticker, quantity, curr_date, momentum_positive, EMA20_signal):
    global funds_available
    price_bump = 0.03
    action_history = []
#    db = mysql.connect(
#        database = "Time_Series",
#        host = "localhost",
#        user = "root",
#        passwd = "V!shnuPurana36")
    db = get_connection()
    cursor = db.cursor(named_tuple=True)
    record = cursor.fetchone()
    count_sql = "SELECT * FROM trade_portfolio"
    cursor.execute(count_sql)
    current_stock_count = cursor.fetchall()        
    if(len(current_stock_count) > 0):
        stock_qty_sql = "SELECT quantity FROM trade_portfolio WHERE ticker = %s"
        cursor.execute(stock_qty_sql, (ticker,))
        stock_series = cursor.fetchall()   
        quantity = stock_series[0][0]
        if momentum_positive == False:
            if EMA20_signal == False:
                price_sql = "SELECT Close FROM daily_price WHERE ticker = %s AND Date = %s"
                cursor.execute(price_sql, (ticker, curr_date,))
                st_price = cursor.fetchall()   
                price = float(st_price[0][0]) * (1-price_bump)
                commission = 5.0
                sale_funds = (float(quantity) * price)
                new_funds = sale_funds - commission

                if (quantity > 0):
                    
                    remove_sql = "DELETE FROM trade_portfolio WHERE ticker = %s"
                    cursor.execute(remove_sql, (ticker,))
                    db.commit()                    
                    funds_available = funds_available + new_funds
                    action_history.append([curr_date, ticker, quantity, price, new_funds, 'sell', funds_available])
#                    Start of code to save in DB
                    
                    sell_action_sql = "Update action_funds SET sell_date = %s, mkt_price = %s, buy_sell_indicator = %s, funds_available = %s, sale_funds = %s WHERE ticker = %s AND buy_sell_indicator = %s"
                    cursor.execute(sell_action_sql, (curr_date, price, 'SELL', funds_available, new_funds, ticker, 'BUY',))

                    db.commit()

#                    End of code to save in DB
                else:
                    pass
            
            else:
                pass
    else:
        print('The portfolio file with stocks could not be found while selling.')

    if (db.is_connected()):
        cursor.close()
        db.close()
 