#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 14:02:35 2020

@author: vikaslamba
"""

from connection_pool import get_connection
from date_qualifier import qualify_date
import datetime as dt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from mysql.connector import Error
import pickle
from source_stock_data import save_sp500_tickers
import statsmodels.api as sm
#import statsmodels.formula.api as smf
#from scipy import stats
#import matplotlib.pyplot as plt
#import seaborn as sns
#from numpy import nan
#from sklearn import preprocessing
import sqlalchemy
import os

"""
    The below code reads the csv price files for all stocks in the S&P 500 list
    and calculates the montly returns using the last day of each month's close 
    price. The monthly returns are then used to calculate the net annual return
    which is then sorted in a descending order and saved to a csv file. (The last part is not implemented currently.)
"""
def prepare_returns(f_date, reload_sp500=False):
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
            
    db = get_connection()
    cursor = db.cursor(named_tuple=True)
    rem_per_ret_sql = "Delete FROM period_returns"
    cursor.execute(rem_per_ret_sql)
    db.commit()
    
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
        price_sql = "SELECT Close FROM daily_price WHERE ticker = %s AND Date = %s"
        cursor.execute(price_sql, (tickers[i], short_past_date,))
        price_short_past = cursor.fetchall()
        cursor.execute(price_sql, (tickers[i], f_date,))
        price_start = cursor.fetchall()
        cursor.execute(price_sql, (tickers[i], ninety_days_past_date,))
        price_long_past = cursor.fetchall()  
        
        cursor.execute(price_sql, (tickers[i], semi_annual_past_date,))
        semi_annual_past_price = cursor.fetchall() 
        
        cursor.execute(price_sql, (tickers[i], annual_past_date,))
        annual_past_price = cursor.fetchall() 
        try:
            short_return = float(price_start[0][0])/float(price_short_past[0][0])
            long_return = float(price_start[0][0])/float(price_long_past[0][0])
            semi_annual_return = float(price_start[0][0])/float(semi_annual_past_price[0][0])
            annual_return = float(price_start[0][0])/float(annual_past_price[0][0])
            perf_sql = "insert into period_returns (id, ticker, return_date, price, short_return, long_return, semi_annual_return, annual_return) values(%s, %s, %s, %s, %s,  %s, %s,  %s)"
            cursor.execute(perf_sql, (i+1, tickers[i], f_date, price_start[0][0], short_return, long_return, semi_annual_return, annual_return),)
            db.commit()
        except:
            # Add a step to handle the exception
            pass
        
        i += 1
    if (db.is_connected()):
        cursor.close()
        db.close()

'''
def get_fundamentals(a_date, ticker):
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
            sql = "SELECT calc_date FROM fin_ratios WHERE ticker = %s"
            cursor.execute(sql,(ticker,))
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
            senti_sql = "SELECT net_profit_margin, ps_ratio, pb_ratio, pe_ratio FROM fin_ratios WHERE calc_date = %s AND ticker = %s"
            cursor.execute(senti_sql, (next_closest_date, ticker,))
            f_values = cursor.fetchall()
            print(f_values)
            return f_values
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
''' 
           
def predict_the_returns(s_date, e_date, reload_sp500=False):
#    The below code reads the stock prices and fundamental data for all stocks in the S&P 500 list
#    and calculates the regression series based on prices, PE, PB etc to build the model and then predict the 
#    prices. 
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
    
    db = get_connection()
    cursor = db.cursor(named_tuple=True, buffered = True)
    cursor.execute("Delete FROM model_results")
    db.commit()
    fail_value = 1
    for ticker in tickers:
#        ticker = ticker.replace('-', '.')
        ratios_available = False   
        global last_price
#        ticker = 'CCL'
        start_date = s_date
        end_date = e_date
        
#        start_date = '2018-04-27'
#        end_date = '2019-04-28'
#        second_start_date = '2019-02-01'
#        second_end_date = '2020-02-03'
        price_sql = "SELECT close_price, pb_ratio, pe_ratio FROM price_fundamentals WHERE ticker = %s AND price_date >= %s AND price_date <= %s"
        cursor.execute(price_sql, (ticker, start_date, end_date,))
        three_vals = cursor.fetchall()

        prices1 = np.array([0])
        '''
        We reshape prices1 so that........
        '''
        prices = prices1.reshape((1, 1))
        pb_pe_ratios = []
        cl_price = []
#        the_ratio_set1 = np.array([0, 0])
        the_ratio_set1 = np.array([0])
#        the_ratio_set = the_ratio_set1.reshape((1, 2))
        the_ratio_set = the_ratio_set1.reshape((1, 1))
        for j in range(len(three_vals)):
#            print('------------BBBBBBBBBBBBBBBBBBBBBBBBBB---------')
            ratios_available = True
            closing_price = three_vals[j][0]
            cl_price1 = np.array([closing_price])
            cl_price = cl_price1.reshape((1,1))
            prices = np.append(prices, cl_price, axis=0)
    #        pb_pe_ratios.append(three_vals[j][1])
            pb_ratio = three_vals[j][1]
            pe_ratio = three_vals[j][2]
            last_price = closing_price
#            print('Ticker and size -----------------------------------------------------', start_date, end_date, ticker, last_price, len(prices))
            if (pb_ratio is None) or (pe_ratio is None) or (len(three_vals) < 4):
#                print('Setting ratios to false -----------------------------------------------------', ticker)
#                print('------------CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC---------')
                ratios_available = False
                break
            else:
#                pb_pe_ratios1 = np.array([pb_ratio, pe_ratio])
                pb_pe_ratios1 = np.array([pe_ratio])
#                pb_pe_ratios = pb_pe_ratios1.reshape((1, 2))
                pb_pe_ratios = pb_pe_ratios1.reshape((1, 1))
#                print('------------DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD---------')
                the_ratio_set = np.append(the_ratio_set, pb_pe_ratios, axis=0)
        if ratios_available:
            the_ratio_set = np.delete(the_ratio_set, (0), axis=0)
#            print('------------EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE---------')
            '''
                Commented out the deletion of last record below. This was earlier added to ensure that last ratios are left out for prediction.
            '''
            the_ratio_set = np.delete(the_ratio_set, (-1), axis=0)
            prices = np.delete(prices, (0, 1), axis=0)
#            print('----------------------------------------------------------------------------------', ticker) 
        #    print('The ratios are : ', the_ratio_set.shape, the_ratio_set) 
#            print('----------------------------------------------------------------------------------', ticker) 
        #    x = [[0, 1], [5, 1], [15, 2], [25, 5], [35, 11], [45, 15], [55, 34], [60, 35]]
        #    y = [4, 5, 20, 14, 32, 22, 38, 43]
            x = the_ratio_set
            y = prices
            '''
            New code added below to create data to be used for prediction with the model
            '''
            new_ratio_sql = "SELECT pb_ratio, pe_ratio FROM fin_ratios WHERE ticker = %s AND calc_date > %s"
            cursor.execute(new_ratio_sql, (ticker, end_date,))
            new_ratios_vals = cursor.fetchall()
            
#            new_ratio_1 = np.array([0, 0])
#            new_ratio = new_ratio_1.reshape((1, 2))
            
            new_ratio_1 = np.array([0])
            new_ratio = new_ratio_1.reshape((1, 1))
            try:
                new_pb_ratio = new_ratios_vals[0][0]
                new_pe_ratio = new_ratios_vals[0][1]
#                print('------------FFFFFFFFFFFFFFFFFFFFFFFFFFF---------')
            except:
                '''
                The new ratios are being forced to have a value above 0 and not be None
                '''
                new_pb_ratio = 0.1
                new_pe_ratio = 0.1
#                print('------------GGGGGGGGGGGGGGGGGGGGGGGGG---------')
#            dummy = 1      
#            for x in [0, 1, 2]:
            if (new_pb_ratio is not None) and (new_pe_ratio is not None):
#                print('Ticker and its ratios -----------------------------------------------------', ticker, new_pb_ratio, new_pe_ratio)
#                print('------------HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH---------')
        
#                new_pb_pe_ratios1 = np.array([new_pb_ratio, new_pe_ratio])
                new_pb_pe_ratios1 = np.array([new_pe_ratio])
#                new_pb_pe_ratios = new_pb_pe_ratios1.reshape((1, 2))
                new_pb_pe_ratios = new_pb_pe_ratios1.reshape((1, 1))
#                print('Coming here?  -----------------------------------------------------', ticker)
                new_ratio = np.append(new_ratio, new_pb_pe_ratios, axis=0)
                new_ratio = np.delete(new_ratio, (0), axis=0)
                
                '''
                New code added above
                '''
            
                new_x_01 = new_ratio
#                new_x = new_x_01.reshape((1, 2))
                new_x = new_x_01.reshape((1, 1))
            #    x, y = np.array(x), np.array(y)
#                print('Ticker and x and y -----------------------------------------------------', ticker, x, y)
                model = LinearRegression().fit(x, y)
                r_sq = model.score(x, y)
#                print('-------- Value of new_x :', new_x)
    #            print('Model Summary ------------------------------ :', model.summary())
#                print('intercept:', ticker, float(model.intercept_))
#                print('------slope:', model.coef_[0][0], model.coef_[0][1])
                y_pred = model.predict(new_x)
                pred_size = len(y_pred)
    #            print('predicted response:', y_pred.shape, y_pred[pred_size//8], y_pred[pred_size//4], y_pred[pred_size//2], y_pred[pred_size-1], sep='\n')
                model_name = '2_factor_model'
                predicted_Return = (((y_pred[pred_size//8])/ float(last_price))-1)*100
#                print('Here are the values --------------', ticker, round(float(r_sq), 6))
#                print('Here are the values --------------', ticker, round(float(model.intercept_), 2))
#                print('Here are the values --------------', ticker, round(float(model.coef_[0][0]), 6))
#                print('Here are the values --------------', ticker, round(float(model.coef_[0][1]), 6))
#                print('Here are the values --------------', ticker, round(float(predicted_Return), 6))
#                print('Here are the values --------------', ticker, round(float(y_pred[pred_size//4]), 6))
#                print('Here are the values --------------', ticker, round(float(y_pred[pred_size//2]), 6))
#                print('Here are the values --------------', ticker, round(float(y_pred[pred_size-1]), 6))
#                print('------------IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII---------')
#                perf_sql = "insert into model_results (model_name, ticker, Coeff_of_det, intercept, slope_1, slope_2, pred_1, pred_2, pred_3, Pred_4) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
#                cursor.execute(perf_sql, (model_name, ticker, round(float(r_sq), 6), round(float(model.intercept_), 2), round(float(model.coef_[0][0]), 6), round(float(model.coef_[0][1]), 6), round(float(predicted_Return), 6), round(float(y_pred[pred_size//4]), 6), round(float(y_pred[pred_size//2]), 6), round(float(y_pred[pred_size-1]), 6)),)
                perf_sql = "insert into model_results (model_name, ticker, Coeff_of_det, intercept, slope_2, pred_1, pred_2, pred_3, Pred_4) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(perf_sql, (model_name, ticker, round(float(r_sq), 6), round(float(model.intercept_), 2), round(float(model.coef_[0][0]), 6), round(float(predicted_Return), 6), round(float(y_pred[pred_size//4]), 6), round(float(y_pred[pred_size//2]), 6), round(float(y_pred[pred_size-1]), 6)),)
                db.commit()
            
            else:
                print('-------- Ignoring the ticker -------- :', ticker)
                pass
            
        else:
            pass

#    model_tickers = "SELECT ticker FROM model_results WHERE Coeff_of_det > %s AND Coeff_of_det < %s AND slope_2 > %s AND pred_1 > %s"
#    cursor.execute(model_tickers, ('0.08', '0.82', '-0.2', '-3',))
    
    stocks = pd.read_sql("SELECT ticker FROM model_results WHERE intercept > %s AND Coeff_of_det > %s AND Coeff_of_det < %s AND slope_2 > %s AND pred_1 > %s", db, params=('-50', '0.1', '0.92', '-3', '-1',))
    tickers_required = stocks['ticker']
#    cursor.execute(tickers_required)
#    tickers_test = cursor.fetchall()
    print('------------- Here are the test tickers......................: ------------', tickers_required)
    
#    with open("modeltickers.pickle", "rb") as f:
#        modeltickers = pickle.dump(tickers_list, f)    
    
    fob = open("modeltickers.pickle", "wb")
    pickle.dump(tickers_required, fob)  
    fob.close()
    
    with open("modeltickers.pickle", "rb") as f:
            mod_tickers = pickle.load(f)
    print('-------- Here are the model tickers------------ : ', mod_tickers)
    return mod_tickers
    
#    Apply this model to new data 
#    x_new = np.arange(10).reshape((-1, 2))
#    print(x_new)
#    y_new = model.predict(x_new)
#    print(y_new)

""" Commented out temporarily
def move_fundamentals(reload_sp500=False):
    start_date = dt.datetime.strptime('01-01-2009', '%d-%m-%Y')
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
    for ticker in tickers:
        ticker = ticker.replace('-', '.')
        print('The ticker value is ----- : ', ticker)
        db = get_connection()
        cursor = db.cursor(named_tuple=True)
        fundamental_sql = "INSERT INTO price_fundamentals (ticker, price_date, close_price, pe_ratio) SELECT d.ticker,\
            d.Date,\
            d.Close,\
            (SELECT q.pe_ratio\
            FROM fin_ratios AS q\
            WHERE q.ticker = %s AND d.ticker = %s\
            AND q.calc_date >= %s\
            	AND q.calc_date <= d.Date\
            ORDER BY q.calc_date DESC\
            LIMIT 1)\
            FROM daily_price AS d WHERE d.ticker = %s and d.Date >= %s"
        cursor.execute(fundamental_sql, (ticker, ticker, start_date, ticker, start_date,))
        db.commit()
    if (db.is_connected()):
        print('The DB connection is about to be closed. The above ticker should be submitted now.')
        db.close()
    return 1
     
 """    
def piotroski_screen(for_date, reload_sp500=False):
#    The below code reads the stock  data for all stocks in the S&P 500 list
#    and calculates the Z-score for each stock.
#    Get the exposure for each stock to each factor and calculate the Z-score
#    First, calculate the mean for each factor and save in the DB?
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
    db = get_connection()
    cursor = db.cursor(named_tuple=True, buffered = True)
    database_username = 'root'
    database_password = 'V!shnuPurana36'
    database_ip       = 'localhost'
    database_name     = 'Time_Series'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    factor_list = ['ps_ratio', 'pe_ratio', 'pcf_ratio', 'graham_number', 'ev_ebitda', 'rd_revenue', 'volume', 'rsi', 'ema21', 'ema50']
    for factor in factor_list:
        factor_sql = "SELECT "+ factor + " FROM price_fundamentals WHERE price_date = %s"
        cursor.execute(factor_sql, (for_date,))
        factor_val = cursor.fetchall()
        df = pd.DataFrame(factor_val, columns = ['value'])
        df['value'] = pd.to_numeric(df['value'], errors='ignore')
        mean_results = df.mean(axis=0)
        m_results = pd.DataFrame([mean_results])
        std_dev = df.std(axis=0)
        s_dev_results = pd.DataFrame([std_dev])
        '''
        Calculate the Universe Standard Deviation
        for each ticker:
            Get the factor value and calculate the factor Z-score
            Store the factor Z-score for each ticker
            Calculate the mean Z-Score for each ticker - using z-scores for all factors
        '''
        for ticker in tickers:
            factor_sql = "SELECT "+ factor + " FROM price_fundamentals WHERE ticker = %s AND price_date = %s"
            cursor.execute(factor_sql, (ticker, for_date,))
            ticker_factor = cursor.fetchall()
            ticker_factor_df = pd.DataFrame(ticker_factor, columns=['factor_val'])
            ticker_factor_df['factor_val'] = pd.to_numeric(ticker_factor_df['factor_val'], errors = 'ignore')
            f_z_score = (ticker_factor_df['factor_val'] - m_results['value'])/s_dev_results['value']
#            print('The factor_z_score value for ' +ticker+ 'is : ', f_z_score, for_date, ticker, factor)
            f_df = pd.DataFrame(columns = ['calc_date', 'ticker', 'factor_name', 'factor_value', 'univ_factor_mean', 'std_deviation', 'factor_z_score', 'weighted_z_score'])
            f_df['factor_value'] = ticker_factor_df['factor_val']
            f_df['univ_factor_mean'] = m_results['value']
            f_df['std_deviation'] = s_dev_results['value']
            f_df['factor_z_score'] = f_z_score
            f_df['weighted_z_score'] = 0
            f_df['calc_date'] = for_date
            f_df['ticker'] = ticker
            f_df['factor_name'] = factor            
            f_df.to_sql(con=database_connection, name='z_scores', if_exists='append', index=False)

def calc_weighted_z_score(for_date, reload_sp500=False):
#    The below code reads the stock  data for all stocks in the S&P 500 list
#    and calculates the Z-score for each stock.
#    Get the exposure for each stock to each factor and calculate the Z-score
#    First, calculate the mean for each factor and save in the DB?
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
    db = get_connection()
    cursor = db.cursor(named_tuple=True, buffered = True)
    database_username = 'root'
    database_password = 'V!shnuPurana36'
    database_ip       = 'localhost'
    database_name     = 'Time_Series'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    factor_list = ['ps_ratio', 'pe_ratio', 'pcf_ratio', 'graham_number', 'ev_ebitda', 'rd_revenue', 'volume', 'rsi', 'ema21', 'ema50']
#    for factor in factor_list:
#        factor_sql = "SELECT factor_z_score FROM z_scores WHERE price_date = %s"
#        cursor.execute(factor_sql, (for_date,))
#        factor_val = cursor.fetchall()
#        df = pd.DataFrame(factor_val, columns = ['value'])
#        df['value'] = pd.to_numeric(df['value'], errors='ignore')
#        mean_results = df.mean(axis=0)
#        m_results = pd.DataFrame([mean_results])
#        std_dev = df.std(axis=0)
#        s_dev_results = pd.DataFrame([std_dev])
    '''
    Calculate the Universe Standard Deviation
    for each ticker:
        Get the factor value and calculate the factor Z-score
        Store the factor Z-score for each ticker
        Calculate the mean Z-Score for each ticker - using z-scores for all factors
    '''
    for ticker in tickers:
        factor_sql = "SELECT factor_z_score FROM z_scores WHERE ticker = %s AND calc_date = %s"
        cursor.execute(factor_sql, (ticker, for_date,))
        ticker_z_score = cursor.fetchall()
        ticker_z_score_df = pd.DataFrame(ticker_z_score, columns=['z_score_val'])
        ticker_z_score_df['z_score_val'] = pd.to_numeric(ticker_z_score_df['z_score_val'], errors = 'ignore')
        
        weighted_z_score = ticker_z_score_df.iloc[9][0] + ticker_z_score_df.iloc[8][0] + ticker_z_score_df.iloc[7][0] + \
            ticker_z_score_df.iloc[5][0] + ticker_z_score_df.iloc[4][0] - \
            ticker_z_score_df.iloc[2][0] - ticker_z_score_df.iloc[1][0] - \
            ticker_z_score_df.iloc[0][0]
        f_w_z_score = pd.DataFrame(data=[weighted_z_score], columns=['weighted_z_score'])
#        f_w_z_score['weighted_z_score'] = weighted_z_score
#        f_w_z_score = pd.DataFrame(weighted_z_score)
#        
        print('The 2 values are :..... ', ticker, f_w_z_score)
        
        z_df = pd.DataFrame(columns = ['calc_date', 'factor_name', 'ticker', 'weighted_z_score'])
#        f_df['factor_value'] = ticker_factor_df['factor_val']
#        f_df['univ_factor_mean'] = m_results['value']
#        f_df['std_deviation'] = s_dev_results['value']
#        f_df['factor_z_score'] = f_z_score
        z_df['weighted_z_score'] = f_w_z_score['weighted_z_score']
        z_df['factor_name'] = 'weighted_value'
        z_df['calc_date'] = for_date
        z_df['ticker'] = ticker         
        z_df.to_sql(con=database_connection, name='z_scores', if_exists='append', index=False)        
       
def regression_mean():
#    The below code reads the stock regression data for all stocks in the S&P 500 list
#    and calculates the mean of the regression series.
    
    db = get_connection()
    cursor = db.cursor(named_tuple=True, buffered = True)
    database_username = 'root'
    database_password = 'V!shnuPurana36'
    database_ip       = 'localhost'
    database_name     = 'Time_Series'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    factor_list = ['PS_Ratio', 'PE_Ratio', 'Pcf_Ratio', 'Graham_Num', 'EV_EBITDA', 'RD_Revenue', 'Volume', 'RSI', 'EMA21', 'EMA50']
    for factor in factor_list:
        PS_sql1 = "SELECT coeff, std_error, t_statistic, p_value FROM regression_returns WHERE factor = %s"
        cursor.execute(PS_sql1, (factor,))
        factor_val1 = cursor.fetchall()   
        df = pd.DataFrame(factor_val1, columns = ['coeff', 'std_error', 't_statistic', 'p_value'])
        df['coeff'] = pd.to_numeric(df['coeff'], errors='ignore')
        df['std_error'] = pd.to_numeric(df['std_error'], errors='ignore')
        df['t_statistic'] = pd.to_numeric(df['t_statistic'], errors='ignore')
        df['p_value'] = pd.to_numeric(df['p_value'], errors='ignore')
        mean_results = df.mean(axis=0)
#        print('The mean results of the factor : ', factor, mean_results, mean_results[0], mean_results[1], mean_results[2], mean_results[3])
        m_results = pd.DataFrame([mean_results], columns = ['coeff', 'std_error', 't_statistic', 'p_value'])
        m_results['coeff'] = mean_results[0]
        m_results['std_error'] = mean_results[1]
        m_results['t_statistic'] = mean_results[2]
        m_results['p_value'] = mean_results[3]
        m_results.insert(4, 'ticker', 'Universe10')
        m_results.insert(5, 'reg_model_name', 'Fundamenal-3')
        m_results.insert(6, 'factor', factor)
        '''
        Save the regression results universe average for each stock to the DB
        '''
        m_results.to_sql(con=database_connection, name='regression_returns', if_exists='append', index=False)


"""
#    The below code reads the stock prices and fundamental data for all stocks in the S&P 500 list
#    and calculates the regression series based on prices, PE, PB etc to build the model and then predict the 
#    prices.
"""
def regression_returns(s_date, e_date, reload_sp500=False):
#    The below code reads the stock prices and fundamental data for all stocks in the S&P 500 list
#    and calculates the regression series based on prices, PE, PB etc to build the model and then predict the 
#    prices. 
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
    
    db = get_connection()
    cursor = db.cursor(named_tuple=True, buffered = True)
    '''
    Commented the Delete command below to keep all results
    '''
#    cursor.execute("Delete FROM model_results")
    db.commit()
    
    database_username = 'root'
    database_password = 'V!shnuPurana36'
    database_ip       = 'localhost'
    database_name     = 'Time_Series'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    fail_value = 1
    data = [[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]]
    fin_stats = pd.DataFrame(data, columns = ['coeff', 'std_error', 't_statistic', 'p_value'])
    fin_stats['coeff'] = pd.to_numeric(fin_stats['coeff'], errors='ignore')
    fin_stats['std_error'] = pd.to_numeric(fin_stats['std_error'], errors='ignore')
    fin_stats['t_statistic'] = pd.to_numeric(fin_stats['t_statistic'], errors='ignore')
    fin_stats['p_value'] = pd.to_numeric(fin_stats['p_value'], errors='ignore')
    for ticker in tickers:
        if ticker != 'BRK-B' or ticker != 'BF-B':
            ticker = ticker.replace('-', '.')
        ratios_available = False   
        global last_price
#        print('2 - The flowing ticker changed is : ', ticker)
        start_date = s_date
        end_date = e_date
        
        price_sql = "SELECT daily_return, ps_ratio, pe_ratio, pcf_ratio, graham_number, ev_ebitda, rd_revenue, volume, rsi, ema21, ema50, ema200, pfe, price_date FROM price_fundamentals WHERE ticker = %s AND price_date >= %s AND price_date <= %s AND price_date <= %s"
        cursor.execute(price_sql, (ticker, start_date, end_date, dt.datetime.strptime('29-10-2020', '%d-%m-%Y')))
        three_vals = cursor.fetchall()
#        print('The date and length of returns is : ', dt.datetime.strptime('29-10-2020', '%d-%m-%Y'), len(three_vals))
        '''
        We reshape prices1 so that........
        '''       
        pb_pe_ratios = []
        d_return = []
        the_ratio_set1 = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        prices1 = np.array([0])
#        the_ratio_set1 = np.array([0])
        the_ratio_set = the_ratio_set1.reshape((1, 11))
        prices = prices1.reshape((1, 1))
        for j in range(len(three_vals)):
            ratios_available = True
            daily_return = three_vals[j][0]
#            d_return1 = np.array([daily_return])
#            d_return = d_return1.reshape((1,1))
#            print('The price shape & values & len are : ', prices.shape, prices, len(three_vals))
#            prices = np.append(prices, d_return, axis=0)
#            print('The price shape & values after appending are : ', prices.shape, prices)
    #        pb_pe_ratios.append(three_vals[j][1])
            ps_ratio = three_vals[j][1]
            pe_ratio = three_vals[j][2]
            pcf_ratio = three_vals[j][3]
            graham_number = three_vals[j][4]
            ev_ebitda = three_vals[j][5]
            rd_revenue = three_vals[j][6]
            volume = three_vals[j][7]
            rsi = three_vals[j][8]
            ema21 = three_vals[j][9]
            ema50 = three_vals[j][10]
            ema200 = three_vals[j][11]
            pfe = three_vals[j][12]
#            price_date = three_vals[j][13]
            last_price = daily_return
            if (ps_ratio is None) or (pe_ratio is None) or (len(three_vals) < 4):
#                print('Setting ratios to false -----------------------------------------------------', ticker)
                ratios_available = False
                break
            else:
                pb_pe_ratios1 = np.array([daily_return, ps_ratio, pe_ratio, pcf_ratio, graham_number, ev_ebitda, rd_revenue, volume, rsi, ema21, ema50])
                pb_pe_ratios = pb_pe_ratios1.reshape((1, 11))
                the_ratio_set = np.append(the_ratio_set, pb_pe_ratios, axis=0)

        if ratios_available:
            the_ratio_set = np.delete(the_ratio_set, (0), axis=0)

            '''
                Commented out the deletion of last record below. 
                This was earlier added to ensure that last ratios are left out for prediction.
            '''
#                the_ratio_set = np.delete(the_ratio_set, (-1), axis=0)
            prices = np.delete(prices, (0), axis=0)
#            print('--------------------------------- Prices -------------------------------------------------', prices.tolist()) 
#            print('The ratios are : ', the_ratio_set.shape, the_ratio_set, prices.shape, prices) 
            '''
            dataset = pd.DataFrame()
            dataset['PE_Ratio'] = the_ratio_set[:, 1:2].tolist()
            dataset['PB_Ratio'] = the_ratio_set[:, 0:1].tolist()
            dataset['PS_Ratio'] = prices.tolist()
            '''
#            x = the_ratio_set
            dataset = pd.DataFrame(the_ratio_set, columns=['D_Returns', 'PS_Ratio', 'PE_Ratio', 'Pcf_Ratio', 'Graham_Number', 'EV_EBITDA', 'RD_Revenue', 'Volume', 'RSI', 'EMA21', 'EMA50'])
            '''
            The below code 
            '''
            dataset.dropna(axis=0, inplace=True)
#            dataset = dataset[dataset.RD_Revenue != 0]
#            print('------------- The Dataset is: -------------------', dataset)
            if len(dataset) > 0:
                x1 = dataset.iloc[:, 1:11]
                '''
                Commenting out the normalisation of the data. It will need to be 
                brought in once the model transforms to a ML model.
                '''
#                min_max_scaler = preprocessing.MinMaxScaler()
#                x_scaled = min_max_scaler.fit_transform(x1)
#                x = pd.DataFrame(x_scaled)
                x = pd.DataFrame(x1)
#                print('........... The data shape is ------------ :', ticker, len(x), x.shape)
                y = dataset.iloc[:, 0:1]
    #            y = prices
                '''
                New code added below to create data to be used for prediction with the model. Using out of sample data
                that is after the end date.
                '''
                new_ratio_sql = "SELECT pb_ratio, pe_ratio, ps_ratio FROM fin_ratios WHERE ticker = %s AND calc_date > %s"
                cursor.execute(new_ratio_sql, (ticker, end_date,))
                new_ratios_vals = cursor.fetchall()
                
                new_ratio_1 = np.array([0, 0, 0])
                new_ratio = new_ratio_1.reshape((1, 3))

                try:
                    new_pb_ratio = new_ratios_vals[0][0]
                    new_pe_ratio = new_ratios_vals[0][1]
                    new_ps_ratio = new_ratios_vals[0][2]
                except:
                    '''
                    The new ratios are being forced to have a value above 0 and not be None
                    '''
                    new_pb_ratio = 0.1
                    new_pe_ratio = 0.1
                    new_ps_ratio = 0.1

            if (new_pe_ratio is not None) and (new_ps_ratio is not None) and (len(dataset) > 0):
        
                new_pb_pe_ratios1 = np.array([new_pb_ratio, new_pe_ratio, new_ps_ratio])
                new_pb_pe_ratios = new_pb_pe_ratios1.reshape((1, 3))
                new_ratio = np.append(new_ratio, new_pb_pe_ratios, axis=0)
                new_ratio = np.delete(new_ratio, (0), axis=0)
                
                '''
                New code added above
                '''   
                '''
                Commenting out temporarily as it is though to not be in use. Only x & y values are used further down in model2
                '''
                '''
                new_x_01 = new_ratio
                new_x = new_x_01.reshape((1, 3))
#                print('Ticker and x and y -----------------------------------------------------', ticker)
                model = LinearRegression()
                model.fit(x, y)
                r_sq = model.score(x, y)
                
                '''
#                x = sm.add_constant(x)
#                print('Model Summary by fitting------------------------------ :', sm.OLS(y, x).fit().summary())
 
                '''
                plt.figure()
                plt.scatter(x=list(range(0, 667)),y= dataset['PE_Ratio'].tolist(), color='blue')         
                plt.scatter(x=list(range(0, 667)), y= dataset['PB_Ratio'].tolist(), color='black')
                plt.show()
                '''
#                '''
#                Creating an array of type float including all the independent variables.            
                x = np.array(x, dtype=float)
                y = np.array(y, dtype=float)
#                slope, intercept, r_value, p_value, std_err = stats.linregress(x,y)
#                print('---------------------Here are all the values  : ', slope, intercept, r_value, p_value, std_err)
                
                
#                x = sm.add_constant(x)
                model2 = sm.OLS(y, x).fit()
#                predictions = model.predict(x) 
                 
                print_model = model2.summary()
                print('The model summary is.....................',print_model)

#                alpha = 0.5
                params = model2.params
                RSq = model2.rsquared
                StdErr = model2.bse
                pvalues = model2.pvalues
                tvalues = model2.tvalues
                F_prob = model2.f_pvalue
                
                res_df = pd.DataFrame()
                res_df['coeff'] = params
                res_df['std_error'] = StdErr
                res_df['t_statistic'] = tvalues
                res_df['p_value'] = pvalues
#                print('............ Printing the value of Params: ', params)
                res_df['coeff'] = pd.to_numeric(res_df['coeff'], errors='ignore')
                res_df['std_error'] = pd.to_numeric(res_df['std_error'], errors='ignore')
                res_df['t_statistic'] = pd.to_numeric(res_df['t_statistic'], errors='ignore')
                res_df['p_value'] = pd.to_numeric(res_df['p_value'], errors='ignore')
#                print('Here is the starting fin_stats value::::::::::::::::::::::::', fin_stats)
                '''
                The below code adds the regression values  of the previous stock to the regression values 
                of the current stock and takes a mean of the values. The mean values are then printed.
                In the end, it will provide the mean regression values for entire Universe.
                These values are not the actual regression values of the stock being regressed right now.
                For a particular stock's regression values, please see model2.summary() values above.
                '''
                '''
                df = pd.concat([fin_stats, res_df], sort=False)
#                print('............ Printing the value of res_def: ', res_df, df)
                foo = df.groupby(df.index).mean()
#                print('............ Printing the value of Foo: ', foo)
                fin_stats = foo
                '''
                print('Ticker and x and y -----------------------------------------------------', ticker)
                reg_results = pd.DataFrame()
                reg_results['coeff'] = res_df['coeff']
                reg_results['std_error'] = res_df['std_error']
                reg_results['t_statistic'] = res_df['t_statistic']
                reg_results['p_value'] = res_df['p_value']
                factor_names = ['PS_Ratio', 'PE_Ratio', 'Pcf_Ratio', 'Graham_Num', 'EV_EBITDA', 'RD_Revenue', 'Volume', 'RSI', 'EMA21', 'EMA50']
                reg_results.insert(4, 'factor', factor_names)
                reg_results.insert(5, 'ticker', ticker)
                reg_results.insert(6, 'reg_model_name', 'Fundamenal-3')

#                reg_results['t_statistic'] = reg_results['t_statistic'].fillna(0)
                reg_results = reg_results.fillna(0)
#                print('Again...................', reg_results['t_statistic'])
                '''
                Save the regression results for each stock to the DB
                '''
                reg_results.to_sql(con=database_connection, name='regression_returns', if_exists='append', index=False)
#                factor_names = ['PS_Ratio', 'PE_Ratio', 'Pcf_Ratio', 'Graham_Num', 'EV_EBITDA', 'RD_Revenue', 'Volume', 'RSI', 'EMA21', 'EMA50']
#                used_factors = pd.DataFrame(factor_names)
#                print('............ Printing the value of used_factors: ', factor_names)
                '''
                foo.insert(4, 'factor', factor_names)
                foo.insert(5, 'ticker', 'Universe')
                foo.insert(6, 'reg_model_name', 'Fundamenal-3')
                '''
                '''
                Save the regression results mean for the complete universe to the DB
                '''                
#                foo.to_sql(con=database_connection, name='regression_returns', if_exists='append', index=False)
                '''
                params = np.append(model.intercept_,model.coef_)
                predictions = model.predict(x)
                newX = pd.DataFrame({"Constant":np.ones(len(x))}).join(pd.DataFrame(x))
                MSE = (sum((y-predictions)**2))/(len(newX)-len(newX.columns))
                var_b = MSE*(np.linalg.inv(np.dot(newX.T,newX)).diagonal())
                sd_b = np.sqrt(var_b)
                ts_b = params/ sd_b
#                print('---------------------Here are the 2 values - len of newX and len of newX[0] : ', (newX), (newX[0]))
                p_values =[2*(1-stats.t.cdf(np.abs(i),(len(newX)-len(newX[0])))) for i in ts_b]
                sd_b = np.round(sd_b,3)
                ts_b = np.round(ts_b,3)
                p_values = np.round(p_values,3)
                params = np.round(params,4)
                print('...............Here are the 3 values - MSE, var_b, sd_b, ts_b, p_values :  ', MSE, var_b, sd_b, ts_b, p_values)
                myDF3 = pd.DataFrame()
                myDF3["Coefficients"],myDF3["Standard Errors"],myDF3["t values"],myDF3["Probabilities"] = [params,sd_b,ts_b,p_values]
                print(myDF3)
                '''
#                Re-activate the prediction code below - uncomment
                '''
                y_pred = model.predict(new_x)
                pred_size = len(y_pred)
                model_name = '2_factor_model'
                predicted_Return = (((y_pred[pred_size//8])/ float(last_price))-1)*100
#                print('Here are the values --------------', ticker, round(float(r_sq), 6))
#                print('Here are the values --------------', ticker, round(float(model.intercept_), 2))
#                print('Here are the values --------------', ticker, round(float(model.coef_[0][0]), 6))
#                    print('Here are the values --------------', ticker, round(float(model.coef_[0][1]), 6))
#                print('Here are the values --------------', ticker, round(float(predicted_Return), 6))
#                print('Here are the values --------------', ticker, round(float(y_pred[pred_size//4]), 6))
#                print('Here are the values --------------', ticker, round(float(y_pred[pred_size//2]), 6))
#                print('Here are the values --------------', ticker, round(float(y_pred[pred_size-1]), 6))
#                print('------------IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII---------')
#                perf_sql = "insert into model_results (model_name, ticker, Coeff_of_det, intercept, slope_1, slope_2, pred_1, pred_2, pred_3, Pred_4) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
#                cursor.execute(perf_sql, (model_name, ticker, round(float(r_sq), 6), round(float(model.intercept_), 2), round(float(model.coef_[0][0]), 6), round(float(model.coef_[0][1]), 6), round(float(predicted_Return), 6), round(float(y_pred[pred_size//4]), 6), round(float(y_pred[pred_size//2]), 6), round(float(y_pred[pred_size-1]), 6)),)
                perf_sql = "insert into model_results (model_name, ticker, Coeff_of_det, intercept, slope_2, pred_1, pred_2, pred_3, Pred_4) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(perf_sql, (model_name, ticker, round(float(r_sq), 6), round(float(model.intercept_), 2), round(float(model.coef_[0][0]), 6), round(float(predicted_Return), 6), round(float(y_pred[pred_size//4]), 6), round(float(y_pred[pred_size//2]), 6), round(float(y_pred[pred_size-1]), 6)),)
                db.commit()
                '''
            else:
                print('-------- Ignoring the ticker -------- :', ticker)
                pass
            
        else:
            pass
        

#    Commenting the below snippet - do we need it if we are not delivering the tickers back?
'''
#    model_tickers = "SELECT ticker FROM model_results WHERE Coeff_of_det > %s AND Coeff_of_det < %s AND slope_2 > %s AND pred_1 > %s"
#    cursor.execute(model_tickers, ('0.08', '0.82', '-0.2', '-3',))
    
    stocks = pd.read_sql("SELECT ticker FROM model_results WHERE intercept > %s AND Coeff_of_det > %s AND Coeff_of_det < %s AND slope_2 > %s AND pred_1 > %s", db, params=('-50', '0.1', '0.92', '-3', '-1',))
    tickers_required = stocks['ticker']
#    cursor.execute(tickers_required)
#    tickers_test = cursor.fetchall()
    print('------------- Here are the test tickers......................: ------------', tickers_required)
    
#    with open("modeltickers.pickle", "rb") as f:
#        modeltickers = pickle.dump(tickers_list, f)    
    
    fob = open("modeltickers.pickle", "wb")
    pickle.dump(tickers_required, fob)  
    fob.close()
    
    with open("modeltickers.pickle", "rb") as f:
            mod_tickers = pickle.load(f)
    print('-------- Here are the model tickers------------ : ', mod_tickers)
    return mod_tickers

'''

"""
    The below function generates and provides the monthly return of each stock by using the daily return values 
    for the given date range. The daily returns used are the ones generated and saved earlier - see the function 
    get_data_from_yahoo()
"""
def find_monthly_return(s_date, e_date, reload_sp500=False):
#    The below code reads the stock prices and fundamental data for all stocks in the S&P 500 list
#    and calculates the regression series based on prices, PE, PB etc to build the model and then predict the 
#    prices. 
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
    
    db = get_connection()
    cursor = db.cursor(named_tuple=True, buffered = True)
    '''
    Commented the Delete command below to keep all results
    '''
#    cursor.execute("Delete FROM model_results")
    db.commit()
    
    database_username = 'root'
    database_password = 'V!shnuPurana36'
    database_ip       = 'localhost'
    database_name     = 'Time_Series'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    fail_value = 1
    data = [[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]]
    fin_stats = pd.DataFrame(data, columns = ['coeff', 'std_error', 't_statistic', 'p_value'])
    fin_stats['coeff'] = pd.to_numeric(fin_stats['coeff'], errors='ignore')
    fin_stats['std_error'] = pd.to_numeric(fin_stats['std_error'], errors='ignore')
    fin_stats['t_statistic'] = pd.to_numeric(fin_stats['t_statistic'], errors='ignore')
    fin_stats['p_value'] = pd.to_numeric(fin_stats['p_value'], errors='ignore')
    for ticker in tickers:
        if ticker != 'BRK-B' or ticker != 'BF-B':
            ticker = ticker.replace('-', '.')
        global last_price
        start_date = s_date
        end_date = e_date
        
        price_sql = "SELECT price_date, daily_return FROM price_fundamentals WHERE ticker = %s AND price_date >= %s AND price_date <= %s AND price_date <= %s"
        cursor.execute(price_sql, (ticker, start_date, end_date, dt.datetime.strptime('29-10-2020', '%d-%m-%Y')))
        three_vals = cursor.fetchall()
     
        pb_pe_ratios = []
        the_ratio_set1 = np.array([0, 0])
        the_ratio_set = the_ratio_set1.reshape((1, 2))
        for j in range(len(three_vals)):
            price_date = three_vals[j][0]
            daily_return = three_vals[j][1]
            
            pb_pe_ratios1 = np.array([daily_return, price_date])
            pb_pe_ratios = pb_pe_ratios1.reshape((1, 2))
            the_ratio_set = np.append(the_ratio_set, pb_pe_ratios, axis=0)
            
        dataset = pd.DataFrame(the_ratio_set, columns=['D_Returns', 'Price_Date'])
        dataset['month'] = dataset['Price_Date'].astype(str).str[:7]
        dataset = dataset.drop('Price_Date', axis='columns')
        monthly_return = dataset.groupby('month').sum()
        monthly_return['Ticker'] = ticker
        print(monthly_return)

#    Apply this model to new data 
#    x_new = np.arange(10).reshape((-1, 2))
#    print(x_new)
#    y_new = model.predict(x_new)
#    print(y_new)
        
        

    """
    The below function compares the price with EMA200, EMA50 and EMA 21 and 
    generates the signal.
    """

"""
    The below function generates buy, hold or sell signals using EMA21, EMA50 & EMA200 values and saves signals on a csv file.
"""
def find_the_signal(s_date, e_date, reload_sp500=False):

    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
    
    db = get_connection()
    cursor = db.cursor(named_tuple=True, buffered = True)
    '''
    Commented the Delete command below to keep all results
    '''
#    cursor.execute("Delete FROM model_results")
    db.commit()
    
#    database_username = 'root'
#    database_password = 'V!shnuPurana36'
#    database_ip       = 'localhost'
#    database_name     = 'Time_Series'
#    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
#                                                   format(database_username, database_password, 
#                                                          database_ip, database_name))
#    fail_value = 1
#    data = [[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]]
#    fin_stats = pd.DataFrame(data, columns = ['coeff', 'std_error', 't_statistic', 'p_value'])
#    fin_stats['coeff'] = pd.to_numeric(fin_stats['coeff'], errors='ignore')
#    fin_stats['std_error'] = pd.to_numeric(fin_stats['std_error'], errors='ignore')
#    fin_stats['t_statistic'] = pd.to_numeric(fin_stats['t_statistic'], errors='ignore')
#    fin_stats['p_value'] = pd.to_numeric(fin_stats['p_value'], errors='ignore')
    for ticker in tickers:
        if ticker != 'BRK-B' or ticker != 'BF-B':
            ticker = ticker.replace('-', '.')
        global last_price
        start_date = s_date
        end_date = e_date
        
        price_sql = "SELECT ticker, Date, Close, EMA21, EMA50, EMA200 FROM daily_price WHERE ticker = %s AND Date >= %s AND Date <= %s AND DATE <= %s"
        cursor.execute(price_sql, (ticker, start_date, end_date, dt.datetime.strptime('29-10-2020', '%d-%m-%Y')))
        three_vals = cursor.fetchall()
     
#        pb_pe_ratios = []
#        the_ratio_set1 = np.array([0, 0, 0, 0, 0])
#        the_ratio_set = the_ratio_set1.reshape((1, 5))
        for j in range(len(three_vals)):
            stock = three_vals[j][0]
            date = three_vals[j][1]
            price = three_vals[j][2]
            ema21 = three_vals[j][3]
            ema50 = three_vals[j][4]
#            ema200 = three_vals[j][5]
            signal = 'none'
            
            
            df = pd.DataFrame()
            if (price >= ema50):
#                print('***** This is a strong buy signal for. ***** ', date, stock, price, ema50)
                signal = 'BUY'
                new_row = {'Date': date, 'Stock': stock, 'Price': price, 'EMA21': ema21, 'EMA50': ema50, 'Signal': signal}
                df['Date'] = date
                df['Stock'] = stock
                df['Price'] = price
                df['EMA21'] = ema21
                df['EMA50'] = ema50
                df['Signal'] = signal
                df.loc[len(df)] = new_row
#                print('@@@@@ This is a strong signal for - @@@@@ ', date, stock, price, signal, df['Signal'].tolist())
                

            elif(price > ema21):
#                print('@@@@@ This is a weak buy signal for - @@@@@ ', date, stock, price, ema21)
                signal = 'WEAK BUY'
                new_row = {'Date': date, 'Stock': stock, 'Price': price, 'EMA21': ema21, 'EMA50': ema50, 'Signal': signal}
                df['Date'] = date
                df['Stock'] = stock
                df['Price'] = price
                df['EMA21'] = ema21
                df['EMA50'] = ema50
                df['Signal'] = signal
                df.loc[len(df)] = new_row
            elif(price <= ema21):
#                print('@@@@@ This is a strong sell signal for - @@@@@ ', date, stock, price, ema21)
                signal = 'SELL'
                new_row = {'Date': date, 'Stock': stock, 'Price': price, 'EMA21': ema21, 'EMA50': ema50, 'Signal': signal}
                df['Date'] = date
                df['Stock'] = stock
                df['Price'] = price
                df['EMA21'] = ema21
                df['EMA50'] = ema50
                df['Signal'] = signal
                df.loc[len(df)] = new_row

#            if not os.path.exists('Signals/{}.csv'.format(ticker), mode='a', header = not os.path.exists('Signals/{}.csv'.format(ticker))):
            
            print('@@@@@ This is a signal for - @@@@@ ', date, stock, price, signal, df['Signal'].tolist())
#            df.reset_index(inplace=True)
#            print('@@@@@ This is a strong signal for - @@@@@ ', date, stock, price, signal, df['Signal'].tolist())
#            df.set_index("Date", inplace=True)                
#                df.to_csv('Signals/{}.csv'.format(ticker))
            df.to_csv('Signals/{}.csv'.format(ticker), mode='a', header = not os.path.exists('Signals/{}.csv'.format(ticker)))
#            else:
#                pass
    #            print('Already have {}'.format(ticker))


#        dataset = pd.DataFrame(the_ratio_set, columns=['D_Returns', 'Price_Date'])
#        dataset['month'] = dataset['Price_Date'].astype(str).str[:7]
#        dataset = dataset.drop('Price_Date', axis='columns')
#        monthly_return = dataset.groupby('month').sum()
#        monthly_return['Ticker'] = ticker
#        print(monthly_return)