#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 19 10:10:58 2024

@author: vikaslamba
"""

import datetime as dt
import quantstats as qs
# import os
# import pandas as pd
# from pandas_datareader import data as pdr
# from source_stock_data import get_data_from_yahoo
# from choose_high_mom_stocks import regression_mean, piotroski_screen, calc_weighted_z_score, find_monthly_return, find_the_signal
from date_qualifier import qualify_date
from source_and_calc import (
    get_data_from_yahoo, prepare_returns, 
    create_return_deciles, go_long_short, 
    portfolio_performance, portfolio_valuation, 
    getsp100, portfolio_returns, get_commodities_data, 
    save_all_US_tickers, get_all_us_data_from_yahoo
)
from perf import build_tearsheet

def engine_start():
    start_date = dt.datetime.strptime('30-03-2009', '%d-%m-%Y')
    end_date = dt.datetime.strptime('30-06-2019', '%d-%m-%Y')
    # end_date = dt.datetime.now()
    s_date = qualify_date(start_date) 
    end_date = qualify_date(end_date)
    previous_date = s_date
    qtr_count = 0
    strategy_period = 80
    allocation_period = 45
    return_type = "semi_annual_return"


    # portfolio turnover period is defined as 90 days or a quarter.
    """
    1.) get_data_from_yahoo()
    The below function sources daily returns from Yahoo and calculates the monthly and quarterly return of each stock  
    along with other indicators such as EMA21, EMA51, EMA200 etc. Data is saved to daily_returns table.
    This function does not need to be called each time the strategy is run as the stocks in the list should not change frequently.
    However, a test should be added to check if a new stock has been added and if so, the below function should be run?
    """
    # tickers = get_data_from_yahoo()
    # getsp100(start_date, end_date)

    # save_all_US_tickers()

    # tickers = get_all_us_data_from_yahoo()

    # get_commodities_data()
    
    # Add the loop here to run this for the start date specified above and then at the end - after go_long_short -
    # update the date to 90 days forward (to form a full quarter).
    # Continue the loop until 4 quarters have been covered.
    while qtr_count < strategy_period:
        """
        2.) prepare_returns(s_date)
        The below function sources daily returns from Yahoo and calculates the monthly and quarterly return of each stock  
        along with other indicators such as EMA21, EMA51, EMA200 etc. Data is saved to period_returns table.
        """
        prepare_returns(s_date)

        """
        3.) create_return_deciles(s_date)
        The below function creates the top and bottom deciles based on quarterly returns from the table period_returns
        and saves them to the table top_bottom_deciles.
        """
        create_return_deciles(s_date, return_type)

        """
        4.) go_long_short(return_date)
        The below function creates 5 long positions and 5 short positions based on the top and bottom deciles 
        and saves them to the table trade_portfolio.
        """
        go_long_short(s_date, previous_date, qtr_count, return_type)

        """
        5.) portfolio_valuation()
        The below function values the positions and in turn the value of the portfolio and stores it for each period.
        """

        portfolio_valuation(s_date, qualify_date(s_date + dt.timedelta(days=(1)*allocation_period)), qtr_count)

        """
        6.) portfolio_performance()
        The below function calculates the portfolio value as well as return amounts for each period and save it.
        """
        portfolio_performance(s_date, qualify_date(s_date + dt.timedelta(days=(1)*allocation_period)), qtr_count)
        

        """
        7.) Update the counters
        Update the counter values to ensure loop ends and also 
        forward the date for which the cycle should run
        """
        qtr_count += 1
        previous_date = s_date
        s_date = qualify_date(s_date + dt.timedelta(days=(1)*allocation_period))
    
    portfolio_returns(s_date, qualify_date(s_date + dt.timedelta(days=(1)*allocation_period)), qtr_count)
    build_tearsheet()


# Main body of the cockpit
# Here we call the required functions
engine_start()