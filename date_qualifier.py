#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 12:01:52 2020

@author: vikaslamba
"""

#import bs4 as bs
import datetime as dt
import holidays
#import numpy as np
#import datetime as dt
#import requests
#import yfinance as yf
# import csv
#import pandas_market_calendars as mcal
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, \
    USLaborDay, USThanksgivingDay

us_holidays = holidays.UnitedStates()  # or holidays.US()
#yf.pdr_override

hurricane_sandy_day_1 = dt.datetime.strptime('29-10-2012', '%d-%m-%Y')
hurricane_sandy_day_2 = dt.datetime.strptime('30-10-2012', '%d-%m-%Y')
g_bush_memorial_day = dt.datetime.strptime('05-12-2018', '%d-%m-%Y')
#cesar_chavez_day = dt.datetime.strptime('31-03-2017', '%d-%m-%Y')
nine_eleven_day_1 = dt.datetime.strptime('11-09-2001', '%d-%m-%Y')
nine_eleven_day_2 = dt.datetime.strptime('12-09-2001', '%d-%m-%Y')
nine_eleven_day_3 = dt.datetime.strptime('13-09-2001', '%d-%m-%Y')
nine_eleven_day_4 = dt.datetime.strptime('14-09-2001', '%d-%m-%Y')
juneteenth_22 = dt.datetime.strptime('20-06-2022', '%d-%m-%Y')

class USTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        Holiday('2nd Jan', month=1, day=2, observance=nearest_workday),
        Holiday('cesarchavezday', month=3, day=31, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]


def get_trading_close_holidays(year):
    inst = USTradingCalendar()
    return inst.holidays(dt.datetime(year-1, 12, 31), dt.datetime(year, 12, 31))


#if __name__ == '__main__':
#    holi_values = get_trading_close_holidays(2020)
#    print(holi_values)
    
#start_date = dt.datetime(2020, 1, 4)
#the_year = 2020

def qualify_date(the_date):
    hol_dates = get_trading_close_holidays(the_date.year)
    if the_date in hol_dates or (the_date == hurricane_sandy_day_1) \
    or (the_date == hurricane_sandy_day_2)\
    or (the_date == nine_eleven_day_1) or (the_date == nine_eleven_day_4):
        the_date += dt.timedelta(days=1)
        the_date = check_exception_days(the_date)
        the_date = return_weekday(the_date)
#        print('The date being qualified is :', the_date )
        return the_date
    else:
        the_date = return_weekday(the_date)
        the_date = check_exception_days(the_date)
#        print('The date is otherwise :', the_date )
        return the_date

def check_exception_days(the_date):
    if (the_date == hurricane_sandy_day_1) or (the_date == hurricane_sandy_day_2) \
    or (the_date == g_bush_memorial_day)\
    or(the_date == juneteenth_22)\
    or (the_date == nine_eleven_day_2):
        the_date += dt.timedelta(days=1)
        if (the_date == hurricane_sandy_day_2):
            the_date += dt.timedelta(days=1)
        elif (the_date == nine_eleven_day_3):
            the_date += dt.timedelta(days=5)
        return the_date
    elif (the_date == nine_eleven_day_4):
        the_date += dt.timedelta(days=4)
        return the_date
    else:
        return the_date

def check_holiday(the_date):
    hol_dates = get_trading_close_holidays(the_date.year)
    if the_date in hol_dates:
        the_date += dt.timedelta(days=1)
        return the_date
    else:
        return the_date
    
def return_weekday(one_date):
    weekno = one_date.weekday()
    if weekno<5:
#        one_date = check_holiday(one_date)
        return one_date
    elif weekno == 5:
        one_date += dt.timedelta(days=2)
        one_date = check_holiday(one_date)
        return one_date
    else:
        one_date += dt.timedelta(days=1)
        one_date = check_holiday(one_date)
        return one_date

#datem = qualify_date(dt.datetime.strptime('31-03-2017', '%d-%m-%Y'))
#print (datem)

#dt.datetime.strptime('2019-01-21', '%d-%m-%Y')
