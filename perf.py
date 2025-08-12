#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 16:48:58 2024

@author: vikaslamba
"""

import datetime as dt
from connection_pool import get_con_alchemy
import os
import pandas as pd
from dash import Dash, dcc, html, dash_table
from collections import OrderedDict
import quantstats as qs
import webbrowser as web

# print([f for f in dir(qs.stats) if f[0] != '_'])

def build_tearsheet():

    db = get_con_alchemy()
    df_port = pd.read_sql("SELECT value_date, portfolio_value, benchmark_value  FROM portfolio_performance WHERE value_date >= %(s_date)s AND value_date <= %(e_date)s", db,
                params={"s_date":dt.datetime.strptime('30-03-2002', '%d-%m-%Y'),"e_date":dt.datetime.strptime('27-03-2022', '%d-%m-%Y')})
    df_port['value_date'] = pd.to_datetime(df_port['value_date'], utc=True)
    df_port['portfolio_value'] = df_port['portfolio_value']
    df_port['benchmark_value'] = df_port['benchmark_value']
    df_port.columns = ["value_date", "portfolio_value", "benchmark_value"]
    df_port.set_index(['value_date'], inplace=True)
    my_port = df_port['portfolio_value']
    my_port.index = my_port.index.tz_localize(None)
    bench = df_port['benchmark_value']
    bench.index = bench.index.tz_localize(None)
    qs.extend_pandas()
    qs.reports.html(my_port, bench, rf=0.04, title="Portfolio vs Benchmark Performance", output="001_results/perform.html", compounded=True, match_dates=True)
    web.open_new(f"file:///{os.getcwd()}/001_results/perform.html")

# build_tearsheet()

# s_date = dt.datetime.strptime('01-03-2018', '%d-%m-%Y')
# e_date = dt.datetime.strptime('01-03-2023', '%d-%m-%Y')
# df = pd.read_sql("SELECT value_date, portfolio_value, portfolio_return  FROM portfolio_performance WHERE value_date >= %(s_date)s AND value_date <= %(e_date)s", db,
#              params={"s_date":dt.datetime.strptime('30-03-2013', '%d-%m-%Y'),"e_date":dt.datetime.strptime('27-03-2023', '%d-%m-%Y')})
# dfbench = pd.read_sql("SELECT value_date, portfolio_value, portfolio_return, benchmark_value  FROM portfolio_performance WHERE value_date >= %(s_date)s AND value_date <= %(e_date)s", db,
#              params={"s_date":dt.datetime.strptime('30-03-2013', '%d-%m-%Y'),"e_date":dt.datetime.strptime('27-03-2023', '%d-%m-%Y')})
# dfa = pd.read_sql("SELECT Date, Adj_Close, Daily_Return  FROM daily_price WHERE ticker = 'AMZN' AND Date >= %(s_date)s AND Date <= %(e_date)s", db,
#              params={"s_date":dt.datetime.strptime('30-03-2013', '%d-%m-%Y'),"e_date":dt.datetime.strptime('27-03-2023', '%d-%m-%Y')})
# dfb = pd.read_sql("SELECT Date, Adj_Close, Daily_Return  FROM daily_price WHERE ticker = 'NVDA' AND Date >= %(s_date)s AND Date <= %(e_date)s", db,
#              params={"s_date":dt.datetime.strptime('30-03-2013', '%d-%m-%Y'),"e_date":dt.datetime.strptime('27-03-2023', '%d-%m-%Y')})


# print('Available Stats: ', [fx for fx in dir(qs.stats) if fx[0] != "_'"])
# print(f"******************  Win Loss Ratio: {qs.stats.win_loss_ratio(df_port)}")
# print(f"******************  avg_return: {qs.stats.avg_return(df_port)}")
# print(f"******************  geometric_mean: {qs.stats.geometric_mean(df_port)}")
# print(f"******************  avg_win: {qs.stats.avg_win(df_port)}")
# print(f"******************  consecutive_losses: {qs.stats.consecutive_losses(df_port)}")
# print(f"******************  consecutive_wins: {qs.stats.consecutive_wins(df_port)}")
# print(f"******************  drawdown_details: {qs.stats.drawdown_details(df_port)}")
# print(f"******************  max_drawdown: {qs.stats.max_drawdown(df_port)}")
# print(f"******************  Best Day: {qs.stats.best(df_port)}")
# print(f"******************  monthly_returns: {qs.stats.monthly_returns(df_port)}")
# print(f"******************  cagr: {qs.stats.cagr(df_port)}")
# print(f"******************  rolling_sharpe: {qs.stats.rolling_sharpe(df_port)}")
# print(f"******************  expected_return: {qs.stats.expected_return(df_port)}")

"""
data = OrderedDict(
    [
        ("Stock", ["Portfolio Max and Min", "Benchmark Max and Min", "Sharpe plus Drawdown"]),
        ("Maximum Value", [df.portfolio_value.max(), dfbench.benchmark_value.max(), qs.stats.sharpe(df.portfolio_value)]),
        ("Minimum Value", [df.portfolio_value.min(), dfbench.benchmark_value.min(), qs.stats.max_drawdown(df.portfolio_value)]),
    ]
)

dfc = pd.DataFrame(data)

app = Dash(__name__)
app.title = 'Lamba Labs Analytics'

# ...

app.layout = html.Div(
    children=[
        html.H1(children="Portfolio Analytics"),
        html.P(
            children=(
                "Analyze the behavior of Portfolio value and the returns"
                " realized between 2018 and 2020"
                "Highest value for Apple in the period:  df.Adj_Close.max() "
            ),
        ),
        dash_table.DataTable(
            data=dfc.to_dict('records'),
            columns=[{'id': c, 'name': c} for c in dfc.columns]
        ),
                           
        dcc.Graph(
            figure={
                "data": [
                    {
                        "x": df.value_date,
                        "y": df.portfolio_value, 'name':'Portfolio', "color":'green',
                        "type": "lines",
                    },
                    {
                        "x": dfbench.value_date,
                        "y": dfbench.benchmark_value, 'name':'Benchmark - SP100', "color":'red',
                        "type": "lines",
                    },
                    # {
                    #     "x": dfa.Date,
                    #     "y": dfb.Adj_Close, 'name':'NVDA', 'line':dict(color='orange'),
                    #     "type": "lines",
                    # },
                ],
                "layout": {"title": "Portfolio Value"},
            },
        ),
        dcc.Graph(
            figure={
                "data": [
                    {
                        "x": df.value_date,
                        "y": df.portfolio_return,
                        "type": "lines",
                    },
                ],
                "layout": {"title": "Portfolio Returns"},
            },
        ),
        
    ]
)

# ...

if __name__ == "__main__":
    app.run_server(host='localhost',port=8005)
    # The below also works
    # app.run_server(debug=False)
    # The below does not work while Debug is set to True
    # app.run_server(host='0.0.0.0', port=8050, debug=True)

"""