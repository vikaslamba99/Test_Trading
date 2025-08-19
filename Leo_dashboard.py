# Author: Vikas Lamba
# Date: 2025-08-19
# Description: This is the main dashboard application for the Leo Trading Platform.

import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import celery
from celery import Celery

# Import backend modules
import Leo_analytics
import Leo_news
import Leo_data_ingestion

# --- Background Callbacks Setup (for long-running tasks) ---
# This setup is required to run long tasks like updating the database
# without freezing the web application.
# It uses Celery as a task queue.
celery_app = Celery(__name__, broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
background_callback_manager = dash.CeleryManager(celery_app)

# --- App Initialization ---
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    background_callback_manager=background_callback_manager
)
server = app.server # Expose the server variable for deployment

# --- Helper Functions for Layout ---

def create_sparkline(prices, color):
    """Creates a sparkline figure for a list of prices."""
    if not prices or len(prices) < 2:
        return go.Figure() # Return empty figure if not enough data

    fig = go.Figure(go.Scatter(
        y=prices,
        mode='lines',
        line=dict(color=color, width=2),
        fill='tozeroy',
        fillcolor=f'rgba({int(color[1:3], 16)}, {int(color[3:5], 16)}, {int(color[5:7], 16)}, 0.1)'
    ))
    fig.update_layout(
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        margin=dict(l=0, r=0, t=0, b=0),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        height=50,
    )
    return fig

def create_movers_card(title, movers_data, color):
    """Creates a card component for top winners or losers."""
    if movers_data is None:
        return dbc.Card(dbc.CardBody("Could not load data."))

    card_content = [dbc.CardHeader(title)]
    list_group_items = []
    for mover in movers_data:
        list_group_items.append(
            dbc.ListGroupItem(
                dbc.Row([
                    dbc.Col(html.B(mover['ticker']), width="auto"),
                    dbc.Col(f"{mover['change_pct']}%", width="auto", className=f"text-{'success' if mover['change_pct'] > 0 else 'danger'}"),
                    dbc.Col(dcc.Graph(figure=create_sparkline(mover['last_5_days_prices'], color)), width=4),
                ], align="center"),
                className="d-flex justify-content-between align-items-center"
            )
        )

    card_content.append(dbc.ListGroup(list_group_items, flush=True))
    return dbc.Card(card_content, className="shadow-sm")

# --- Initial Data Fetch ---
# Fetch data on startup to populate the initial view
winners_data, losers_data = Leo_analytics.get_top_movers()
news_data = Leo_news.get_news_headlines()

# --- App Layout ---
app.layout = dbc.Container([
    # Title
    html.H1("Leo", className="text-center my-4"),

    # Top Movers and News Row
    dbc.Row([
        # Column 1: Top Winners
        dbc.Col(create_movers_card("Top 10 Winners", winners_data, 'green'), md=4),

        # Column 2: Top Losers
        dbc.Col(create_movers_card("Top 10 Losers", losers_data, 'red'), md=4),

        # Column 3: News
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Latest Financial News"),
                dbc.ListGroup(
                    [dbc.ListGroupItem(html.A(item['title'], href=item['link'], target="_blank")) for item in news_data]
                    if news_data else [dbc.ListGroupItem("Could not load news.")]
                    , flush=True)
            ], className="shadow-sm"),
            md=4
        ),
    ], className="mb-4"),

    # Update Button Row
    dbc.Row([
        dbc.Col([
            html.Div(id='update-button-container', children=[
                dbc.Button("Update Database", id="update-button", color="primary", className="me-2"),
            ]),
            dcc.Interval(id='progress-interval', n_intervals=0, interval=1000),
            html.Div(id='update-status', className="mt-2")
        ], width={"size": 6, "offset": 3}, className="text-center")
    ], className="mb-4"),

], fluid=True)


# --- Callbacks ---

@dash.callback(
    Output('update-status', 'children'),
    Input('update-button', 'n_clicks'),
    background=True,
    manager=background_callback_manager,
    running=[
        (Output("update-button", "disabled"), True, False),
        (Output("update-button-container", "children"),
         dbc.Spinner(dbc.Button("Updating...", id="update-button", color="primary", disabled=True)),
         dbc.Button("Update Database", id="update-button", color="primary")),
    ],
    prevent_initial_call=True
)
def update_database_callback(n_clicks):
    """
    A background callback to update the database with the latest stock data.
    """
    if n_clicks:
        print("Update callback triggered.")
        Leo_data_ingestion.update_stock_data_to_latest()
        # The return value will be displayed in the 'update-status' Div
        return f"Update complete as of {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}."
    return ""


# --- Main Execution Block ---
if __name__ == '__main__':
    # Note: For production, use a proper WSGI server like Gunicorn or Waitress
    # Example: gunicorn Leo_dashboard:server -b 0.0.0.0:8090
    app.run(debug=True, host='0.0.0.0', port=8090)
