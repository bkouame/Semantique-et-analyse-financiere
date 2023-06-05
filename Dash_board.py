import datetime
from dash import Dash, dcc, html, Output, Input, dash_table
import pandas as pd
import plotly.graph_objs as go
import zipfile
from io import StringIO
import project_functions as pf
from scipy.signal import argrelextrema
import numpy as np
import options_and_black_scholes as obs
import json

granularities = {
    'hour': 'H',
    'day': 'D',
    'week': 'W',
    'month': 'M',
    'Year': 'Y'
}

lines_list = ["Moving Average", "Resistance", "Support", "Breakout"]

with open("ticker_and_corporation_file.json", "r") as f:
    ticker_and_corp_data = json.load(f)


def reset_index(data_f: pd.DataFrame):
    return data_f.reset_index()


def show_moving_average(data_f: pd.DataFrame, figure: go.Figure, name: str):
    figure.add_trace(go.Line(x=data_f["date"], y=data_f["Moving_Average"], line=dict(color='orange'), name=name))


def show_resistance(data_f: pd.DataFrame, figure: go.Figure, order):
    data_f = data_f.reset_index()
    local_max_index = argrelextrema(data_f['high'].values, np.greater_equal, order=order)[0]
    resistance_shapes = []
    for i in local_max_index:
        j = 0
        while i - j - 1 > 0 and j <= order and i + j < len(data_f) - 1:
            resistance_shapes.append(
                dict(type='line', x0=data_f['date'][i + j], y0=data_f['high'][i], x1=data_f['date'][i + j + 1],
                     y1=data_f['high'][i],
                     line=dict(color='blue', width=1)))
            resistance_shapes.append(
                dict(type='line', x0=data_f['date'][i - j], y0=data_f['high'][i], x1=data_f['date'][i - j - 1],
                     y1=data_f['high'][i],
                     line=dict(color='blue', width=1)))
            j += 1
    figure.update_layout(shapes=resistance_shapes)


def show_support(data_f: pd.DataFrame, figure: go.Figure, order):
    data_f = data_f.reset_index()
    local_min_index = argrelextrema(data_f['low'].values, np.less_equal, order=order)[0]
    support_shapes = []
    for i in local_min_index:
        h = 0
        while i - h - 1 > 0 and h <= order and i + h < len(data_f) - 1:
            support_shapes.append(
                dict(type='line', x0=data_f['date'][i + h], y0=data_f['low'][i], x1=data_f['date'][i + h + 1],
                     y1=data_f['low'][i],
                     line=dict(color='red', width=1)))
            support_shapes.append(
                dict(type='line', x0=data_f['date'][i - h], y0=data_f['low'][i], x1=data_f['date'][i - h - 1],
                     y1=data_f['low'][i],
                     line=dict(color='red', width=1)))
            h += 1
    figure.update_layout(shapes=support_shapes)


def show_break_out(data_f: pd.DataFrame, figure: go.Figure, name: str):
    figure.add_trace(go.Line(x=data_f["date"], y=data_f["EWM"], name=name))
    data_f["bullish_breakout"] = data_f["EWM"] > data_f["EWM"].quantile(0.9)
    data_f["bearish_breakout"] = data_f["EWM"] < data_f["EWM"].quantile(0.1)
    for i in data_f.index:
        if data_f["bullish_breakout"][i]:
            figure.add_vrect(x0=data_f["date"][i], x1=(data_f["date"][i] + datetime.timedelta(days=1)),
                             fillcolor="green", opacity=0.25,
                             line_width=0)
        if data_f["bearish_breakout"][i]:
            figure.add_vrect(x0=data_f["date"][i], x1=(data_f["date"][i] + datetime.timedelta(days=1)), fillcolor="red",
                             opacity=0.25,
                             line_width=0)
    figure.update_layout()


def add_lines(data_f: pd.DataFrame, figure: go.Figure, line, nb_days, order, name1, name2):
    data_f["Moving_Average"] = data_f["close"].rolling(window=nb_days, min_periods=1).mean()
    data_f["EWM"] = data_f["close"].ewm(span=nb_days).mean()
    if line == lines_list[0]:
        show_moving_average(data_f, figure, name1)
    if line == lines_list[1]:
        show_resistance(data_f, figure, order)
    if line == lines_list[2]:
        show_support(data_f, figure, order)
    if line == lines_list[3]:
        show_break_out(data_f, figure, name2)


def show_pattern(data_f: pd.DataFrame, pattern):
    if pattern is None:
        return data_f
    return data_f[data_f[f"is_{pattern}"] == True]


def add_all_pattern(data_f: pd.DataFrame):
    pf.add_useful_columns(data_f)
    pf.add_trend(data_f)
    pf.add_all_pattern(data_f)
    pf.add_pattern_names_column(data_f)
    return data_f.dropna()


def change_granularity(data_f: pd.DataFrame, granularity):
    data_f = reset_index(data_f)
    data_f['date'] = pd.to_datetime(data_f['date'])
    data_f = data_f.groupby(pd.Grouper(key='date', freq=granularities[granularity])).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"})
    data_f.columns = ["open", "high", "low", "close"]
    return data_f.dropna().reset_index()


# Load the data
stock_zip_file = zipfile.ZipFile("C:/Users/Franck/Desktop/stock_data.zip")
option_zip_file = zipfile.ZipFile("C:/Users/Franck/Desktop/dash_bs_option.zip")
assets = []
for file_name in stock_zip_file.namelist():
    assets.append(file_name.replace("_chart.csv", ""))


def read_stock_file(asset_name):
    data_f = pd.read_csv(StringIO(stock_zip_file.read(asset_name + "_chart.csv").decode()), index_col=0)
    data_f = reset_index(data_f)
    return data_f


def read_option_file(asset_name):
    data = json.load(StringIO(option_zip_file.read(asset_name + "_opt.json").decode()))
    end_date = pd.to_datetime(data["results"]["intraday"][0]["end"])
    option_type = data['results']['intraday'][0]['equityinfo']["longname"].split()[-1]
    strike_price = float(data['results']['intraday'][0]['equityinfo']["longname"].split()[-2])
    option_candle = pd.DataFrame(data["results"]["intraday"][0]["interval"])
    option_candle["startdatetime"] = pd.to_datetime(option_candle["startdatetime"], utc=True)
    option_candle["startdatetime"] = option_candle["startdatetime"].dt.tz_localize(None)
    option_candle = option_candle.sort_values("startdatetime", ascending=True).reset_index()
    candle_start = option_candle["startdatetime"][0]
    candle_end = option_candle["startdatetime"][len(option_candle) - 1]
    return option_candle, end_date, option_type, strike_price, candle_start, candle_end


# Create the layout

app = Dash(__name__)
app.title = "Sémantique et analyse financière"
app.layout = html.Div([
    html.H2("Bienvenue dans le dashboard de notre projet cassiopée"),
    html.Div([
        html.Label('Asset:'),
        dcc.Dropdown(
            id='asset-dropdown',
            options=[{'label': asset_name, 'value': asset_name} for asset_name in assets],
            value=list(assets[0]),
            style={'width': '300px'}
        ),
    ], style={'margin-bottom': '20px'}),
    html.Div([
        html.Label('Granularity:'),
        dcc.Dropdown(
            id='granularity-dropdown',
            options=[{'label': g.capitalize(), 'value': g} for g in granularities.keys()],
            value='day',
            style={'width': '300px'}
        ),
    ], style={'margin-bottom': '20px'}),
    html.Div([
        html.Label('Add line:'),
        dcc.Checklist(
            id='lines',
            options=[{'label': line, 'value': line} for line in lines_list],
            value=[lines_list[0]],
            style={'width': '300px'}
        ),
    ], style={'margin-bottom': '20px'}),
    html.Div([
        html.Label('Filter pattern:'),
        dcc.Dropdown(
            id='patterns',
            options=[{'label': pattern, 'value': pattern} for pattern in pf.Pattern_list],
            value=None,
            style={'width': '300px'}
        ),
    ], style={'margin-bottom': '20px'}),
    html.Div([
        html.Label(id="days-label"),
        dcc.Input(
            id='days',
            type='number',
            value=10,
        )], style={'position': 'absolute', 'top': 120, 'right': 0}, ),
    html.Div([
        html.Label(id="delta-label"),
        dcc.Input(
            id='delta',
            type='number',
            value=3,
        )], style={'position': 'absolute', 'top': 150, 'right': 0}),
    html.Div(id='candlestick-chart'),
    html.Div(html.H3(id="granularity-table-label")),
    html.Br(),
    html.Div(id='table-container'),
    html.H2("Comparison between Black-Scholes prices and real prices"),
    html.Div(id='simulation-chart'),
])


# Define the callbacks
@app.callback(
    Output('candlestick-chart', 'children'),
    Output('days-label', 'children'),
    Input('asset-dropdown', 'value'),
    Input('granularity-dropdown', 'value'),
    Input('lines', 'value'),
    Input('days', 'value'),
    Input('patterns', 'value'),
)
def update_candlestick_chart(selected_asset, selected_granularity, selected_line, entered_number, selected_pattern):
    label = f"Number of {selected_granularity} to compute the trend: "
    name1 = f"{entered_number} {selected_granularity} Moving average"
    name2 = f"{entered_number} {selected_granularity} EWM"
    if (selected_asset is None) or (selected_granularity is None) or (entered_number is None):
        return None, label
    selected_asset = "".join(selected_asset)
    dff = read_stock_file(selected_asset)
    dff = change_granularity(dff, selected_granularity)
    dff = add_all_pattern(dff)
    if selected_pattern is not None:
        selected_pattern = "".join(selected_pattern)
        candlestick1 = go.Candlestick(x=dff[dff[f"is_{selected_pattern}"] == False]['date'],
                                      open=dff[dff[f"is_{selected_pattern}"] == False]['open'],
                                      high=dff[dff[f"is_{selected_pattern}"] == False]['high'],
                                      low=dff[dff[f"is_{selected_pattern}"] == False]['low'],
                                      close=dff[dff[f"is_{selected_pattern}"] == False]['close'],
                                      hovertext=[f"Pattern Name: {pattern}" for pattern in
                                                 dff[dff[f"is_{selected_pattern}"] == False]["pattern_name"]],
                                      opacity=0.4)
        candlestick2 = go.Candlestick(x=dff[dff[f"is_{selected_pattern}"] == True]['date'],
                                      open=dff[dff[f"is_{selected_pattern}"] == True]['open'],
                                      high=dff[dff[f"is_{selected_pattern}"] == True]['high'],
                                      low=dff[dff[f"is_{selected_pattern}"] == True]['low'],
                                      close=dff[dff[f"is_{selected_pattern}"] == True]['close'],
                                      hovertext=[f"Pattern Name: {pattern}" for pattern in
                                                 dff[dff[f"is_{selected_pattern}"] == True]["pattern_name"]],
                                      opacity=1)
        fig = go.Figure(data=[candlestick1] + [candlestick2],
                        layout=go.Layout(title=ticker_and_corp_data[selected_asset], height=800, width=1500))

    else:
        candlestick = go.Candlestick(x=dff['date'], open=dff['open'], high=dff['high'],
                                     low=dff['low'], close=dff['close'],
                                     hovertext=[f"Pattern Name: {pattern}" for pattern in dff["pattern_name"]],
                                     opacity=1)
        fig = go.Figure(data=[candlestick],
                        layout=go.Layout(title=ticker_and_corp_data[selected_asset], height=800, width=1500))

    for line in selected_line:
        add_lines(dff, fig, line, entered_number, entered_number, name1, name2)
    return dcc.Graph(figure=fig), label


@app.callback(
    Output('table-container', 'children'),
    Output('granularity-table-label', 'children'),
    Output('delta-label', 'children'),
    Input('asset-dropdown', 'value'),
    Input('granularity-dropdown', 'value'),
    Input('days', 'value'),
    Input('delta', 'value')
)
def update_table(selected_asset, selected_granularity, entered_number, delta_value):
    table_label = f"This is the statistics table of the patterns with {selected_granularity} as granularity"
    delta_label = f"Delta value in {selected_granularity}: "
    selected_asset = "".join(selected_asset)
    if (delta_value is None) or (selected_granularity is None):
        return None, table_label, delta_label
    dff = read_stock_file(selected_asset)
    dff = change_granularity(dff, selected_granularity)
    dff = add_all_pattern(dff)
    data_frame = pd.DataFrame()
    data_frame["pattern"] = pf.Pattern_list
    data_frame["count"] = data_frame["pattern"].apply(lambda x: len(dff[dff[f"is_{x}"] == True]))
    data_frame["frequency in %"] = data_frame["pattern"].apply(
        lambda x: len(dff[dff[f"is_{x}"] == True]) * 100 / len(dff) if len(dff) != 0 else 0)
    data_frame["bullish reversal rate in %"] = data_frame["pattern"].apply(
        lambda x: pf.bullish_reversal_rate(dff, x, entered_number))
    data_frame["bearish reversal rate in %"] = data_frame["pattern"].apply(
        lambda x: pf.bearish_reversal_rate(dff, x, entered_number))
    data_frame[f"up trend after {delta_value} {selected_granularity} probability"] = data_frame["pattern"].apply(
        lambda x: pf.up_trend_after_pattern_rate(dff, x, delta_value))
    data_frame[f"down trend after {delta_value} {selected_granularity} probability"] = data_frame["pattern"].apply(
        lambda x: pf.down_trend_after_pattern_rate(dff, x, delta_value))
    data_frame[f"stable trend after {delta_value} {selected_granularity} probability"] = data_frame["pattern"].apply(
        lambda x: pf.stable_trend_after_pattern_rate(dff, x, delta_value))
    table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in data_frame.columns],
        data=data_frame.to_dict("records"),
        page_size=5,
        style_cell={'minWidth': '100px', 'textAlign': 'center'}
    )
    return table, table_label, delta_label


@app.callback(
    Output('simulation-chart', 'children'),
    Input('asset-dropdown', 'value'),
)
def update_simulation_chart(selected_asset):
    selected_asset = "".join(selected_asset)
    if selected_asset is None:
        return None
    df_opt, end, op_type, strike, opt_dt_start, opt_dt_end = read_option_file(selected_asset)
    df_stock = read_stock_file(selected_asset)
    df_stock = obs.add_black_scholes_options_prices(df_stock, opt_dt_start, opt_dt_end, end, op_type, strike)
    df_opt = obs.change_granularity(df_opt, "startdatetime")
    bs_fig = go.Figure()
    bs_fig.add_trace(go.Scatter(x=df_opt["startdatetime"], y=df_opt["close"], name="Real Price"))
    bs_fig.add_trace(
        go.Scatter(x=df_stock["date"], y=df_stock["black_scholes_option_price"], name="Black-Scholes Price"))
    bs_fig.update_layout(title=f"{ticker_and_corp_data[selected_asset]} option prices",
                         xaxis_title="Date",
                         yaxis_title="Price")
    # obs.show_simulation(selected_asset, bs_fig, f"{ticker_and_corp_data[selected_asset]} option prices")
    return dcc.Graph(figure=bs_fig)


if __name__ == '__main__':
    # Run the app
    app.run_server(port=8050, debug=False)