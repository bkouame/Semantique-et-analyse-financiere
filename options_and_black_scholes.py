import json
import pandas as pd
import zipfile
import numpy as np
from scipy.stats import norm
import plotly.graph_objs as go

# option_zip = "C:/Users/Franck/Desktop/dash_bs_option.zip"
# stock_zip = "C:/Users/Franck/Desktop/stock_data.zip"
risk_free_file = "C:/Users/Franck/Desktop/USTREASURY-BILLRATES.csv"

rf_df = pd.read_csv(risk_free_file)
rf_df["Date"] = pd.to_datetime(rf_df["Date"]).dt.tz_localize(None)


# print(rf_df)
"""def load_ticker_option_data(ticker):
    with zipfile.ZipFile(option_zip, "r") as dir_path:
        for file_name in dir_path.namelist():
            if file_name[0:4] == ticker:
                try:
                    with dir_path.open(file_name, "r") as file:
                        data = json.load(file)
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
                except Exception as e:
                    raise e"""


def black_scholes_option_price(S, K, r, sigma, T, option_type: str):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == "Call":
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


"""def load_ticker_stock_data(ticker):
    with zipfile.ZipFile(stock_zip, "r") as stock_dir:
        for file_name in stock_dir.namelist():
            if file_name == f"{ticker}_chart.csv":
                with stock_dir.open(file_name, "r") as f:
                    st_df = pd.read_csv(f)
                    return st_df"""


"""def count_nb_data_point_in_a_year(df: pd.DataFrame):
    df["date"] = pd.to_datetime(df["date"])
    data_f = df.groupby(pd.Grouper(key='date', freq='H')).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"})
    data_f.columns = data_f.columns = ["open", "high", "low", "close"]
    data_f = data_f.dropna().reset_index()
    data_f2 = data_f.groupby(pd.Grouper(key='date', freq='Y')).agg({"open": "count"})
    data_f2.columns = ["nb points"]
    data_f2 = data_f2.dropna().reset_index()
    return data_f2"""


def change_granularity(stock_data: pd.DataFrame, key):
    stock_data[key] = pd.to_datetime(stock_data[key])
    data_f = stock_data.groupby(pd.Grouper(key=key, freq='H')).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"})
    data_f.columns = ["open", "high", "low", "close"]
    data_f = data_f.dropna().reset_index()
    return data_f


def select_stock_data_range(stock_data: pd.DataFrame, option_data_start, option_data_end):
    data_f = change_granularity(stock_data, "date")
    return data_f[(data_f["date"] >= option_data_start) & (data_f["date"] <= option_data_end)]


def add_black_scholes_options_prices(df: pd.DataFrame, opt_data_start, opt_data_end, end_date, option_type,
                                     strike_price):
    df = select_stock_data_range(df, opt_data_start, opt_data_end)
    df["log(close/close.shift)"] = np.log(df['close'] / df['close'].shift())
    df["volatility"] = df["log(close/close.shift)"].expanding().std() * np.sqrt(1700)
    df["volatility"] = df["volatility"].fillna(df["volatility"].mean())
    df["int_date"] = pd.to_datetime(df["date"].dt.date)
    df = df.merge(rf_df[["Date", "52 Wk Coupon Equiv"]], how='left', left_on='int_date', right_on='Date')
    df.drop(["int_date", "Date"], axis=1, inplace=True)
    df["black_scholes_option_price"] = black_scholes_option_price(df["close"], strike_price,
                                                                  df["52 Wk Coupon Equiv"] / 100,
                                                                  df["volatility"],
                                                                  (end_date - df["date"]).dt.total_seconds() / 31536000,
                                                                  option_type)
    return df


"""def show_simulation(ticker, fig: go.Figure, title: str):
    df_opt, end, op_type, strike, opt_dt_start, opt_dt_end = load_ticker_option_data(ticker)
    df_stock = load_ticker_stock_data(ticker)
    df_stock = add_black_scholes_options_prices(df_stock, opt_dt_start, opt_dt_end, end, op_type, strike)
    df_opt = change_granularity(df_opt, "startdatetime")
    fig.add_trace(go.Scatter(x=df_opt["startdatetime"], y=df_opt["close"], name="Real Price"))
    fig.add_trace(go.Scatter(x=df_stock["date"], y=df_stock["black_scholes_option_price"], name="Black-Scholes Price"))
    fig.update_layout(title=title,
                      xaxis_title="Date",
                      yaxis_title="Price")"""