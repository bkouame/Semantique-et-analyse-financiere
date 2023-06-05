import os
import zipfile
import atexit
import pandas as pd
from tqdm import tqdm
import scipy.stats as s
import json
import numpy as np
import sys
import signal


def change_granularity_to_day(df: pd.DataFrame):
    df['date'] = pd.to_datetime(df['date'])
    data_f = df.groupby(pd.Grouper(key='date', freq='D')).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    return data_f.reset_index()


def change_granularity_to_day_2(df: pd.DataFrame):
    df["date"] = pd.to_datetime(df['date'])
    df["data"] = df["date"].dt.date
    data_f = df.groupby("date").agg(
        {"open": "first", "high": ["max"], "low": ["min"], "close": "last", "volume": ["sum"]})
    data_f.columns = ["open", "high", "low", "close", "volume"]
    return data_f.reset_index()


# min-max normalization
def add_normalized_columns(df: pd.DataFrame):
    df["n_open"] = (df["open"] - df["open"].min()) / (df["open"].max() - df["open"].min())
    df["n_high"] = (df["high"] - df["high"].min()) / (df["high"].max() - df["high"].min())
    df["n_low"] = (df["low"] - df["low"].min()) / (df["low"].max() - df["low"].min())
    df["n_close"] = (df["close"] - df["close"].min()) / (df["close"].max() - df["close"].min())
    return df


def add_pattern_columns(df: pd.DataFrame, epsilon_2, epsilon_3, epsilon_4, belt_hold_ratio,
                        doji_ratio):
    df["body_length"] = abs(df["close"] - df["open"])
    df["lower_shadow"] = df[["open", "close"]].min(axis=1) - df["low"]
    df["upper_shadow"] = df["high"] - df[["open", "close"]].max(axis=1)
    df["is_Hammer"] = (2 * df["body_length"] <= df["lower_shadow"]) & (
            df["lower_shadow"] <= 3 * df["body_length"]) & (
                              df["upper_shadow"] == 0)
    df["is_Belt_Hold_Bearish"] = (df["close"] < df["open"]) & (df["open"] == df["high"]) & (
            abs(df["close"] - df["low"]) <= epsilon_2) & (
                                         df["body_length"] >= belt_hold_ratio * abs(df["close"] - df["low"]))
    df["is_Belt_Hold_Bullish"] = (df["close"] > df["open"]) & (df["open"] == df["low"]) & (
            abs(df["high"] - df["close"]) <= epsilon_3) & (df["body_length"] >= belt_hold_ratio * abs(
        df["high"] - df["close"]))
    df["is_Dragonfly_Doji"] = (abs(df["open"] - df["high"]) <= epsilon_4) & (
            abs(df["close"] - df["high"]) <= epsilon_4) & (df["lower_shadow"] >= doji_ratio * df["body_length"])
    df["is_Gravestone_Doji"] = (abs(df["open"] - df["low"]) <= epsilon_4) & (
            abs(df["close"] - df["low"]) <= epsilon_4) & (df["upper_shadow"] >= doji_ratio * df["body_length"])
    df["is_Long-Legged-Doji"] = (abs(df["open"] - df["close"]) <= epsilon_4) & (
            df["lower_shadow"] >= (doji_ratio / 2) * df["body_length"]) & (
                                        df["upper_shadow"] >= (doji_ratio / 2) * df["body_length"])

    return df.reset_index()


def add_trend(df: pd.DataFrame, nb_days):
    # alter
    df["Moving_Average"] = df["close"].rolling(window=nb_days).mean()
    df.loc[df["close"] > df["Moving_Average"], 'Trend'] = 'Up'
    df.loc[df["close"] < df["Moving_Average"], 'Trend'] = 'Down'
    df.loc[df["close"] == df["Moving_Average"], 'Trend'] = 'Stable'
    return df


def add_trend_2(df: pd.DataFrame, nb_days, threshold):
    df["Moving_Average"] = df["close"].rolling(window=nb_days).mean()
    df.loc[(df["close"] - df["Moving_Average"]) / df["Moving_Average"] > threshold, 'Trend'] = 'Up'
    df.loc[(df["close"] - df["Moving_Average"]) / df["Moving_Average"] < -threshold, 'Trend'] = 'Down'
    df.loc[((df["close"] - df["Moving_Average"]) / df["Moving_Average"] <= threshold) & (
            -threshold <= (df["close"] - df["Moving_Average"]) / df["Moving_Average"]), 'Trend'] = 'Stable'
    return df


def add_hammer(df: pd.DataFrame):
    df["is_Hammer"] = (2 * df["body_length"] <= df["lower_shadow"]) & (
            df["lower_shadow"] <= 3 * df["body_length"]) & (
                              df["upper_shadow"] == 0)
    return df


def add_dragon_fly_doji(df: pd.DataFrame, epsilon, ratio):
    df["is_Dragonfly_Doji"] = (abs(df["open"] - df["high"]) <= epsilon) & (
            abs(df["close"] - df["high"]) <= epsilon) & (df["lower_shadow"] >= ratio * df["body_length"])
    return df


def add_grave_stone_doji(df: pd.DataFrame, epsilon, ratio):
    df["is_Gravestone_Doji"] = (abs(df["open"] - df["low"]) <= epsilon) & (
            abs(df["close"] - df["low"]) <= epsilon) & (df["upper_shadow"] >= ratio * df["body_length"])
    return df


def add_long_legged_doji(df: pd.DataFrame, epsilon, ratio):
    df["is_Long-Legged-Doji"] = (abs(df["open"] - df["close"]) <= epsilon) & (
            df["lower_shadow"] >= ratio * df["body_length"]) & (df["upper_shadow"] >= ratio * df["body_length"])
    return df


def add_engulfing_bullish(df: pd.DataFrame, nb_days):
    df["is_Engulfing_Bullish"] = (df.shift(nb_days)["Trend"] == "Down") & (df["close"] < df["open"]) & (
            df.shift(-1)["close"] > df.shift(-1)["open"]) & (df.shift(-1)["open"] < df["body_length"]) & (
                                         df.shift(-1)["close"] > df["body_length"])


def algebric_income(df: pd.DataFrame, paid_price, term):
    return df["close"].shift(-term) - paid_price


# alter the function as to be able to get the mean, bullish_reversal_rate, and pvalue for the t-test of list of pattern with just one loop
previous_base_line_value = []
previous_pattern_line_value = dict()
iter_number = 0


def save_variables():
    with open("last_base_line_concat.json", "w") as f:
        json.dump(previous_base_line_value, f)
    with open("last_pattern_line_concat.json", "w") as f:
        json.dump(previous_pattern_line_value, f)
    with open("iteration.txt", "w") as f:
        f.write(str(iter_number - 1))


def signal_handler(sig, frame):
    save_variables()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def bullish_reversal_rate(pattern_list, nb_days):
    global previous_pattern_line_value
    global previous_base_line_value
    global iter_number
    zip_path = "C:/Users/Franck/Desktop/charts_barchart.zip"
    base_line = []
    pattern_line = dict()
    for pattern in pattern_list:
        pattern_line[pattern] = []
    with zipfile.ZipFile(zip_path, "r") as dir_path:
        for index, file_name in tqdm(enumerate(dir_path.namelist())):
            previous_base_line_value = base_line
            previous_pattern_line_value = pattern_line
            iter_number = index
            with dir_path.open(file_name) as file:
                data_frame = pd.read_csv(file)
                if not data_frame.empty:
                    try:
                        data_frame = change_granularity_to_day_2(data_frame)
                        data_frame = add_trend_2(data_frame, nb_days, 1e-3)
                        data_frame = add_pattern_columns(data_frame, 0.1, 0.2, 0.2, 10, 10)
                        base_line = base_line + list(
                            (data_frame[data_frame["Trend"].shift(nb_days) == "Down"]["Trend"] == "Up"))
                        # base_line = pd.concat([base_line,(data_frame["Trend"].shift(nb_days) == "Down")&(data_frame["Trend"].shift(-nb_days) == "Up")])
                        for pattern in pattern_list:
                            pattern_line[pattern] = pattern_line[pattern] + list((data_frame[(data_frame["Trend"].shift(
                                nb_days) == "Down") & (data_frame[f"is_{pattern}"].shift(nb_days) == True)][
                                                                                      "Trend"] == "Up"))
                            # pattern_line[pattern] = pd.concat([pattern_line[pattern],(data_frame["Trend"].shift(nb_days) == "Down")&(data_frame[f"is_{pattern}"]==True)&(data_frame["Trend"].shift(-nb_days) == "Up")])
                    except Exception as e:
                        save_variables()
                        raise e

    res = []
    for pattern, line in pattern_line.items():
        res.append(
            [pattern, sum(line), np.mean(line), sum(base_line), np.mean(base_line),
             s.ttest_ind(line, base_line).pvalue])
    df = pd.DataFrame(res, columns=["pattern_name", "count", "pattern_mean", "base_line_count", "base_line_mean",
                                    "pvalue_of_t_ttest"])
    df.to_excel("Pattern_reversal_stats_2.xlsx")


bullish_reversal_rate(
    ["Hammer", "Long-Legged-Doji", "Gravestone_Doji", "Dragonfly_Doji", "Belt_Hold_Bearish", "Belt_Hold_Bullish"], 10)
