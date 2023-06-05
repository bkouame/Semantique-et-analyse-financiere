import os
import pandas as pd
from tqdm import tqdm
import scipy.stats as s


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


def n_add_trend(df: pd.DataFrame, nb_days):
    df["n_Moving_Average"] = df["n_close"].rolling(window=nb_days).mean()
    df.loc[df["n_close"] > df["n_Moving_Average"], 'Trend'] = 'Up'
    df.loc[df["n_close"] < df["n_Moving_Average"], 'Trend'] = 'Down'
    df.loc[df["n_close"] == df["n_Moving_Average"], 'Trend'] = 'Stable'
    return df


# data_frame = pd.read_csv("MSFT_chart.csv")
"""data_frame = pd.read_csv("^GSPC.csv")
data_frame.columns = map(lambda x: x.lower(), data_frame.columns)
data_frame['date'] = data_frame['date'].apply(lambda x: pd.to_datetime(x, utc=True).date())
df = change_granularity_to_day_2(data_frame)
df = add_pattern_columns(df, 0.1, 0.1, 0.2, 0.2, 10, 10)
df = add_trend(df, 10)
#print(len(df[df["Trend"]=="Down"]))
#print((df["is_Hammer"]==True).sum())
base_line = (df[df["Trend"].shift(-10) == "Down"]["Trend"] == "Up")
hammer = (df[(df["Trend"].shift(-10) == "Down") & (df["is_Hammer"].shift(-10) == True) ]["Trend"] == "Up")
a=s.ttest_ind(hammer, base_line).pvalue
print(len(base_line),len(hammer))
print((base_line.sum())/len(base_line))
print((hammer.sum())/len(hammer))
print(a)
print(df[df["Trend"].shift(-10) == "Down"])"""


# alter the function as to be able to get the mean, bullsih_reversal_rate, and pvalue for the t-test of list of pattern with just one loop
def bullish_reversal_rate(pattern_list, nb_days):
    dir_path = "C:/Users/Franck/Desktop/Yahoo_Sample_for_Stats"
    base_line = pd.Series()
    pattern_line = dict()
    for pattern in pattern_list:
        pattern_line[pattern] = pd.Series()
    for file_name in tqdm(os.listdir(dir_path)):
        file = os.path.join(dir_path, file_name)
        data_frame = pd.read_csv(file)
        # print(f"working with {file}")
        try:
            data_frame.columns = map(lambda x: x.lower(), data_frame.columns)
            data_frame['date'] = data_frame['date'].apply(lambda x: pd.to_datetime(x, utc=True).date())
            data_frame = change_granularity_to_day_2(data_frame)
            data_frame = add_trend_2(data_frame, nb_days,1e-3)
            data_frame = add_pattern_columns(data_frame, 0.1, 0.2, 0.2, 10, 10)
            base_line = pd.concat([base_line,(data_frame[data_frame["Trend"].shift(nb_days) == "Down"]["Trend"] == "Up")])
            #base_line = pd.concat([base_line,(data_frame["Trend"].shift(nb_days) == "Down")&(data_frame["Trend"].shift(-nb_days) == "Up")])
            for pattern in pattern_list:
                pattern_line[pattern] = pd.concat([pattern_line[pattern], (data_frame[(data_frame["Trend"].shift(nb_days) == "Down") & (data_frame[f"is_{pattern}"].shift(nb_days) == True)]["Trend"] == "Up")])
                #pattern_line = pd.concat([pattern_line,(data_frame["Trend"].shift(nb_days) == "Down")&(data_frame[f"is_{pattern}"]==True)&(data_frame["Trend"].shift(-nb_days) == "Up")])

        except:
            pass
    res = []
    for pattern, line in pattern_line.items():
        res.append([pattern,line.sum(),line.mean(),base_line.sum(),base_line.mean(),s.ttest_ind(line, base_line).pvalue])
    df = pd.DataFrame(res,columns=["pattern_name","count","pattern_mean","base_line_count","base_line_mean","pvalue_of_t_ttest"])
    df.to_excel("Pattern_reversal_stat_1800.xlsx")

bullish_reversal_rate(
    ["Hammer", "Long-Legged-Doji", "Gravestone_Doji", "Dragonfly_Doji", "Belt_Hold_Bearish", "Belt_Hold_Bullish"], 6)
