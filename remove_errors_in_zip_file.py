import zipfile
import pandas as pd
import json


# Adding granularity, trend setup and useful columns

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


def add_useful_columns(df: pd.DataFrame):
    df["body_length"] = abs(df["close"] - df["open"])
    df["lower_shadow"] = df[["open", "close"]].min(axis=1) - df["low"]
    df["upper_shadow"] = df["high"] - df[["open", "close"]].max(axis=1)
    df["is_Tall"] = df["body_length"] > df["body_length"].quantile(0.8)
    df["is_Short"] = df["body_length"] < df["body_length"].quantile(0.2)
    df["is_Normal_Size"] = (df["is_Tall"] == False) & (df["is_Short"] == False)
    df["center"] = (df["open"] + df["close"]) / 2


def add_trend(df: pd.DataFrame, nb_days=10, threshold=1e-2):
    df["Moving_Average"] = df["close"].rolling(window=nb_days).mean()
    df.loc[(df["close"] - df["Moving_Average"]) / df["Moving_Average"] > threshold, 'Trend'] = 'Up'
    df.loc[(df["close"] - df["Moving_Average"]) / df["Moving_Average"] < -threshold, 'Trend'] = 'Down'
    df.loc[((df["close"] - df["Moving_Average"]) / df["Moving_Average"] <= threshold) & (
            -threshold <= (df["close"] - df["Moving_Average"]) / df["Moving_Average"]), 'Trend'] = 'Stable'


# Adding pattern columns

def add_hammer(df: pd.DataFrame, epsilon=1e-2):
    df["is_Hammer"] = (2 * df["body_length"] <= df["lower_shadow"]) & (
            df["lower_shadow"] <= 3 * df["body_length"]) & (
                              df["upper_shadow"] <= epsilon) & (df["is_Short"] == True)


def add_dragon_fly_doji(df: pd.DataFrame, epsilon=1e-2, ratio=10):
    df["is_Dragon_Fly_Doji"] = (abs(df["open"] - df["high"]) <= epsilon) & (
            abs(df["close"] - df["high"]) <= epsilon) & (df["lower_shadow"] >= ratio * df["body_length"])


def add_grave_stone_doji(df: pd.DataFrame, epsilon=1e-2, ratio=10):
    df["is_Grave_Stone_Doji"] = (abs(df["open"] - df["low"]) <= epsilon) & (
            abs(df["close"] - df["low"]) <= epsilon) & (df["upper_shadow"] >= ratio * df["body_length"])


def add_long_legged_doji(df: pd.DataFrame, epsilon=1e-2, ratio=5):
    df["is_Long_Legged_Doji"] = (abs(df["open"] - df["close"]) <= epsilon) & (
            df["lower_shadow"] >= ratio * df["body_length"]) & (df["upper_shadow"] >= ratio * df["body_length"])


def add_engulfing_bullish(df: pd.DataFrame):
    df["is_Engulfing_Bullish"] = (df["Trend"] == "Down") & (df["close"] < df["open"]) & (
            df.shift(-1)["close"] > df.shift(-1)["open"]) & (
                                         df.shift(-1)["open"] < df[["open", "close"]].min(axis=1)) & (
                                         df.shift(-1)["close"] > df[["open", "close"]].max(axis=1))


def add_closing_black_marubozu(df: pd.DataFrame, epsilon=1e-2):
    df["is_Closing_Black_Marubozu"] = (df["is_Tall"] == True) & (df["close"] < df["open"]) & (
            df["lower_shadow"] <= epsilon)


def add_closing_white_marubozu(df: pd.DataFrame, epsilon=1e-2):
    df["is_Closing_White_Marubozu"] = (df["is_Tall"] == True) & (df["close"] > df["open"]) & (
            df["upper_shadow"] <= epsilon)


def add_opening_black_marubozu(df: pd.DataFrame, epsilon=1e-2):
    df["is_Opening_Black_Marubozu"] = (df["is_Tall"] == True) & (df["close"] < df["open"]) & (
            df["upper_shadow"] <= epsilon)


def add_opening_white_marubozu(df: pd.DataFrame, epsilon=1e-2):
    df["is_Opening_White_Marubozu"] = (df["is_Tall"] == True) & (df["close"] > df["open"]) & (
            df["upper_shadow"] <= epsilon)


def add_white_marubozu(df: pd.DataFrame, epsilon=1e-2):
    df["is_White_Marubozu"] = (df["is_Tall"] == True) & (df["close"] > df["open"]) & (
            df["upper_shadow"] <= epsilon) & (df["lower_shadow"] <= epsilon)


def add_rickshaw_man(df: pd.DataFrame, epsilon=1e-2, ratio=10):
    df["is_Rickshaw_Man"] = (abs(df["close"] - df["open"]) <= epsilon) & (
            abs(df["body_length"] - df["center"]) <= epsilon) & (df["upper_shadow"] >= ratio * df["body_length"]) & (
                                    df["lower_shadow"] >= ratio * df["body_length"])


def add_shooting_star_one_candle(df: pd.DataFrame, epsilon=1e-2):
    df["is_Shooting_Star_One_Candle"] = (df["Trend"] == "Up") & (df["upper_shadow"] >= 2 * df["body_length"]) & (
            df["is_Short"] == True) & (df["lower_shadow"] <= epsilon)


def add_black_spinning_top(df: pd.DataFrame):
    df["is_Black_Spinning_Top"] = (df["open"] > df["close"]) & (df["is_Short"] == True) & (
            df["upper_shadow"] > df["body_length"]) & (df["lower_shadow"] > df["body_length"])


def add_white_spinning_top(df: pd.DataFrame):
    df["is_White_Spinning_Top"] = (df["open"] < df["close"]) & (df["is_Short"] == True) & (
            df["upper_shadow"] > df["body_length"]) & (df["lower_shadow"] > df["body_length"])


def add_takuri_line(df: pd.DataFrame, epsilon=1e-2):
    df["is_Takuri_Line"] = (df["Trend"] == "Down") & (df["upper_shadow"] <= epsilon) & (
            df["lower_shadow"] >= 3 * df["body_length"])


def add_bullish_meeting_lines(df: pd.DataFrame, epsilon=1e-2):
    df["is_Bullish_Meeting_Lines"] = (df["Trend"] == "Down") & (df["open"] > df["close"]) & (df["is_Tall"] == True) & (
            df.shift(-1)["open"] < df.shift(-1)["close"]) & (abs(df.shift(-1)["close"] - df["close"]) <= epsilon)


def add_on_neck(df: pd.DataFrame, epsilon=1e-2):
    df["is_On_Neck"] = (df["Trend"] == "Down") & (df["open"] > df["close"]) & (df["is_Tall"] == True) & (
            df.shift(-1)["open"] < df.shift(-1)["close"]) & (abs(df.shift(-1)["close"] - df["low"]) <= epsilon)


def add_piercing_pattern(df: pd.DataFrame):
    df["is_Piercing_Pattern"] = (df["Trend"] == "Down") & (df["open"] > df["close"]) & (
            df.shift(-1)["open"] < df.shift(-1)["close"]) & (df.shift(-1)["open"] < df["low"]) & (
                                        df.shift(-1)["close"] >= df["center"]) & (df.shift(-1)["close"] <= df["open"])


def add_bearish_separating_lines(df: pd.DataFrame, epsilon=1e-2):
    df["is_Bearish_Separating_Lines"] = (df["Trend"] == "Down") & (df["is_Tall"] == True) & (
            df["close"] > df["open"]) & (
                                                df.shift(-1)["close"] < df.shift(-1)["open"]) & (
                                                df.shift(-1)["is_Tall"] == True) & (
                                                abs(df.shift(-1)["open"] - df["open"]) <= epsilon)


def add_bullish_separating_lines(df: pd.DataFrame, epsilon=1e-2):
    df["is_Bullish_Separating_Lines"] = (df["Trend"] == "Up") & (df["is_Tall"] == True) & (df["close"] < df["open"]) & (
            df.shift(-1)["close"] > df.shift(-1)["open"]) & (df.shift(-1)["is_Tall"] == True) & (
                                                abs(df.shift(-1)["open"] - df["open"]) <= epsilon)


def add_shooting_star_two_candles(df: pd.DataFrame, epsilon=1e-2):
    df["is_Shooting_Star_Two_Candles"] = (df["Trend"] == "Up") & (df["open"] < df["close"]) & (
            df.shift(-1)["is_Short"] == True) & (df.shift(-1)["upper_shadow"] >= 3 * df.shift(-1)["body_length"]) & (
                                                 df.shift(-1)["lower_shadow"] <= epsilon) & (
                                                 df.shift(-1)["open"] >= df["close"])


def add_thrusting(df: pd.DataFrame):
    df["is_Thrusting"] = (df["Trend"] == "Down") & (df["open"] > df["close"]) & (
            df.shift(-1)["close"] > df.shift(-1)["open"]) & (df.shift(-1)["open"] < df["low"]) & (
                                 df.shift(-1)["close"] <= df["center"])


def add_tweezers_bottom(df: pd.DataFrame, epsilon=1e-2):
    df["is_Tweezers_Bottom"] = (df["Trend"] == "Down") & (abs(df["low"] - df.shift(-1)["low"]) <= epsilon)


def add_tweezers_top(df: pd.DataFrame, epsilon=1e-2):
    df["is_Tweezers_Top"] = (df["Trend"] == "Down") & (abs(df["high"] - df.shift(-1)["high"]) <= epsilon)


# Adding all pattern

def add_all_pattern(df: pd.DataFrame):
    add_hammer(df)
    add_dragon_fly_doji(df)
    add_grave_stone_doji(df)
    add_long_legged_doji(df)
    add_engulfing_bullish(df)
    add_closing_black_marubozu(df)
    add_closing_white_marubozu(df)
    add_opening_black_marubozu(df)
    add_opening_white_marubozu(df)
    add_white_marubozu(df)
    add_rickshaw_man(df)
    add_shooting_star_one_candle(df)
    add_black_spinning_top(df)
    add_white_spinning_top(df)
    add_takuri_line(df)
    add_bullish_meeting_lines(df)
    add_on_neck(df)
    add_piercing_pattern(df)
    add_bearish_separating_lines(df)
    add_bullish_separating_lines(df)
    add_shooting_star_two_candles(df)
    add_thrusting(df)
    add_tweezers_bottom(df)
    add_tweezers_top(df)


Trend_list = ["Up", "Down", "Stable"]
Pattern_list = ["Hammer",
                "Dragon_Fly_Doji",
                "Grave_Stone_Doji",
                "Long_Legged_Doji",
                "Engulfing_Bullish",
                "Closing_Black_Marubozu",
                "Closing_White_Marubozu",
                "Opening_Black_Marubozu",
                "Opening_White_Marubozu",
                "White_Marubozu",
                "Rickshaw_Man",
                "Shooting_Star_One_Candle",
                "Black_Spinning_Top",
                "White_Spinning_Top",
                "Takuri_Line",
                "Bullish_Meeting_Lines",
                "On_Neck",
                "Piercing_Pattern",
                "Bearish_Separating_Lines",
                "Bullish_Separating_Lines",
                "Shooting_Star_Two_Candles",
                "Thrusting",
                "Tweezers_Bottom",
                "Tweezers_Top"]
zip_file = "C:/Users/Franck/Desktop/charts_barchart.zip"


# zip_file = "/mnt/c/Users/Franck/Desktop/charts_barchart.zip"

# zip_file = "/mnt/c/Users/Franck/Desktop/Bar_Chat_Sample.zip"


def remove_files(start_index, end_index, zip_path=zip_file):
    files_to_remove = []
    with zipfile.ZipFile(zip_path, "r") as dir_path:
        for index, file_name in enumerate(dir_path.filelist):
            if start_index <= index <= end_index:
                if file_name.file_size > 3072:
                    with dir_path.open(file_name) as file:
                        data_frame = pd.read_csv(file)
                        try:
                            print(index)
                            if data_frame.columns[0] == 'date' and data_frame.columns[1] == 'X' and data_frame.columns[2] == 'open' and data_frame.columns[3] == 'high' and data_frame.columns[4] == 'low' and data_frame.columns[5] == 'close' and data_frame.columns[6] == 'volume':
                                pass
                            else:
                                files_to_remove.append(file_name.filename)
                        except:
                            files_to_remove.append(file_name.filename)
    with open("files_raising_errors.txt", "a+") as f:
        for file_name in files_to_remove:
            print(file_name)
            f.write(f"{file_name}'\n'")
    print("finished !")


remove_files(0, 145524)
