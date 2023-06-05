from project_functions import *
import zipfile

# zip_file = "C:/Users/Franck/Desktop/charts_barchart.zip"


# zip_file = "/mnt/c/Users/Franck/Desktop/charts_barchart.zip"


zip_file = "C:/Users/Franck/Desktop/Bar_Chat_Sample.zip"


# function to get reversal stats of a whole zipfile
def bullish_reversal_rate_of_a_zip(start_index, end_index, i, delta=3, nb_days=10, pattern_list=None,
                                   zip_path=zip_file):
    if pattern_list is None:
        pattern_list = Pattern_list
    pattern_other_info = dict()
    for pattern in pattern_list:
        pattern_other_info[pattern] = dict()
        for feature in feature_list:
            pattern_other_info[pattern][feature] = 0
    number_of_candle_line = 0
    base_line = []
    pattern_line = dict()
    for pattern in pattern_list:
        pattern_line[pattern] = []
    with zipfile.ZipFile(zip_path, "r") as dir_path:
        for index, file_name in enumerate(dir_path.filelist):
            if start_index <= index <= end_index:
                if file_name.file_size > 3072:
                    with dir_path.open(file_name) as file:
                        data_frame = pd.read_csv(file)
                        try:
                            data_frame = change_granularity_to_day_2(data_frame)
                            add_trend(data_frame, nb_days, 1e-3)
                            add_useful_columns(data_frame)
                            add_all_pattern(data_frame)
                            number_of_candle_line += len(data_frame) - 1
                            base_line = base_line + list(
                                (data_frame[data_frame["Trend"].shift(nb_days) == "Down"]["Trend"] == "Up"))
                            for pattern in pattern_list:
                                pattern_line[pattern] = pattern_line[pattern] + list(
                                    (data_frame[(data_frame["Trend"].shift(
                                        nb_days) == "Down") & (data_frame[f"is_{pattern}"].shift(nb_days) == True)][
                                         "Trend"] == "Up"))
                                pattern_other_info[pattern]["pattern_count"] += int(
                                    data_frame[data_frame[f"is_{pattern}"] == True].count()[0])
                                pattern_other_info[pattern]["up_trend_after_pattern_count"] += int(data_frame[
                                                                                                       (
                                                                                                               data_frame.shift(
                                                                                                                   -delta)[
                                                                                                                   "Trend"] == "Up") & (
                                                                                                               data_frame[
                                                                                                                   f"is_{pattern}"] == True)].count()[
                                                                                                       0])
                                pattern_other_info[pattern]["down_trend_after_pattern_count"] += int(data_frame[
                                                                                                         (
                                                                                                                 data_frame.shift(
                                                                                                                     -delta)[
                                                                                                                     "Trend"] == "Down") & (
                                                                                                                 data_frame[
                                                                                                                     f"is_{pattern}"] == True)].count()[
                                                                                                         0])
                                pattern_other_info[pattern]["stable_trend_after_pattern_count"] += int(data_frame[
                                                                                                           (
                                                                                                                   data_frame.shift(
                                                                                                                       -delta)[
                                                                                                                       "Trend"] == "Stable") & (
                                                                                                                   data_frame[
                                                                                                                       f"is_{pattern}"] == True)].count()[
                                                                                                           0])
                                pattern_other_info[pattern]["up_after_up"] += int(data_frame[
                                                                                      (data_frame.shift(-delta)[
                                                                                           "Trend"] == "Up") & (
                                                                                              data_frame[
                                                                                                  f"is_{pattern}"] == True) & (
                                                                                              data_frame[
                                                                                                  "Trend"] == "Up")].count()[
                                                                                      0])
                                pattern_other_info[pattern]["up_after_down"] += int(data_frame[
                                                                                        (data_frame.shift(-delta)[
                                                                                             "Trend"] == "Up") & (
                                                                                                data_frame[
                                                                                                    f"is_{pattern}"] == True) & (
                                                                                                data_frame[
                                                                                                    "Trend"] == "Down")].count()[
                                                                                        0])
                                pattern_other_info[pattern]["up_after_stable"] += int(data_frame[
                                                                                          (data_frame.shift(-delta)[
                                                                                               "Trend"] == "Up") & (
                                                                                                  data_frame[
                                                                                                      f"is_{pattern}"] == True) & (
                                                                                                  data_frame[
                                                                                                      "Trend"] == "Stable")].count()[
                                                                                          0])
                                pattern_other_info[pattern]["down_after_up"] += int(data_frame[
                                                                                        (data_frame.shift(-delta)[
                                                                                             "Trend"] == "Down") & (
                                                                                                data_frame[
                                                                                                    f"is_{pattern}"] == True) & (
                                                                                                data_frame[
                                                                                                    "Trend"] == "Up")].count()[
                                                                                        0])
                                pattern_other_info[pattern]["down_after_down"] += int(data_frame[
                                                                                          (data_frame.shift(-delta)[
                                                                                               "Trend"] == "Down") & (
                                                                                                  data_frame[
                                                                                                      f"is_{pattern}"] == True) & (
                                                                                                  data_frame[
                                                                                                      "Trend"] == "Down")].count()[
                                                                                          0])
                                pattern_other_info[pattern]["down_after_stable"] += int(data_frame[
                                                                                            (data_frame.shift(-delta)[
                                                                                                 "Trend"] == "Down") & (
                                                                                                    data_frame[
                                                                                                        f"is_{pattern}"] == True) & (
                                                                                                    data_frame[
                                                                                                        "Trend"] == "Stable")].count()[
                                                                                            0])
                                pattern_other_info[pattern]["stable_after_up"] += int(data_frame[
                                                                                          (data_frame.shift(-delta)[
                                                                                               "Trend"] == "Stable") & (
                                                                                                  data_frame[
                                                                                                      f"is_{pattern}"] == True) & (
                                                                                                  data_frame[
                                                                                                      "Trend"] == "Up")].count()[
                                                                                          0])
                                pattern_other_info[pattern]["stable_after_down"] += int(data_frame[
                                                                                            (data_frame.shift(-delta)[
                                                                                                 "Trend"] == "Stable") & (
                                                                                                    data_frame[
                                                                                                        f"is_{pattern}"] == True) & (
                                                                                                    data_frame[
                                                                                                        "Trend"] == "Down")].count()[
                                                                                            0])
                                pattern_other_info[pattern]["stable_after_stable"] += int(data_frame[
                                                                                              (data_frame.shift(-delta)[
                                                                                                   "Trend"] == "Stable") & (
                                                                                                      data_frame[
                                                                                                          f"is_{pattern}"] == True) & (
                                                                                                      data_frame[
                                                                                                          "Trend"] == "Stable")].count()[
                                                                                              0])
                                pattern_other_info[pattern]["up_and_pattern"] += int(data_frame[
                                                                                         (data_frame[
                                                                                              f"is_{pattern}"] == True) & (
                                                                                                 data_frame[
                                                                                                     "Trend"] == "Up")].count()[
                                                                                         0])
                                pattern_other_info[pattern]["down_and_pattern"] += int(data_frame[
                                                                                           (data_frame[
                                                                                                f"is_{pattern}"] == True) & (
                                                                                                   data_frame[
                                                                                                       "Trend"] == "Down")].count()[
                                                                                           0])
                                pattern_other_info[pattern]["stable_and_pattern"] += int(data_frame[
                                                                                             (data_frame[
                                                                                                  f"is_{pattern}"] == True) & (
                                                                                                     data_frame[
                                                                                                         "Trend"] == "Stable")].count()[
                                                                                             0])
                        except:
                            continue

    with open(f"/mnt/c/Users/Franck/PycharmProjects/base_line_{i}.json", "w") as f:
        json.dump(base_line, f)
    with open(f"/mnt/c/Users/Franck/PycharmProjects/pattern_line_{i}.json", "w") as f:
        json.dump(pattern_line, f)
    with open(f"/mnt/c/Users/Franck/PycharmProjects/pattern_other_info_{i}.json", "w") as f:
        json.dump(pattern_other_info, f)
    with open(f"/mnt/c/Users/Franck/PycharmProjects/candle_count_{i}.txt", "w") as f:
        f.write(f"{number_of_candle_line}")
