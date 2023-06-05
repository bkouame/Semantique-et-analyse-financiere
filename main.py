import pandas as pd
import zipfile
import json

ticker_file = "C:/Users/Franck/Desktop/options_tickers.txt"
option_zip = "C:/Users/Franck/Desktop/options.zip"

final_list = []

with zipfile.ZipFile(option_zip, "r") as opt_file:
    with open(ticker_file, "r") as f:
        for elt in f:
            one_ticker_list = []
            for opt_file_name in opt_file.namelist():
                if f"{opt_file_name[12:16]}\n" == elt:
                    one_ticker_list.append(opt_file_name)
            final_list.append(one_ticker_list)

print(final_list)


def concat_one_ticker_opt_json_file():
    with zipfile.ZipFile(option_zip, "r") as option_file:
        for file in final_list[0]:
            with option_file.open(file, "r") as fic:
                data = json.load(fic)
            print(data)


concat_one_ticker_opt_json_file()
print(final_list)