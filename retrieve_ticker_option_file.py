import numpy as np
import pandas as pd
import zipfile

ticker_file = "C:/Users/Franck/Desktop/options_tickers.txt"
option_zip = "C:/Users/Franck/Desktop/options.zip"
rep_ticker_file = "C:/Users/Franck/Desktop/option_files_with_repetition.txt"


def retrieve_ticker_in_option_zip():
    with zipfile.ZipFile(option_zip, "r") as f:
        with open(rep_ticker_file, 'a+') as file:
            for file_name in f.namelist():
                file.write(f"{file_name[12:16]}\n")


def get_unique_ticker_name():
    with open(ticker_file, "a+") as f:
        with open(rep_ticker_file, "r") as rep_file:
            df = pd.read_csv(rep_file, header=None)
            unique_ticker_list = np.unique(df.to_numpy())
            for ticker in unique_ticker_list:
                f.write(f"{ticker}\n")


retrieve_ticker_in_option_zip()
get_unique_ticker_name()
