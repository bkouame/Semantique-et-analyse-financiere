import zipfile
import os
from tqdm import tqdm
import pandas as pd

ticker_file = "C:/Users/Franck/Desktop/options_tickers.txt"
stock_zip_path = "C:/Users/Franck/Desktop/stock_data.zip"


def create_zip(zip_file_path):
    L = []
    with zipfile.ZipFile(zip_file_path, 'r') as original_zip:
        with open(ticker_file, "r") as f:
            tc_df = pd.read_csv(f, header=None)
            for ticker in tc_df.to_numpy():
                ticker_in_zip = False
                for file_name in tqdm(original_zip.namelist()):
                    if ticker[0] == file_name.split("_")[0]:
                        ticker_in_zip = True
                if not ticker_in_zip:
                    L.append(ticker[0])
    print(L)


create_zip(stock_zip_path)
