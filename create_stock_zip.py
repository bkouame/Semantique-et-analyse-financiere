import zipfile
from tqdm import tqdm
import pandas as pd

stock_zip = "C:/Users/Franck/Desktop/charts_barchart.zip"
ticker_file = "C:/Users/Franck/Desktop/options_tickers.txt"


def create_zip(zip_file_path):
    res_zip_path = "C:/Users/Franck/Desktop/stock_data.zip"
    with zipfile.ZipFile(zip_file_path, 'r') as original_zip:
        with zipfile.ZipFile(res_zip_path, 'w') as res_zip:
            with open(ticker_file, "r") as f:
                tc_df = pd.read_csv(f, header=None)
                for ticker in tc_df.to_numpy():
                    for file_name in tqdm(original_zip.namelist()):
                        if ticker[0] == file_name.split("_")[0]:
                            data = original_zip.read(file_name)
                            res_zip.writestr(file_name, data)


create_zip(stock_zip)
