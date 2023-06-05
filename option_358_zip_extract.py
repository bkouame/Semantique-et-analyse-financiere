import zipfile
import pandas as pd
from tqdm import tqdm
import os

option_zip = "C:/Users/Franck/Desktop/options.zip"
ticker_file = "C:/Users/Franck/Desktop/options_tickers.txt"
dash_option = "C:/Users/Franck/Desktop/dash_bs_option.zip"
directory = "C:/Users/Franck/Desktop/dash_bs_option"

L = []


def ticker_file_name_list():
    with zipfile.ZipFile(option_zip, "r") as opt_dir:
        with open(ticker_file, "r") as f:
            tc_df = pd.read_csv(f, header=None)
            for ticker in tqdm(tc_df.to_numpy()):
                tick_list = []
                for file_name in opt_dir.namelist():
                    if file_name[12:16] == ticker[0]:
                        tick_list.append(file_name)
                L.append(tick_list)


def select_ticker_file_with_max_size(file_list):
    max_size_file = file_list[0]
    with zipfile.ZipFile(option_zip, "r") as option_dir:
        for file in file_list:
            if option_dir.getinfo(file).file_size >= option_dir.getinfo(max_size_file).file_size:
                max_size_file = file
    return max_size_file


def create_max_size_file_txt_file():
    with open("max_size_ticker_option_file_list.txt", "a+") as ms_f:
        for ticker_list in tqdm(L):
            ms_f.write(f"{select_ticker_file_with_max_size(ticker_list)}\n")


def create_max_size_ticker_opt_file_zip():
    with zipfile.ZipFile(option_zip, "r") as zip_path:
        with open("max_size_ticker_option_file_list.txt", "r") as max_f:
            with zipfile.ZipFile(dash_option, 'w') as res_zip:
                ms_f_df = pd.read_csv(max_f, header=None)
                for file_name in tqdm(ms_f_df.to_numpy()):
                    data = zip_path.read(file_name[0])
                    res_zip.writestr(file_name[0], data)


for filename in os.listdir(directory):
    if os.path.isfile(os.path.join(directory, filename)):
        # Construct the new filename
        new_filename = f"{filename[1:5]}_opt.json"

        # Create the full file paths
        current_filepath = os.path.join(directory, filename)
        new_filepath = os.path.join(directory, new_filename)

        # Rename the file
        os.rename(current_filepath, new_filepath)