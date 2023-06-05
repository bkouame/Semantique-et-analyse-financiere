import pandas as pd
import json

ticker_file = "C:/Users/Franck/Desktop/options_tickers.txt"
nvdaq_file = "C:/Users/Franck/Downloads/nasdaq_screener_1685355014823.csv"

tick_df = pd.read_csv(ticker_file, header=None)
tick_df.columns = ["ticker"]

nvdaq_df = pd.read_csv(nvdaq_file)
tick_df = tick_df.merge(nvdaq_df[["Symbol", "Name"]], how="left", left_on="ticker", right_on="Symbol")
tick_df.drop("Symbol", axis=1, inplace=True)
tick_df["Name"] = tick_df["Name"].fillna("")

ticker_dict = {}
for index, row in tick_df.iterrows():
    ticker_dict[row["ticker"]] = row["Name"]

with open("ticker_and_corporation_file.json", "w") as f:
    json.dump(ticker_dict, f)
