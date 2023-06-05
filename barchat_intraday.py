import csv
import requests
import os.path
from urllib.parse import unquote
import time

header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/110.0",
}


def get_intra_day_data(ticker):
    # if not os.path.exists("C:/Users/Franck/Desktop/BarChat_intraday_data/" + ticker + "_intra_day.csv"):
    if not os.path.exists(ticker + "_intra_day.csv"):

        print(f"working with ticker {ticker}")
        main_url = "https://www.barchart.com/proxies/timeseries/queryformtminutes.ashx?"
        sub_url = "https://www.barchart.com"
        with requests.Session() as req:
            req.headers.update(header)
            r = req.get(sub_url)
            req.headers.update(
                {'X-XSRF-TOKEN': unquote(r.cookies.get_dict()['XSRF-TOKEN'])})
            params = {
                "symbol": ticker,
                "interval": "5",
                "start": "00000000000000",
                "end": "20230223235959",
                "volume": "contract",
                "order": "asc",
                "dividends": "false",
                "backadjust": "false",
                "daystoexpiration": "1",
                "contractroll": "expiration",
            }
            r = req.get(url=main_url, params=params)
            # with open("C:/Users/Franck/Desktop/BarChat_intraday_data/" + ticker + "_intra_day.csv", "w") as file:
            with open(ticker + "_intra_day.csv", "a+") as file:
                file.write(f"date,x,open,high,low,close,volume\n")
                file.write(r.text)
    else:
        print(f"intra_day data of {ticker} already exists")


"""a = time.time()
with open("barchat_ticker_84000.csv", "r") as file:
    csv_file = csv.reader(file)
    iteration = 0
    for index, line in enumerate(csv_file):
        if index < 20:
            iteration += 1
            print(f"we are at the iteration {iteration} which represents {iteration / 84000} % of the total")
            get_intra_day_data(line[0])
        else:
            break
b = time.time()
print(b - a)"""

get_intra_day_data("^GSPC")
