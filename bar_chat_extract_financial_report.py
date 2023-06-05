import csv
import pandas as pd
import requests
import os.path

report = ["income-statement", "balance-sheet", "cash-flow"]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
}


def extract_report(ticker, report_index):
    if not os.path.exists(ticker + "_" + report[report_index] + ".csv"):
        url = "https://www.barchart.com/stocks/quotes/" + ticker + "/" + report[report_index] + "/annual?reportPage="
        final_data_frame = pd.DataFrame()
        i = 1
        print("working with ticker " + ticker + " on report " + report[report_index])
        while True:
            try:
                tb = pd.read_html(requests.get(url + str(i), headers=headers).content)
                final_data_frame = pd.concat([final_data_frame, tb[0]], axis=1)
                print(i)
                i += 1
                if i>20:
                    break
                final_data_frame.to_csv(ticker + "_" + report[report_index] + ".csv")
            except:
                break
    else:
        print(report[report_index] + " report of this " + ticker + " already exits")


with open("filtered_symbol.csv", "r") as symbol_file:
    csv_file = csv.reader(symbol_file)
    iteration = 0
    for line in csv_file:
        iteration+=1
        print("we are at the " + str(iteration) + "-th iteration which represents " + str(iteration * 100 / 44977) + "%" + " of the total")
        for i in range(3):
            extract_report(line[0], i)
