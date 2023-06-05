import csv
import pandas as pd
import asyncio
import aiohttp
import requests
import os.path

report = ["income-statement", "balance-sheet", "cash-flow"]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
}

"""class Ticker:
    def __init__(self, name):
        self.income_statement = []
        self.balance_sheet = []
        self.cash_flow = []
        self.name = name
        self.loop = asyncio.get_event_loop()

    def __str__(self):
        return self.name

    async def add_link_async(self, session):
        for i in range(3):
            k = 1
            url = "https://www.barchart.com/stocks/quotes/" + self.name + "/" + report[i] + "/annual?reportPage="
            while True:
                try:
                    a = asyncio.create_task(session.get(url + str(k)))
                    async with session.get(url + str(k)) as resp:
                        pd.read_html(await resp.text())
                    if i == 0:
                        self.income_statement.append(a)

                    if i == 1:
                        self.balance_sheet.append(a)
                    if i == 2:
                        self.cash_flow.append(a)
                    print(k)
                    k += 1
                except:
                    break

    def add_link(self, session):
        self.loop.run_until_complete(self.add_link_async(session))


def get_tasks(session):
    ticker_list = []
    compteur = 0
    with open("filtered_symbol.csv", "r") as symbol_file:
        csv_file = csv.reader(symbol_file)
        for line in csv_file:
            compteur += 1
            ticker = Ticker(name=line[0])
            print(ticker.name)
            ticker.add_link(session=session)
            print(ticker.income_statement)
            ticker_list.append(ticker)
    return ticker_list"""


async def extract_report(ticker, report_index):
    async with aiohttp.ClientSession(headers=headers) as session:
        url = "https://www.barchart.com/stocks/quotes/" + ticker + "/" + report[report_index] + "/annual?reportPage="
        final_data_frame = pd.DataFrame()
        i = 1
        if not os.path.exists(ticker + "_" + report[report_index] + ".csv"):
            print("working with ticker " + ticker + " on report " + report[report_index])
            while True:
                try:
                    async with session.get(url + str(i)) as resp:
                        tb = pd.read_html(await resp.text())
                        final_data_frame = pd.concat([final_data_frame, tb[0]], axis=1, ignore_index=True)
                        print(i)
                        i += 1
                        if i > 20:
                            break
                        final_data_frame.to_csv(ticker + "_" + report[report_index] + ".csv")
                        return True
                except:
                    return False
        else:
            print(report[report_index] + " report of this " + ticker + " already exits")


async def all_symbol_extract():
    with open("filtered_symbol_2.csv", "r") as symbol_file:
        csv_file = csv.reader(symbol_file)
        for line in csv_file:
            for i in range(3):
                await extract_report(line[0], i)


asyncio.run(all_symbol_extract())

"""session = aiohttp.ClientSession(headers=headers)
print(get_tasks(session))
session.close()"""
