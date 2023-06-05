import aiohttp
import asyncio
import csv
import os.path
from urllib.parse import unquote
import time

header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/110.0",
}


async def get_intra_day_data(ticker, session):
    if not os.path.exists(f"C:/Users/Franck/Desktop/BarChat_intraday_data/{ticker}_intra_day.csv"):
        try:
            print(f"working with ticker {ticker}")
            main_url = "https://www.barchart.com/proxies/timeseries/queryformtminutes.ashx?"
            sub_url = "https://www.barchart.com"
            async with session.get(sub_url) as r:
                xsrf_token = unquote(r.cookies.get('XSRF-TOKEN').value)
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
            headers = {'X-XSRF-TOKEN': xsrf_token}
            async with session.get(url=main_url, headers=headers, params=params) as r:
                if r.status == 200:
                    text = await r.text()
                    with open(f"C:/Users/Franck/Desktop/BarChat_intraday_data/{ticker}_intra_day.csv", "w") as file:
                        file.write(text)
        except:
            pass
    else:
        print(f"intra_day data of {ticker} already exists")


a = time.time()


async def main():
    async with aiohttp.ClientSession(headers=header) as session:
        with open("barchat_ticker_84000.csv", "r") as file:
            csv_file = csv.reader(file)
            tasks = []
            for index, line in enumerate(csv_file):
                if index < 50:
                    tasks.append(asyncio.ensure_future(get_intra_day_data(line[0], session)))
                if (index + 1) % 10 == 0:
                    await asyncio.gather(*tasks)
                    tasks = []


asyncio.run(main())
b = time.time()
print(b - a)
