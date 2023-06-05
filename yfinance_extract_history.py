import yfinance as yf
import os.path


def extract_history(ticker):
    if not os.path.exists(f"C:/Users/Franck/Desktop/All_yahoo_finance_historical_data/{ticker}.csv"):
        try:
            yahoo_ticker = yf.Ticker(ticker)
            dataDF = yahoo_ticker.history(period="max")
            dataDF.to_csv(f"C:/Users/Franck/Desktop/All_yahoo_finance_historical_data/{ticker}.csv")
        except:
            pass
    else:
        print(ticker + " history file already exits")


with open("share_franck.txt", "r") as ticker_list:
    i = 0
    for ticker in ticker_list:
        a = ticker.replace("\n", "")
        extract_history(a)
        i += 1
        print("we are at the " + str(i) + "-th iteration which represents " + str(
            i * 100 / 80618) + "%" + " of the total")
