import csv

with open("All_Bar_chat_symbol_84000.csv", 'r') as file:
    res_final = open("barchat_ticker_84000.csv", "a+")
    csv_file = csv.reader(file)
    for line in csv_file:
        res_final.write(
            line[0].replace("'", "").replace(" ", "") + "," + line[1].replace("'", "").replace(" ", "") + "," + line[
                2] + "\n")
    res_final.close()
