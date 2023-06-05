from bs4 import BeautifulSoup
import requests
import re

url = "https://finance.yahoo.com/sitemap_en-us_quotes_index.xml"
site_map_content = requests.get(url)
res_file = open("all_yahoo_finance_ticker.txt", "a+")
soup = BeautifulSoup(site_map_content.text, "html.parser")
all_sub_site_map = soup.find_all('loc')
for sub_map in all_sub_site_map:
    url2 = sub_map.text
    sub_site_map_content = requests.get(url2)
    sub_soup = BeautifulSoup(sub_site_map_content.text, "html.parser")
    all_sub_link = sub_soup.find_all(text=re.compile("summary"))
    for link in all_sub_link:
        link_text = link.parent.text.split("/")
        res_file.write(link_text[4] + "\n")
res_file.close()
