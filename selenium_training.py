from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import os

os.environ['PATH'] += r"C:\chromedriver_win32"
driver = webdriver.Chrome()
driver.get("https://finance.yahoo.com/")
driver.implicitly_wait(600)
while True:
    pass
