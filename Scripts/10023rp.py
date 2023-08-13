from bs4 import BeautifulSoup
import requests

from selenium import webdriver
from selenium.webdriver.common.by import By
import chromedriver_binary
from selenium.webdriver.chrome.options import Options
import time


#url = "https://www.konga.com/category/electronics-5261"

def caller(url):

    arr = []
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(8)

    data = driver.find_element(By.XPATH, '//*[@id="mainContent"]/section[3]/section/section/section/section/ul')
    #response = driver.page_source
    #soup = BeautifulSoup(response, 'html.parser')

    #print(soup)

    # class_='bbe45_3oExY _22339_3gQb9'
    #links = soup.find_all(attrs={'class':"_7e903_3FsI6"})
    response = data.get_attribute('innerHTML')

    soup = BeautifulSoup(response, 'html.parser')
    links = soup.find_all(attrs={'class':"bbe45_3oExY _22339_3gQb9"})

    #print(links.prettify())

    for link in links:
        lnks = link.find(attrs={'class':"_4941f_1HCZm"})
        print(lnks.h3.text,end='\n')
        arr.append(lnks.h3.text)
    return arr




def main():

    data = []
    payload = {}
    for k in range(1,22):
        url = f"https://www.konga.com/category/electronics-5261?page={k}"
        print("RUNNING FOR URL:",url)
        res = caller(url)
        data.extend(res)
    payload["Product"] = data
    return payload



main()


    