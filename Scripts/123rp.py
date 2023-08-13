from bs4 import BeautifulSoup
#import pandas as pd
import requests
import matplotlib

url = "https://www.newsnow.co.uk/h/Industry+Sectors/Energy+&+Utilities/Oil+&+Gas?type=ln"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')


def main():
	print("RUNNING--------------------")
	links = soup.find_all('a', {'class':'hll'})
	i = 0
	payload = {}
	Links = []
	refs = []
	for ln in links:
		print(ln.text)
		Links.append(ln.text)
		refs.append(ln['href'])
		i +=1
	payload["Link"] = Links
	payload["hrefs"] = refs
	return payload



