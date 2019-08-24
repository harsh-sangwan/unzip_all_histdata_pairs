import urllib
from bs4 import BeautifulSoup as bs
import requests
from glob import glob
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os
from time import sleep
import wget
import zipfile
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


exe_path = '/usr/local/bin/chromedriver'

def unzip_file(filename, extract_to):
    with zipfile.ZipFile(filename, 'r') as zip_file:
        zip_file.extractall(extract_to)

# print(page.source)
def delete_except(path, extension='.csv'):
    unzipped_files = os.listdir(path)
    for unzipped_file in unzipped_files:
        if extension in unzipped_file:
            pass
        else:
            try:
                os.remove(path + unzipped_file)
            except:
                pass

base_url = 'https://www.histdata.com'
url = base_url + '/download-free-forex-data/?/metatrader/1-minute-bar-quotes'
print(url)

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)


r = session.get(url)
soup = bs(r.content)

all_quotes = []

for links in soup.findAll('a'):

    temp = url.split('/')[-1]
    if temp in links['href']:
        href    = base_url + links['href']
        quote   = href.split('/')[-1]

        all_quotes.append(quote)
        print(href)
        print(quote)

        quote_path = quote + '/'
        
        if not os.path.exists(quote_path):
          os.makedirs(quote_path)

        quote_page = session.get(href)
        quote_soup = bs(quote_page.content)

        for quote_zip_links in quote_soup.findAll('a'):
            if temp in quote_zip_links['href']:
                quote_zip_link = base_url + quote_zip_links['href']
                print(quote_zip_link)

                download_page       = session.get(quote_zip_link)
                download_soup       = bs(download_page.content)
                temp_file           = str(download_soup.find(id='a_file')).split('.zip')[0].split('>')[-1] + '.zip'
                temp_file_list      = temp_file.split('_')
                temp_file_list[-2]  = temp_file_list[-2] + temp_file_list[-1]

                temp_file_list.remove(temp_file_list[-1])
                filename = '_'.join(temp_file_list)

                print(filename)
                driver = webdriver.Chrome(exe_path)
                driver.get(quote_zip_link)
                download_link = driver.find_element_by_id("a_file")
                download_link.click()

                while os.path.exists('../../Downloads/'+filename) is False:
                  sleep(10)
                  #if stuck for file
                  #refresh using driver.navigate().refresh()

                driver.close()

                os.rename('../../Downloads/'+filename, quote_path + filename)
                unzip_file(quote_path+filename, quote_path)

                delete_except(quote_path, extension='.csv')

#downloaded all files for the pair
#merge all csv files
for quote in all_quotes:
    quote_path = quote + '/'
    filenames = glob(quote_path+'*.csv')
    df_append = pd.DataFrame()
    for file in filenames:
        df = pd.read_csv(file, sep=';', index_col=0, header=None,
                         names=['Time', 'Open', 'High', 'Low', 'Close', 'unknown'])
        df_append = df_append.append(df)

    df_append = df_append.sort_index()
    df.index = pd.to_datetime(df.index)
    df.to_csv(quote_path+'merged_sorted.csv')

