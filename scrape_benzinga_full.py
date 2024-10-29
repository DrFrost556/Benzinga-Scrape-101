import os
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import numpy as np

companies = pd.read_csv('new_companies.csv')['ticker'].values
#indice = np.where(companies == 'OMAB')[0]
#idx = indice[0]
#companies = companies[idx+1:]

symbols = []
for stock in companies:
	if stock not in symbols:
		symbols.append(stock)
		file_path = f'./benzinga_scrape/partner_headlines/{stock}.csv'
		if not os.path.isfile(file_path):
			symbols.remove(stock)

symbols = pd.DataFrame(symbols, columns = ['ticker'])
symbols.to_csv('new_companies.csv')

if 'benzinga_scrape' not in os.listdir():
	print('Creating folder /benzinga_scrape...')
	os.mkdir('benzinga_scrape')
if 'analyst_ratings' not in os.listdir('benzinga_scrape'):
	print('Creating folder /benzinga_scrape/analyst_ratings...')
	os.mkdir('benzinga_scrape/analyst_ratings')
if 'partner_headlines' not in os.listdir('benzinga_scrape'):
	print('Creating folder /benzinga_scrape/partner_headlines')
	os.mkdir('benzinga_scrape/partner_headlines')
if 'earnings_headlines' not in os.listdir('benzinga_scrape'):
	print('Creating folder /benzinga_scrape/earnings_headlines')
	os.mkdir('benzinga_scrape/earnings_headlines')
if 'media_headlines' not in os.listdir('benzinga_scrape'):
	print('Creating folder /benzinga_scrape/earnings_headlines')
	os.mkdir('benzinga_scrape/media_headlines')
if 'insider_headlines' not in os.listdir('benzinga_scrape'):
	print('Creating folder /benzinga_scrape/earnings_headlines')
	os.mkdir('benzinga_scrape/insider_headlines')
if 'general_headlines' not in os.listdir('benzinga_scrape'):
	print('Creating folder /benzinga_scrape/general_headlines')
	os.mkdir('benzinga_scrape/general_headlines')



def get_benzinga_data(stock):
	user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20240103 Firefox/123.0"
	options = webdriver.FirefoxOptions()
	options.add_argument(f"--user-agent={user_agent}")
	ff = webdriver.Firefox(options=options)
	ff.install_addon('ublock_origin-1.56.0.xpi')
	max_clicks = 20
	click_count = 0
	wait = WebDriverWait(ff,15)
	try:
		ff.get(f'https://benzinga.com/quote/{stock}/news')

		wait.until(EC.element_to_be_clickable((By.XPATH,'/html/body/div[1]/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[2]/div/div[3]/div[1]/label'))).click()
		time.sleep(1)

		while True:
			try:
				item_count = len(ff.find_elements(By.XPATH,"/html/body/div[1]/div[2]/div[2]/div/div/div/div[2]/div/div/div[3]/div"))
				elem = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[3]/button')))
				elem.click()

				new_item_count = len(ff.find_elements(By.XPATH,"/html/body/div[1]/div[2]/div[2]/div/div/div/div[2]/div/div/div[3]/div"))

				if new_item_count == item_count:
					click_count += 1
				else:
					click_count = 0
					item_count = new_item_count

				if click_count >= max_clicks:
					break
			except Exception as e:
				break

		analyst_ratings = []


		date_index = 1
		while True:
			try:
				current_index = 1
				found_elements = False  # Initialize a flag to track if elements are found for the current date_index
				while True:
					try:
						header = f'/html/body/div[1]/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[3]/div[{date_index}]/ul/li[{current_index}]/a'
						headline = ff.find_element("xpath",f'/html/body/div[1]/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[3]/div[{date_index}]/ul/li[{current_index}]/a/div[1]/span').text
						url = ff.find_element("xpath", header).get_attribute('href')
						publisher = ff.find_element("xpath",f'/html/body/div[1]/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[3]/div[{date_index}]/ul/li[{current_index}]/a/div[2]').text
						date = ff.find_element("xpath",f'/html/body/div[1]/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[3]/div[{date_index}]/ul/li[{current_index}]/a/div[2]/span[2]').text
						analyst_ratings.append([stock, headline, date])
						current_index += 1
						found_elements = True  # Set the flag to True if elements are found
					except Exception as e:
						break  # Break out of the inner loop if no elements are found for the current_index
				if not found_elements:  # If no elements are found for the current date_index, break out of the outer loop
					break
				date_index += 1
			except Exception as e:
				break
		analyst_ratings = pd.DataFrame(analyst_ratings, columns = ['ticker','headline','date'])
		analyst_ratings.to_csv('benzinga_scrape/general_headlines/{}.csv'.format(stock))
		ff.close()

	except Exception as e:
		print(e)
		ff.close()

from joblib import Parallel, delayed
core_count = int(input('How many cores to use?: '))

print('Starting data mine...')
Parallel(core_count, 'loky', verbose = 10)(delayed(get_benzinga_data)(stock) for stock in symbols)


