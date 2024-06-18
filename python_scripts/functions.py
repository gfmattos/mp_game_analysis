from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
from datetime import date, datetime
import time

def initialize_browser(pag=None, headless=True):

    service = Service(ChromeDriverManager().install())
    options = Options()
    options.add_argument('--start-maximized')

    if headless:
        options.add_argument('--headless')

    browser = webdriver.Chrome(service=service, options=options)
    browser.implicitly_wait(10)

    if pag == None:
        browser.get('https://www.metacritic.com/browse/game/')
    else:
        browser.get(f'https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2024&page={pag}')

    return browser


def get_number_pages(soup):

    n_pag = int(soup.find_all('span', attrs={'class':'c-navigationPagination_itemButtonContent'})[-2].text)

    return n_pag


def store_data(conn,names,dates,scores,names_scrap,dates_scrap,scores_scrap):
    
    for name in names_scrap[1::2]:
        names.append(name.text)

    for date in dates_scrap:
        dates.append(date.text.strip())

    for score in scores_scrap:
        scores.append(int(score.text))
    
    games={'names':names,'dates':dates,'scores':scores}
    df = pd.DataFrame(data=games)

    df.to_sql(con=conn, index=False, name='TB_reviews', if_exists='append')

    #print('Data have been stored successfully')


def exec_webscraping(conn):

    browser = initialize_browser(headless=False)

    soup = BeautifulSoup(browser.page_source, 'html.parser')

    n_pag = get_number_pages(soup)

    print(f'We have a total of {n_pag} pages')
        
    names=list()
    dates=list()
    scores=list()

    start_time = time.time()

    print(f'Starting webscraping at {datetime.now()}')

    for pag in range(1,n_pag+1):

        if pag%10 == 0 or pag == n_pag:
            print(f'Starting webscraping in page {pag}')

        browser.get(f'https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2024&page={pag}')

        games_block = soup.find('div', attrs={'class':'c-productListings'})

        names_scrap = games_block.select("h3[class='c-finderProductCard_titleHeading'] > span")
        dates_scrap = games_block.select("div[class='c-finderProductCard_meta'] > span[class='u-text-uppercase']")
        scores_scrap = games_block.select("span[class='c-finderProductCard_metaItem c-finderProductCard_score'] > div > div > span")

        store_data(conn,names,dates,scores,names_scrap,dates_scrap,scores_scrap)

    print('All data has been exported successfully into your MySQL database!')

    end_time = time.time()

    execution_time = (end_time-start_time)/60

    print(f"Webscraping's Execution Time: {execution_time:.2f} minutes")
            
    browser.quit()


def exec_pipe():
    
    start_time = time.time()

    print(f'Starting program at {datetime.now()}')

    conn = sqlalchemy.create_engine('mysql+mysqlconnector://root:admin@localhost:3306/mp_game_analysis')

    exec_webscraping(conn)

    end_time = time.time()

    execution_time = (end_time-start_time)/60

    print(f"Total Execution Time: {execution_time:.2f} minutes")