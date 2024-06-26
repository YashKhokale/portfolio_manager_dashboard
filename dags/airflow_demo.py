from datetime import datetime,timedelta,date
from airflow.utils.dates import days_ago
import pendulum

# from demo import Portfolio_manager
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.decorators import dag,task, task_group
from typing import Literal
import os 
import json
import requests
from bsedata.bse import BSE
import os 
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from time import sleep



default_args1={
    'owner':'yash'
}

def file_reader(dir:Literal["ddl", "elt"],sub_dir:Literal["load", "stage""raw","biz"],filename,file_type):
    print(f'/opt/airflow/portfolio_manager_dashboard/{dir}/{sub_dir}/{filename}.{file_type}')
    with open(f'/opt/airflow/portfolio_manager_dashboard/{dir}/{sub_dir}/{filename}.{file_type}','r') as file:
        reader=file.read()
    return reader

def file_writer(filename,file_type, data):
    with open(f'/opt/airflow/portfolio_manager_dashboard/bucket/progress/{filename}.{file_type}','w') as file:
        if file_type=='json':
            file.write(json.dumps(data))
        else:
            file.write(str(data))

@dag(
    dag_id='test_id',
    description='test description',
    default_args=default_args1,
    start_date= pendulum.today('UTC').add(days=-1),
    schedule= '@daily',
    tags=['first_dag','testing tags'])
def test_id_func():

    @task
    def webscrape_screener():
        screener_username='yashkhokale19@gmail.com'
        screener_password='Test@1234'
        main_url="https://www.screener.in/"
        login_url=main_url + 'login/?'

        # ChromeDriver options to run in headless mode
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')  # Required for Linux
        options.add_argument('--disable-dev-shm-usage')  # Required for Linux
        # options = Options()
        # options.headless = True
        driver = webdriver.Chrome(options=options)
        driver.get(login_url)
        sleep(2)
        search_box = driver.find_element(By.ID, "id_username")
        search_box.send_keys(screener_username)
        search_box2 = driver.find_element(By.ID, "id_password")
        search_box2.send_keys(screener_password)
        search_box2.submit()
        sleep(2)
        driver.get(main_url + 'screens/1545871/conservative/')
        # driver.get('https://www.screener.in/login/?')
        sleep(2)
        print("logged in Screener!!!")
        try:
            # Find the table element using XPath
            table_element = driver.find_element(By.XPATH, "/html/body/main/div[2]/div[5]/table/tbody")
        except Exception as e:
            print("Did not find the HTML path")
            print(e)
        # Get the HTML content of the table element
        html_content = table_element.get_attribute('outerHTML')
        # Output the HTML content
        file_writer(f"screener_content_{date.today()}",'html',html_content)
        # Close the WebDriver
        driver.quit()
        # return True

    @task
    def process_screener_data():
        with open(f"/opt/airflow/portfolio_manager_dashboard/bucket/progress/screener_content_{date.today()}.html",'r') as file:
            html_content= file.read()
        # Parse the HTML content 
        soup = BeautifulSoup(html_content, 'html.parser')
        # Find all rows in the table body
        rows = soup.find('tbody').find_all('tr')
        # Extract the data from the rows
        data = []
        for row in rows:
            cells = []
            for cell in row.find_all(['td', 'th']):
                if cell.find('a'):  # Check if the cell contains an <a> tag
                    if cell.find('a')['href'].find("company") != -1:
                        cells.append(cell.find('a')['href'])  # Extract the href attribute value
                    cells.append(cell.get_text())  # Extract the text content
                else:
                    cells.append(cell.get_text())  # Extract the text content
            data.append(cells)
        return data

    @task
    def populateLoadScreener(data:list):
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run(f"DELETE FROM LOAD.LOAD_SCREENER;")
        for record in data:
            # print(d)
            query=f"INSERT into LOAD.LOAD_SCREENER (DATA_LIST) VALUES (ARRAY{record})"
            print(query)
            my_pg_hook.run(f"INSERT into LOAD.LOAD_SCREENER (DATA_LIST) VALUES (ARRAY{record})")

    @task    
    def populateStageScreener():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run("DELETE FROM STAGE.STAGE_SCREENER;")
        my_pg_hook.run(file_reader('elt','stage','STAGE_SCREENER','sql'))
        return True
    @task
    def compare_stock_list():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        # Output:
        # compare_stock_list ()
        print('compare_stock_list()')
        result=my_pg_hook.get_records(sql=file_reader('ddl','biz','Diff_stage_screener_biz_stock_data','sql'))
        print(result)
        # print(type(result))
        return result

    @task
    def get_hist_upstox_data(stock_list,data_freq):
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        for row in stock_list:
            ISIN=my_pg_hook.get_records(F"SELECT MAX(DATE) FROM RAW.l_stock_date  WHERE stock_symbol = '{row[0]}' GROUP BY stock_symbol;")
            print(ISIN)
            if len(ISIN)==0:
                ISIN='2007-01-01'
            url = f'https://api.upstox.com/v2/historical-candle/BSE_EQ%7C{row[1]}/{data_freq}/{date.today()}/{ISIN[0][0]}'
            r = requests.get(url)
            sleep(2)
            json_data = r.json()
            print(url)
            file_writer(f'hist_upstox_{data_freq}_{row[0]}_{date.today()}','json',json_data)

    @task
    def get_latest_bse_data():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')

    # Output:
    # Driver Class for Bombay Stock Exchange (BSE)  
        print('get_latest_bse_data()')
        # query=file_reader('ddl','biz','biz_stock_list','sql')
        # stock_list=get_query_result(query)
        stock_list=my_pg_hook.get_records(sql=file_reader('ddl','biz','biz_stock_list','sql'))
        # Join the two lists
        # joined_list = stock_list + hist_list
        json_list=[]
        for row in stock_list:
            b = BSE()
            # to execute "updateScripCodes" on instantiation
            b = BSE(update_codes = True)
            json_data = b.getQuote(str(row[1]))
            # Append new data to existing data
            json_list.append(json_data)
        print(json_list)
        file_writer(f'latest_combined_{date.today()}','json',json_list)


    @task
    def truncateLoadTable():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run("TRUNCATE LOAD.LOAD_STOCK;")

    @task
    def populateLoadTable():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        dir='/opt/airflow/portfolio_manager_dashboard/bucket/progress/'
        for filename in os.listdir(dir):
            if filename.endswith(".json"):
                filepath = os.path.join(dir,filename)
                with open(filepath, "r") as file:
                    json_data = json.load(file)
                my_pg_hook.run(f"INSERT INTO load.load_stock VALUES ('{json.dumps(json_data)}','{filename}');")
    @task
    def truncateStageTable():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run("TRUNCATE STAGE.STAGE_UPSTOX_STOCK;")
        my_pg_hook.run("TRUNCATE STAGE.STAGE_BSE_STOCK;")
    @task
    def populateStageUpstoxStock():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run(file_reader('elt','stage','STAGE_UPSTOX_STOCK','sql'))
    
    @task
    def populateStageBSEStock():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run(file_reader('elt','stage','STAGE_BSE_STOCK','sql'))
    @task  
    def stageCombinedViewStock():
        print("Demo STAGE_STOCK_COMBINED_VIEW!!!")
    
    @task    
    def populateHubStock():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run(file_reader('elt','raw','H_STOCK','sql'))
    @task  
    def populateHubDate():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run(file_reader('elt','raw','H_DATE','sql'))
    @task  
    def populateLinkStockDate():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run(file_reader('elt','raw','L_STOCK_DATE','sql'))
    @task  
    def populateSatStock():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run(file_reader('elt','raw','S_STOCK_DATE-1','sql'))
        my_pg_hook.run(file_reader('elt','raw','S_STOCK_DATE-2','sql'))
    @task  
    def populateSatStockStatus():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run(file_reader('elt','raw','S_STOCK_STATUS-1','sql'))
        my_pg_hook.run(file_reader('elt','raw','S_STOCK_STATUS-2','sql'))
        my_pg_hook.run(file_reader('elt','raw','S_STOCK_STATUS-3','sql'))
        my_pg_hook.run(file_reader('elt','raw','S_STOCK_STATUS-4','sql'))

    @task_group(group_id='LOAD_DW')
    def LOAD_DW():    
        truncateLoadTable() >> populateLoadTable() >> truncateStageTable() >> [populateStageUpstoxStock(), populateStageBSEStock()] >> stageCombinedViewStock() >> [populateHubStock(), populateHubDate()] >> populateLinkStockDate() >> [populateSatStock(), populateSatStockStatus()]

    @task_group(group_id='Webscrape_and_stage_grp')
    def Webscrape_and_stage_grp():    
        webscrape = webscrape_screener()
        process_data_func = process_screener_data()
        StageScreener=populateStageScreener()
        webscrape >> process_data_func >> populateLoadScreener(process_data_func) >> StageScreener
    
    @task_group(group_id='API_call')
    def API_call():    
        stock_list = compare_stock_list() 
        stock_list >> [get_hist_upstox_data(stock_list,'month') ,get_hist_upstox_data(stock_list,'day') ]
        get_latest_bse_data()


    LOAD_DW_func_grp=LOAD_DW()
    Webscrape_func_grp=Webscrape_and_stage_grp()
    API_call_grp=API_call()

    Webscrape_func_grp >> API_call_grp >>  LOAD_DW_func_grp
    # LOAD_DW_func_grp


test_id_func()

