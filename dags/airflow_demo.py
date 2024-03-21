from datetime import datetime,timedelta,date
from airflow.utils.dates import days_ago
# from airflow import DAG

# from demo import Portfolio_manager
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.decorators import dag,task
from typing import Literal
import os 
# import pandas as pd
import json
import requests
# import pprint
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

# def print_test():
#     print('Testing 123!')


def file_reader(dir:Literal["ddl", "elt"],sub_dir:Literal["load", "stage""raw","biz"],filename,file_type):
    print(f'/home/yashkhokale/portfolio_manager_dashboard/{dir}/{sub_dir}/{filename}.{file_type}')
    with open(f'{dir}/{sub_dir}/{filename}.{file_type}','r') as file:
        reader=file.read()
    return reader

def file_writer(filename,file_type, data):
    with open(f'/home/yashkhokale/portfolio_manager_dashboard/bucket/progress/{filename}.{file_type}','w') as file:
        if file_type=='json':
            file.write(json.dumps(data))
        else:
            file.write(str(data))

# def truncateStageTable():
#     my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
#     my_pg_hook.run("TRUNCATE STAGE.STAGE_UPSTOX_STOCK;")
#     my_pg_hook.run("TRUNCATE STAGE.STAGE_BSE_STOCK;")
#     # my_pg_hook.run("SELECT 123;")
#     # query_executor("TRUNCATE STAGE.STAGE_UPSTOX_STOCK;")
#     # query_executor("TRUNCATE STAGE.STAGE_BSE_STOCK;")

# def populateStageUpstoxStock():
#     my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
#     # query_executor(file_reader('elt','stage','STAGE_UPSTOX_STOCK','sql'))
#     my_pg_hook.run(file_reader('elt','stage','STAGE_UPSTOX_STOCK','sql'))


# def populateStageBSEStock():
#     query_executor(file_reader('elt','stage','STAGE_BSE_STOCK','sql'))

# def populateHubStock():
#     query_executor(file_reader('elt','raw','H_DATE','sql'))
# def populateHubDate():
#     query_executor(file_reader('elt','raw','H_STOCK','sql'))
# def populateLinkStockDate():
#     query_executor(file_reader('elt','raw','L_STOCK_DATE','sql'))
# def populateSatStock():
#     query_executor(file_reader('elt','raw','S_STOCK_DATE-1','sql'))
#     query_executor(file_reader('elt','raw','S_STOCK_DATE-2','sql'))
# def populateSatStockStatus():
#     query_executor(file_reader('elt','raw','S_STOCK_STATUS-1','sql'))
#     query_executor(file_reader('elt','raw','S_STOCK_STATUS-2','sql'))
#     query_executor(file_reader('elt','raw','S_STOCK_STATUS-3','sql'))
#     query_executor(file_reader('elt','raw','S_STOCK_STATUS-4','sql'))


# x=Portfolio_manager()

# with DAG(
#     dag_id='test_id',
#     description='test description',
#     default_args=default_args1,
#     start_date= days_ago(1),
#     schedule_interval= '@daily',
#     tags=['first_dag','testing tags']
# ) as dag:
#     truncateStageTable= PythonOperator(
#         task_id='truncateStageTable',
#         python_callable=truncateStageTable
#     )
#     populateStageUpstoxStock= PythonOperator(
#         task_id='populateStageUpstoxStock',
#         python_callable=populateStageUpstoxStock
#     )

# truncateStageTable >> populateStageUpstoxStock

@dag(
    dag_id='test_id',
    description='test description',
    default_args=default_args1,
    start_date= days_ago(1),
    schedule_interval= '@daily',
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
        with open(f"bucket/progress/screener_content_{date.today()}.html",'r') as file:
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
        # csv_db_ingestion('LOAD', 'LOAD_SCREENER', f'table_data_{date.today()}.csv',data)
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
        print(type(result))
        return result
        # print(list(map(lambda x: x[0], result)))
        # if len(result)>0:
        #     get_hist_upstox_data(result,'day')
        #     get_hist_upstox_data(result,'month')
        # get_latest_bse_data(result)

    @task
    def get_hist_upstox_data(stock_list,data_freq):
        # my_dict={'CREST': 'INE559D01011'}
        # json_list=[]
        for row in stock_list:
            url = f'https://api.upstox.com/v2/historical-candle/BSE_EQ%7C{row[1]}/{data_freq}/{date.today()}'
            r = requests.get(url)
            sleep(2)
            json_data = r.json()
            print(url)
            # json_list.append(json_data)
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
            # if row[0] not in list(map(lambda x: x[0], exclude_list)):
            # print(row[0])            
            b = BSE()
            # print(b)
            # to execute "updateScripCodes" on instantiation
            b = BSE(update_codes = True)
            json_data = b.getQuote(str(row[1]))
            # print(data)
            # print((json_data))
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
        dir='/home/yashkhokale/portfolio_manager_dashboard/bucket/progress/'
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
        # my_pg_hook.run("SELECT 123;")
        # query_executor("TRUNCATE STAGE.STAGE_UPSTOX_STOCK;")
        # query_executor("TRUNCATE STAGE.STAGE_BSE_STOCK;")

    @task
    def populateStageUpstoxStock():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        # query_executor(file_reader('elt','stage','STAGE_UPSTOX_STOCK','sql'))
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
        my_pg_hook.run(file_reader('elt','raw','H_DATE','sql'))

    @task  
    def populateHubDate():
        my_pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        my_pg_hook.run(file_reader('elt','raw','H_STOCK','sql'))

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

    # truncateLoadTable() >> populateLoadTable() >> truncateStageTable() >> [populateStageUpstoxStock(), populateStageBSEStock()] >> stageCombinedViewStock() >> [populateHubStock(), populateHubDate()] >> populateLinkStockDate() >> [populateSatStock(), populateSatStockStatus()]

    webscrape = webscrape_screener()
    data = process_screener_data()
    webscrape >> data
    StageScreener=populateStageScreener()
    TLoadTable=truncateLoadTable()
 
    populateLoadScreener(data) >> StageScreener >> get_latest_bse_data() >>  TLoadTable
     
    stock_list = compare_stock_list() 
    StageScreener >> stock_list
    [get_hist_upstox_data(stock_list,'month') ,get_hist_upstox_data(stock_list,'day') ]  >>  TLoadTable

    TLoadTable >> populateLoadTable() >> truncateStageTable() >> [populateStageUpstoxStock(), populateStageBSEStock()] >> stageCombinedViewStock() >> [populateHubStock(), populateHubDate()] >> populateLinkStockDate() >> [populateSatStock(), populateSatStockStatus()]

test_id_func()

