from selenium import webdriver
from time import sleep
from selenium.webdriver.common.by import By
import psycopg2
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
import json
import requests
import pprint
from bsedata.bse import BSE
from typing import Literal
import os

class Portfolio_manager:
    def __init__(self) -> None:
        self.username='yashkhokale19@gmail.com'
        self.password='Test@1234'
        self.main_url="https://www.screener.in/"
        self.login_url=self.main_url + 'login/?'
        # Establish a connection to the PostgreSQL database
        # self.conn = psycopg2.connect(
        #     dbname="yash_db",
        #     user="yash",
        #     password="test",
        #     host="localhost",
        #     port="5433"
        # )
        self.dbname="yash_db"
        self.user="yash"
        self.password="test"
        # self.host="localhost",
        self.host="172.17.208.1"
        self.port="5433"

        # self.alpha_api_key='ZT190HZDN99BS851'
        self.alpha_api_key='A4TZ89DY9G25HE5U'
        # self.symbol=''
        # self.latest_alpha_url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={self.symbol}.BSE&apikey=' + self.alpha_api_key
   
    def file_writer(self,filename,file_type, data):
        with open(f'bucket/progress/{filename}.{file_type}','w') as file:
            if file_type=='json':
                file.write(json.dumps(data))
            else:
                file.write(str(data))

    def file_reader(self,dir:Literal["ddl", "elt"],sub_dir:Literal["load", "stage","raw","biz"],filename,file_type):
        print(f'{dir}/{sub_dir}/{filename}.{file_type}')
        with open(f'{dir}/{sub_dir}/{filename}.{file_type}','r') as file:
            reader=file.read()
        return reader
    
        # Function to insert JSON data into the table
    
    def query_executor(self, query):
        try:
            # Connect to the PostgreSQL database
            conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            cursor.close()
            print(f"Successfully executed sql !")
        except psycopg2.Error as e:
            print("Error: Unable to insert data")
            print(e)


    # Function to insert JSON data into the table
    # def insert_json(self, schema, table, json_data, record_source):
        # try:
        #     # Connect to the PostgreSQL database
        #     conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
        #     cursor = conn.cursor()
        #     insert_query = f"INSERT INTO {schema}.{table} VALUES (%s,%s);"
        #     cursor.execute(insert_query, (json.dumps(json_data),record_source))
        #     conn.commit()
        #     cursor.close()
        #     print(f"Successfully inserted into {schema}.{table} !!!")
        # except psycopg2.Error as e:
        #     print("Error: Unable to insert JSON data")
        #     print(e)

    def csv_db_ingestion(self, schema, table, csv_filename):
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
        # Create a cursor object
        cur = conn.cursor()
        # Define the COPY command to load the CSV data into the table
        copy_sql = f"""
                COPY {schema}.{table} FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
        # Execute delete statement
        cur.execute(f"DELETE FROM {schema}.{table};")

        # Open and read the CSV file
        with open(csv_filename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            # conn.commit()
       
        # Execute delete statement
        cur.execute("DELETE FROM yash_schema.stage_screener;")

        with open('stage_screener.sql', 'r') as file:
            query = file.read()
        # print(sql_commands)
        cur.execute(query)
        conn.commit()
        # Close the cursor and connection
        cur.close()
        conn.close()
    
    def get_query_result(self, sql_query):
        # SQL query to fetch data from the table
        try:
            # Establish connection to the database
            conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
            cur = conn.cursor()
            # Execute the SQL query
            cur.execute(sql_query)
            # Fetch all rows from the result set
            rows = cur.fetchall()
            # print(rows)
        except psycopg2.Error as e:
            print("Error fetching data:", e)
        finally:
            # Close cursor and connection
            cur.close()
            conn.close()
        return rows
    
    def get_latest_alpha_data(self,stock_list,):

        query=self.file_reader('','biz_stock_list','sql')
        stock_list=self.get_query_result(query)
        json_data1=[]
        for row in stock_list:
            if row[0] not in list(map(lambda x: x[0], stock_list)):
                # print(symbol)            
                self.latest_alpha_url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={row[0]}.BSE&apikey=' + self.alpha_api_key
                r = requests.get(self.latest_alpha_url)
                json_data = r.json()
                # print(list(json_data.keys()))
                json_data1.append(json_data)
            print(json_data1)
            self.file_writer('latest_combined','json',json_data1)

    def get_hist_upstox_data(self,stock_list,data_freq):
        # my_dict={'CREST': 'INE559D01011'}
        # json_list=[]
        for row in stock_list:
            url = f'https://api.upstox.com/v2/historical-candle/BSE_EQ%7C{row[1]}/{data_freq}/{date.today()}'
            r = requests.get(url)
            json_data = r.json()
            print(url)
            # json_list.append(json_data)
            self.file_writer(f'hist_upstox_{data_freq}_{row[0]}_{date.today()}','json',json_data)
            # with open(f'bucket/progress/hist_upstox_{data_freq}_{row[0]}_{date.today()}.json','w') as file:
            #     file.write(json.dumps(json_data))
            # self.insert_json( "load", "load_stock", json_data, f"hist_upstox_{row[0]}_{date.today()}")

    def screener_webscrapping(self):
        driver = webdriver.Chrome()
        driver.get(self.login_url)
        sleep(2)
        search_box = driver.find_element(By.ID, "id_username")
        search_box.send_keys(self.username)
        search_box2 = driver.find_element(By.ID, "id_password")
        search_box2.send_keys(self.password)
        search_box2.submit()
        # driver.get('https://www.screener.in/explore/')
        driver.get(self.main_url + 'screens/1545871/conservative/')
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
        self.file_writer(f"screener_content_{date.today()}",'html',html_content)
        # Close the WebDriver
        driver.quit()

    def process_screener_data(self):
        with open("screener_content.html",'r') as file:
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

        # Convert the data to a DataFrame
        df = pd.DataFrame(data)
        df.to_csv(f'table_data_{date.today()}.csv', index=False)
        # df=df.fillna('demo')
        print(df)
        self.csv_db_ingestion('yash_schema', 'load_screener', f'table_data_{date.today()}.csv')
        

    def compare_stock_list(self):
        # Output:
        # compare_stock_list (self)
        print('compare_stock_list()')
        query=self.file_reader('ddl','biz','Diff_stage_screener_biz_stock_data','sql')

        # with open('Diff_stage_screener_biz_stock_data.sql', 'r') as file:
        #     query = file.read()
        result=self.get_query_result(query)
        # self.query_result(query)
        # print(list(map(lambda x: x[0], result)))
        if len(result)>0:
            # self.get_hist_stock_data(result)
            self.get_hist_upstox_data(result,'day')
            self.get_hist_upstox_data(result,'month')
        self.get_latest_bse_data(result)

    # def get_hist_stock_data(self, stock_list):
        # for symbol in list(map(lambda x: x[0], stock_list)):
        #     url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}.BSE&outputsize=full&apikey=' + 'ZT190HZDN99BS851'
        #     r = requests.get(url)
        #     json_data = r.json()
        #     # data = r.json()
        #     print(url)
        #     self.file_writer(f'hist_alpha_{symbol}_{date.today()}','json',json_data)
            # with open(f'bucket/progress/hist_alpha_{symbol}_{date.today()}.json','w') as file:
            #     file.write(json.dumps(json_data))
        
        # self.insert_json( "load", "load_stock", json_data, f"hist_alpha_{symbol}_{date.today()}")

    def get_latest_bse_data(self,hist_list):
    # Output:
    # Driver Class for Bombay Stock Exchange (BSE)  
        print('get_latest_bse_data()')
        query=self.file_reader('ddl','biz','biz_stock_list','sql')
        stock_list=self.get_query_result(query)
        # Join the two lists
        # joined_list = stock_list + hist_list
        json_list=[]
        for row in stock_list:
            # if row[0] not in list(map(lambda x: x[0], exclude_list)):
            # print(row[0])            
            b = BSE()
            print(b)
            # to execute "updateScripCodes" on instantiation
            b = BSE(update_codes = True)
            json_data = b.getQuote(str(row[1]))
            # print(data)
            print(type(json_data))
            # Append new data to existing data
            json_list.append(json_data)
        print(json_list)
        self.file_writer(f'latest_combined_{date.today()}','json',json_list)

    def bse_api_call(self,code):
        b = BSE()
        print(b)
        # Output:
        # Driver Class for Bombay Stock Exchange (BSE)

        # to execute "updateScripCodes" on instantiation
        b = BSE(update_codes = True)
        data = b.getQuote(str(code))
        # print(data)
        return data

    def visualize(self):
        pass

    def truncateLoadTable(self):
        self.query_executor("TRUNCATE LOAD.LOAD_STOCK;")


    def populateLoadTable(self,json_data,filename):
        # self.load_json_files('bucket/progress')
        self.query_executor(f"INSERT INTO load.load_stock VALUES ('{json.dumps(json_data)}','{filename}');")

    
    def truncateStageTable(self):
        self.query_executor("TRUNCATE STAGE.STAGE_UPSTOX_STOCK;")
        self.query_executor("TRUNCATE STAGE.STAGE_BSE_STOCK;")

    def populateStageUpstoxStock(self):
        self.query_executor(self.file_reader('elt','stage','STAGE_UPSTOX_STOCK','sql'))
    
    def populateStageBSEStock(self):
        self.query_executor(self.file_reader('elt','stage','STAGE_BSE_STOCK','sql'))
    
    def populateHubStock(self):
        self.query_executor(self.file_reader('elt','raw','H_DATE','sql'))

    def populateHubDate(self):
        self.query_executor(self.file_reader('elt','raw','H_STOCK','sql'))

    def populateLinkStockDate(self):
        self.query_executor(self.file_reader('elt','raw','L_STOCK_DATE','sql'))

    def populateSatStock(self):
        self.query_executor(self.file_reader('elt','raw','S_STOCK_DATE-1','sql'))
        self.query_executor(self.file_reader('elt','raw','S_STOCK_DATE-2','sql'))

    def populateSatStockStatus(self):
        self.query_executor(self.file_reader('elt','raw','S_STOCK_STATUS-1','sql'))
        self.query_executor(self.file_reader('elt','raw','S_STOCK_STATUS-2','sql'))
        self.query_executor(self.file_reader('elt','raw','S_STOCK_STATUS-3','sql'))
        self.query_executor(self.file_reader('elt','raw','S_STOCK_STATUS-4','sql'))

    def execute_pipeline(self,json_data,filename):
        self.truncateLoadTable()
        self.populateLoadTable(json_data,filename)
        self.truncateStageTable()
        self.populateStageUpstoxStock()
        self.populateStageBSEStock()        
        self.populateHubStock()
        self.populateHubDate()
        self.populateLinkStockDate()
        self.populateSatStock()
        self.populateSatStockStatus()

    def load_json_files(self,dir):
        for filename in os.listdir(dir):
            if filename.endswith(".json"):
                filepath = os.path.join(dir,filename)
                with open(filepath, "r") as file:
                    json_data = json.load(file)
                    # self.insert_json('load','load_stock', json_data,filename)
                    self.json_data=json_data
                    self.filename=filename
                    # self.query_executor(f"INSERT INTO load.load_stock VALUES ('{json.dumps(json_data)}','{filename}');")
                    self.execute_pipeline(json_data,filename)



x= Portfolio_manager()

# x.screener_webscrapping()
# x.process_screener_data()
# x.get_latest_bse_data()
# x.db_ingestion()
x.compare_stock_list()
# x.visualize()
# x.truncateLoadTable()
# x.populateLoadTable()
# x.load_json_files('bucket/progress/')













