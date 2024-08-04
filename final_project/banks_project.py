import pandas as pd 
import numpy as np 
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import sqlite3

def log_progress(message):
    """Logs the given message with a timestamp to a text file."""
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./banks_project_log.txt", "a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# Initial log entry
log_progress("Preliminaries complete. Initiating ETL process")

def extract(url, table_attribs):
    ''' Extract information from the specified URL and return a DataFrame. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')  
    tables = data.find_all('tbody')
    df = pd.DataFrame(columns=["Rank", "Name", "Market Cap (US$ Billion)"])
    rows = tables[1].find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            rank = cols[0].text.strip()
            name = cols[1].text.strip()
            market_cap = cols[2].text.strip().rstrip('\n').replace(',', '')
            try:
                market_cap_float = float(market_cap)
            except ValueError:
                market_cap_float = None
            data_dict = {
                "Rank": rank,
                "Name": name,
                "Market Cap (US$ Billion)": market_cap_float,     
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)   
    return df

# Example usage
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = {"class": "wikitable sortable"}
df = extract(url, table_attribs)

def transform(df1):
    exchange_rates_df = pd.read_csv('exchange_rate.csv')     
    exchange_rate = exchange_rates_df.set_index('Currency').to_dict()['Rate']
    if 'Market Cap (US$ Billion)' not in df1.columns:
        raise KeyError("'Market Cap (US$ Billion)' column is missing in the DataFrame")
    df1['MC_GBP_Billion'] = [np.round(x * exchange_rate.get('GBP', 0), 2) for x in df1['Market Cap (US$ Billion)']]
    df1['MC_EUR_Billion'] = [np.round(x * exchange_rate.get('EUR', 0), 2) for x in df1['Market Cap (US$ Billion)']]
    df1['MC_INR_Billion'] = [np.round(x * exchange_rate.get('INR', 0), 2) for x in df1['Market Cap (US$ Billion)']]
    log_progress("Data transformation complete. Initiating Loading process")
    return df1

# Sample DataFrame creation with 'Name'
data = {
    'Name': ['Company A', 'Company B', 'Company C', 'Company D', 'Company E'],
    'Market Cap (US$ Billion)': [10.5, 23.7, 5.6, 8.9, 14.3]
}
df1 = pd.DataFrame(data)
exchange_rate_csv = './exchange_rates.csv'
transformed_df = transform(df1)
print(transformed_df)
print("MC_EUR_Billion for the 5th largest bank:", transformed_df['MC_EUR_Billion'][4])
log_progress("Data loaded to Database as a table, Executing queries")

def load_to_csv(df, file_path):
    df.to_csv(file_path, index=False) 
    log_progress("Data saved to CSV file")

data = {
    'Name': ['Company A', 'Company B', 'Company C', 'Company D', 'Company E'],
    'Market Cap (US$ Billion)': [10.5, 23.7, 5.6, 8.9, 14.3],
    'MC_GBP_Billion': [7.67, 17.31, 4.07, 6.50, 10.44],
    'MC_EUR_Billion': [8.93, 20.15, 4.76, 7.57, 12.16],
    'MC_INR_Billion': [782.5, 1760.65, 417.4, 650.5, 1054.15]
}
df = pd.DataFrame(data)
file_path = './transformed_data.csv'
load_to_csv(df, file_path)

def load_to_db(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")

df = pd.DataFrame(data)
conn = sqlite3.connect('Banks.db')
load_to_db(df, conn, 'Largest_banks')
conn.close()
log_progress("Server Connection closed")

def run_query(query_statement, sql_connection):
    print("Query Statement:")
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print("Query Output:")
    print(query_output)

conn = sqlite3.connect('Banks.db')
query1 = "SELECT * FROM Largest_banks"
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query3 = "SELECT Name FROM Largest_banks LIMIT 5"  # Using 'Name' instead of 'Company'
run_query(query1, conn)
run_query(query2, conn)
run_query(query3, conn)
conn.close()
log_progress("Process Complete")
