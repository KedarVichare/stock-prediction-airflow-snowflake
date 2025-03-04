from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook



def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.conn()

@task
def return_last_90d_price(symbol):
    vantage_api_key = Variable.get('vantage_api_key')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
    
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"API request failed: {response.status_code}, {response.text}")

    data = response.json()
    
    if "Time Series (Daily)" not in data:
        raise ValueError(f"Invalid API response: {data}")

    results = []
    for d in data["Time Series (Daily)"]:
        stock_info = data["Time Series (Daily)"][d]
        stock_info["date"] = d
        results.append(stock_info)
    
    return results[:90]

@task
def insert_into_snowflake(records):
    target_table = "dev.raw.stock_data"
    try:
        conn.execute("BEGIN;")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
                          symbol varchar,
                          date date,
                          open number,
                          high number,
                          low number,
                          close number,
                          volume number
                      )""")
        conn.execute(f"""DELETE FROM {target_table}""")
        insert_data = []
        for r in records:
            open_val = r["1. open"]
            high = r["2. high"]
            low = r["3. low"]
            close = r["4. close"]
            volume = r["5. volume"]
            date = r["date"]
            insert_data.append((open_val, high, low, close, volume, date, 'MSFT'))
        insert_sql = f"""
        INSERT INTO {target_table} (open, high, low, close, volume, date, symbol)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        conn.executemany(insert_sql, insert_data)
        conn.execute("COMMIT;")
    except Exception as e:
        conn.execute("ROLLBACK;")
        print(e)    
        raise e    
    finally:
        conn.close()
        conn.close()

@task
def count_records_in_snowflake():
    conn.execute("SELECT COUNT(*) FROM RAW.stock_data;")
    count = conn.fetchone()[0]
    print(f"Number of records in the table: {count}")
    
    conn.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'stock_price_dag',
    default_args=default_args,
    description='Fetch stock data and insert into Snowflake',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:

    stock_symbol = "MSFT"
    conn = return_snowflake_conn()
    price_data = return_last_90d_price(stock_symbol)
    insert_data = insert_into_snowflake(price_data)
    count_records = count_records_in_snowflake()


