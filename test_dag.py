from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
from datetime import datetime
import pandas as pd
import yfinance as yf

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snow_connection')
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(symbols, file_path):
    data = yf.download(symbols, period="180d", group_by = "ticker")
    all_data = []
    for symbol in symbols:
        temp_df = data[symbol].copy()
        temp_df["Symbol"] = symbol  
        temp_df.reset_index(inplace=True)
        all_data.append(temp_df)
    final_df = pd.concat(all_data, ignore_index=True)
    final_df['Date'] = final_df['Date'].dt.strftime('%Y-%m-%d')
    final_df.to_csv(file_path, index=False)

@task
def create_table():
    conn = return_snowflake_conn()
    target_table = "lab1.raw.stock_data"
    try:
        conn.execute("BEGIN;")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
                          date date,
                          open number,
                          high number,
                          low number,
                          close number,
                          volume number,
                          symbol varchar
                      )""")
        conn.execute(f"""DELETE FROM {target_table}""")
        conn.execute("COMMIT;")
    except Exception as e:
        conn.execute("ROLLBACK;")
        print(e)    
        raise e     
    finally:
        conn.close()

@task
def populate_table_via_stage(table, file_path):
    stage_name = f"TEMP_STAGE_{table}"
    file_name = os.path.basename(file_path) 
    conn.execute(f"CREATE TEMPORARY STAGE {stage_name}")
    conn.execute(f"PUT file://{file_path} @{stage_name}")
    copy_query = f"""
        COPY INTO {schema}.{table} (Date, Open, High, Low, Close, Volume, Symbol)
        FROM @{stage_name}/{file_name}
        FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    """
    conn.execute(copy_query)

@task
def train(stock_data_table, stock_forecast_view, stock_forecast_model):
    """
     - Create a view with training related columns
     - Create a model with the view above
    """
    create_view_sql = f"""CREATE OR REPLACE VIEW {schema1}.{stock_forecast_view} AS 
        SELECT DATE, CLOSE, SYMBOL 
        FROM {schema}.{stock_data_table}"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {schema1}.{stock_forecast_model} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{schema1}.{stock_forecast_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    )"""

    try:
        conn.execute(create_view_sql)
        conn.execute(create_model_sql)
        conn.execute(f"CALL {schema1}.{stock_forecast_model}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise

@task
def predict(stock_forecast_model, stock_data_table, stock_predictions_table, stock_final_table):
    """
     - Generate predictions and store the results to a table named forecast_table.
     - Union your predictions with your historical data, then create the final table
    """
    make_prediction_sql = f"""BEGIN
        -- This is the step that creates your predictions.
        CALL {schema1}.{stock_forecast_model}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store your predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {schema1}.{stock_predictions_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {schema1}.{stock_final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {schema}.{stock_data_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {schema1}.{stock_predictions_table};"""

    try:
        conn.execute(make_prediction_sql)
        conn.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise

with DAG(
    dag_id = 'lab1_dag',
    start_date = datetime(2024,6,21),
    catchup=False,
    tags=['ETL'],
    schedule_interval = '0 0 * * *'
) as dag:
    
    schema = "lab1.raw"
    table = "stock_data"
    file_path = "/opt/airflow/dags/stocks.csv"
    conn = return_snowflake_conn()
    extracted_data = extract(["AAPL", "GOOGL"], "/opt/airflow/dags/stocks.csv")
    create_tbl = create_table()
    inserted_data = populate_table_via_stage(table, file_path)

with DAG(
    dag_id = 'ml_dag',
    start_date = datetime(2024,6,21),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule_interval = '1 0 * * *'
) as dag:
    
    schema = "lab1.raw"
    schema1 = "lab1.analytics"
    table = "stock_data"
    view = "train_view"
    func_name = "forecast_function_name"
    stock_tbl = "forecast_table"
    stock_final_tbl = "final_table"
    conn = return_snowflake_conn()
    ml_train = train(table, view, func_name)
    ml_predict = predict(func_name, table, stock_tbl, stock_final_tbl)