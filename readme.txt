Overview
This project contains two Airflow DAGs that automate the ETL (Extract, Transform, Load) process and machine learning forecasting using Snowflake and Yahoo Finance data.
DAGs:
1. lab1_dag - Extracts stock data, creates a Snowflake table, and loads data.
2. ml_dag - Trains a forecasting model in Snowflake and generates stock predictions.
Features
* Full Refresh: The create_table function deletes all existing records before inserting new data.
* Transaction Handling: SQL statements are wrapped in BEGIN, COMMIT, and ROLLBACK for data consistency.
* Incremental Execution: Uses a schedule to run ETL (lab1_dag) daily and ML training (ml_dag) sequentially.
* Integration with Snowflake: Data is stored, transformed, and used for ML forecasting within Snowflake.
Dependencies
* Airflow
* Snowflake Python Connector
* Yahoo Finance (yfinance)
* Pandas
DAG Breakdown
lab1_dag (ETL Pipeline)
1. Extract (extract): Downloads stock data from Yahoo Finance.
2. Create Table (create_table): Ensures the target Snowflake table exists and performs a full refresh.
o Uses BEGIN; and ROLLBACK; to handle failures.
3. Load Data (populate_table_via_stage): Uploads stock data to a Snowflake stage and loads it into the table.
ml_dag (Machine Learning Pipeline)
1. Train Model (train):
o Creates a view of historical stock data.
o Trains a forecasting model using SNOWFLAKE.ML.FORECAST.
2. Predict (predict):
o Generates future stock price forecasts.
o Merges predictions with historical data into a final table.
SQL Transaction Handling
* BEGIN: Marks the start of a transaction.
* COMMIT: Saves changes permanently.
* ROLLBACK: Restores the database to its state before BEGIN if an error occurs.
* DELETE FROM table: Used in create_table for full refresh before loading new data.
How to Use
Running in Airflow
1. Ensure Airflow and dependencies are installed.
2. Add the DAG files to the Airflow DAGs directory.
3. Start Airflow Scheduler and Webserver.
4. Trigger the DAGs manually or wait for scheduled execution.
Pushing to GitHub
Troubleshooting
Issue: DAG Tasks Not Connected
Ensure task dependencies are correctly set:
extracted_data >> create_tbl >> inserted_data
ml_train >> ml_predict
Issue: Not a Git Repository
Run git init in the correct project directory before pushing changes.
Conclusion
This project automates stock data extraction, storage, and forecasting in Snowflake using Airflow. It ensures data integrity with SQL transactions and supports machine learning workflows in Snowflake.

