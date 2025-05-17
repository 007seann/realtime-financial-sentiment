
import pendulum
from airflow import DAG
from airflow.decorators import task
from plugins.common.time_log_decorator import time_log
from airflow.operators.bash import BashOperator
import time
import os
import pandas as pd
import datetime



with DAG(
    dag_id="reports_market_sentiment",
    schedule="0 0 1 1,4,7,10 *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,

) as dag:
    
    ############################### Configurations ################################

    start_date = '2025-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # Download Executor Configurations
    RATE_LIMIT = 5 # Maximum requests per second
    CONCURRENCY_LIMIT = 50 # the number of workers working concurrently
    BATCH_SIZE = 50 # Process 30 tickers at a time
    INITIAL_BACKOFF = 1 # Start with a 1-second delay
    MAX_RETRIES = 5 # Retry up to 5 times on failures
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    api_path = os.path.join(base_path, "api/rapidapi_key.txt")
    api_host = os.path.join(base_path, "api/rapidapi_host.txt")
    with open(api_path, "r") as file:
        api_key = file.read().strip()
    with open(api_host, "r") as file:
        api_host = file.read().strip()
    
    # Save File Paths
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    final_save_path = os.path.join(base_path, "data/SP500/reports/market")
    csv_file_path = os.path.join(base_path, "data/constituents/market/sp500_union_constituents.csv")
    columns = ["Name", "CIK", "Date", "Body" ]
    firms_df = pd.read_csv(csv_file_path)
    columns_to_drop = ['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded']
    firms_df = firms_df.drop(columns=columns_to_drop, errors='ignore')
    firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
    cik = firms_df['CIK'].drop_duplicates().tolist()
    ticker = firms_df['Symbol'].tolist()
    cik_to_ticker = dict(zip(cik, ticker))
    
    # Input File    
    extracted_folder = os.path.join(base_path, "data/SP500/reports/market/json")

    

    ###############################################################################
    @task(task_id='t1_download_id_executor')
    @time_log
    def download_id_executor(save_folder, api_key, api_host, start_date, end_date, **kwargs):
        import asyncio
        start_date = datetime.strptime(start_date, '%Y-%m-%d').year
        end_date = datetime.strptime(end_date, '%Y-%m-%d').year
        pages = [i for i in range(1, 3)] # Assume the total number of annual reports per firm is less than 80
        
        year2unixTime = {}
        for year in range(end_date + 1, start_date - 1, -1): # eg) 2026, 2025, 2024
            current_year_timestamp = int(datetime(year, 1, 1).timestamp())
            year2unixTime[year] = current_year_timestamp
        
        async def async_download_executor():
            from plugins.packages.FTRM.extract_report_ids import fetch_ids_for_ticker
            import aiohttp

            rate_limiter = asyncio.Semaphore(RATE_LIMIT)
            connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY_LIMIT)
            async with aiohttp.ClientSession(connector=connector) as session:
                tickers = list(cik_to_ticker.values())

                # Process in batches
                for i in range(0, len(tickers), BATCH_SIZE):
                    batch = tickers[i:i + BATCH_SIZE]
                    print(f"Processing batch {i // BATCH_SIZE + 1}: {batch}")
                    tasks = [fetch_ids_for_ticker(ticker, session, rate_limiter, save_folder, end_date, start_date, year2unixTime, api_key, api_host, pages, INITIAL_BACKOFF, MAX_RETRIES) for ticker in batch]
                    await asyncio.gather(*tasks)

        # Run the async function
        asyncio.run(async_download_executor())
    
    @task(task_id='t2_download_executor')
    @time_log
    def download_executor(save_folder, api_key, api_host, start_date, end_date, **kwargs):
        import asyncio
        start_date = datetime.strptime(start_date, '%Y-%m-%d').year
        end_date = datetime.strptime(end_date, '%Y-%m-%d').year
        
        async def async_download_executor():
            from plugins.packages.FTRM.extract_reports import fetch_reports
            import aiohttp

            rate_limiter = asyncio.Semaphore(RATE_LIMIT)
            connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY_LIMIT)
            async with aiohttp.ClientSession(connector=connector) as session:
                tickers = list(cik_to_ticker.values())

                # Process in batches
                for i in range(0, len(tickers), BATCH_SIZE):
                    batch = tickers[i:i + BATCH_SIZE]
                    print(f"Processing batch {i // BATCH_SIZE + 1}: {batch}")
                    tasks = [fetch_reports(ticker, session, rate_limiter, save_folder, api_key, api_host, INITIAL_BACKOFF, MAX_RETRIES, end_date, start_date) for ticker in batch]
                    await asyncio.gather(*tasks)

        # Run the async function
        asyncio.run(async_download_executor())


        
    @task(task_id='t3_dtm_constructor')
    @time_log
    def dtm_constructor(data_folder, save_folder, csv_file_path, columns, start_date, end_date, **kwargs):
        import os
        from plugins.packages.PDCM.constructDTM import ConstructDTM
        from pyspark.sql import SparkSession
        import subprocess
        # Optional: Check if Java is visible
        subprocess.run(["java", "-version"], check=True)
                
        os.environ['PYSPARK_SUBMIT_ARGS'] = "--master local[2] pyspark-shell"

        # Initialize Spark session
        spark = (
            SparkSession.builder
            .appName("DataPipeline")
            .master("local[2]")
            # Memory allocations
            .config("spark.driver.memory", "6g")
            .config("spark.executor.memory", "6g")
            .config("spark.sql.shuffle.partitions", "4") 

            .getOrCreate()
        )
        pipeline = ConstructDTM(spark, data_folder, save_folder, csv_file_path, columns, start_date, end_date)
        pipeline.file_aggregator()
        pipeline.process_filings_for_cik_spark(save_folder, start_date, end_date, csv_file_path)
        constituents_metadata_path = os.path.join(base_path, "data/constituents/market/sp500_constituents.csv") # This is for getting the CIKs for the SP500, but only for the year 2006 - 2023
        pipeline.concatenate_parquet_files(final_save_path, csv_file_path, constituents_metadata_path, start_date, end_date)
                
    
    #FTRM -> You should use correct API key to run this part.  The current API is expired
    t1_download_id_executor = download_id_executor(save_folder=final_save_path, api_key=api_key, api_host=api_host, start_date=start_date, end_date=end_date)
    t2_download_executor = download_executor(save_folder=final_save_path, api_key=api_key, api_host=api_host, start_date=start_date, end_date=end_date)
    #PDCM
    t3_dtm_constructor = dtm_constructor(data_folder=extracted_folder, save_folder=final_save_path, csv_file_path=csv_file_path, columns=columns, start_date=start_date, end_date=end_date)

    #SSPM
    t4_run_sent_predictor_local = BashOperator(
        task_id="t4_run_sent_predictor_local",
        bash_command=(
        "python3 /data/seanchoi/SSPM_local/sec_sent_predictor_local.py "
        "{{ params.csv_file_path }} "
        "{{ params.fig_loc }} "
        "{{ params.input_path }} "
        "{{ params.window }}"
        ),
        params={
            "csv_file_path": "data/constituents/market/sp500_union_constituents.csv",
            "fig_loc": "data/SP500/reports/market/outcome/figures",
            "input_path": "data/SP500/reports/market/dtm/final/analysis_report_summary.parquet",
            "window": end_date,
        },
        )


    
    t1_download_id_executor >> t2_download_executor >> t3_dtm_constructor >> t4_run_sent_predictor_local
        