
import pendulum
from airflow import DAG
from airflow.decorators import task
from plugins.common.time_log_decorator import time_log

import time
import os
import pandas as pd
import datetime



with DAG(
    dag_id="sec_firm_sentiment",
    schedule="0 0 1 1,4,7,10 *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,

) as dag:
    
    ############################### Configurations ################################
    type = ['10-K', '10-Q']
    start_date = '2025-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # Save File Paths
    base_path = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    final_save_path = os.path.join(base_path, "data/SP500/sec/firm")
    csv_file_path = os.path.join(base_path, "data/constituents/firms/nvidia_constituents_final.csv")
    columns = ["Name", "CIK", "Date", "Body" ]
    firms_df = pd.read_csv(csv_file_path)
    columns_to_drop = ['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded']
    firms_df = firms_df.drop(columns=columns_to_drop, errors='ignore')
    firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
    
    # Input Files
    data_raw_folder = os.path.join(base_path, "data/SP500/sec/firm/html")
    extracted_folder = os.path.join(base_path, "data/SP500/sec/firm/txt")


    ###############################################################################
    @task(task_id='t1_test')
    def test(PATH):
    
        df = pd.read_csv(PATH, encoding = 'utf-8')
        cik = df['CIK'].drop_duplicates().tolist() 
        
        return cik
    
    @task(task_id='t2_download_executor')
    @time_log
    def download_executor(firm_list_path, type, start_date, end_date, **kwargs):
        from plugins.packages.FTRM.sec_crawler import download_filing
        import os
        import pandas as pd
        
        try:
            df = pd.read_csv(firm_list_path, encoding = 'utf-8')
            cik = df['CIK'].drop_duplicates().tolist()
            ticker = df['Symbol'].tolist()
            cik_ticker = dict(zip(cik, ticker))
        except UnicodeDecodeError:
            df = pd.read_csv(firm_list_path, encoding = 'ISO-8859-1')
            cik = df['CIK'].drop_duplicates().tolist()
            ticker = df['Symbol'].tolist()
            cik_ticker = dict(zip(cik, ticker))
            
        # root_folder = '10k-html'
        for t in type:
            doc_type = t
            headers = {'User-Agent': 'University of Edinburgh s2101367@ed.ac.uk'} # User Emails

            if not os.path.exists(data_raw_folder):
                os.makedirs(data_raw_folder)
            # The `download_fillings` function is a custom function imported from the
            # `plugins.packages.FTRM.sec_crawler` module. This function is used to download filings
            # for a list of companies based on their CIK (Central Index Key) and ticker symbols. The
            # function takes parameters such as the dictionary mapping CIK to ticker symbols, the data
            # folder path where the filings will be saved, the type of document to download (e.g.,
            # '10-K' or '10-Q'), headers for the HTTP request, start date, and end date for the
            # filings to be downloaded.
            cik = list(cik_ticker.keys())[0]
            ticker = list(cik_ticker.values())[0]
            download_filing(cik, ticker, data_raw_folder, doc_type,headers, start_date, end_date)
        
    
    @task(task_id='t3_txt_convertor')
    @time_log
    def txt_convertor(data_folder, save_folder, **kwargs):
        from plugins.packages.FTRM.sec_txt_extractor import process_fillings_for_cik
        import concurrent.futures
        import os
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for cik in os.listdir(data_folder):
                future = executor.submit(process_fillings_for_cik, cik, data_folder, save_folder)
                futures.append(future)
                
                
            # Wait for all tasks to complete
            for future in futures:
                future.result()
            
            # All tasks are completed, shutdown the executor
            executor.shutdown()

        
    @task(task_id='t4_dtm_constructor')
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
        
    @task(task_id='t5_sent_predictor')
    @time_log
    def sent_predictor(window, **kwargs):
        from plugins.packages.SSPM.sent_predictor_firm import SentimentPredictor
        config = {
            "constituents_path": os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), "data/constituents/firms/nvidia_constituents_final.csv"),
            "fig_loc": os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), "data/SP500/sec/firm/outcome/figures"),
            "input_path": os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), "data/SP500/sec/firm/processed/dtm_0001045810.parquet"),
            "window": window,
            }
        predictor = SentimentPredictor(config)
        predictor.run()
    

    
    #FTRM
    t2_download_executor = download_executor(csv_file_path, type = type, start_date=start_date, end_date=end_date)
    t3_txt_convertor = txt_convertor(data_raw_folder, extracted_folder)
    #PDCM
    t4_dtm_constructor = dtm_constructor(extracted_folder, final_save_path, csv_file_path, columns, start_date, end_date)

    #SSPM
    t5_sent_predictor = sent_predictor(window=end_date)

    
    t2_download_executor >> t3_txt_convertor >> t4_dtm_constructor >> t5_sent_predictor

        