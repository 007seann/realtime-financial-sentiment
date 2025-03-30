from plugins.packages.PDCM.pipeline import run_process_for_cik
from plugins.packages.PDCM.metadata import FileMetadata, Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from pyspark.sql.functions import col, lit


import os
import tqdm
import hashlib
import datetime
import logging
import multiprocessing
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import re
import json
import sys

from concurrent.futures import ThreadPoolExecutor, as_completed
# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# print('os.path.dirname(__file__)', os.path.dirname(__file__))
# print("Resolved path being added to sys.path in constructDTM:", os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# ------------------ SQLAlchemy Setup ------------------ #

class ConstructDTM:
    def __init__(self, spark, data_folder, save_folder, firms_csv_file_path, columns, start_date, end_date):
        self.spark = spark
        self.data_folder = data_folder
        self.save_folder = save_folder
        self.columns = columns
        self.start_date = start_date
        self.end_date = end_date
        self.output_folder = os.path.join(save_folder, 'company_df')
        os.makedirs(self.output_folder, exist_ok=True)
        
        
        # Set up firms dictionary from CSV file
        firms_df = pd.read_csv(firms_csv_file_path)
        firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
        firms_dict = firms_df.set_index('Symbol')['CIK'].to_dict()
        firms_dict = {cik: symbol for symbol, cik in firms_dict.items()}
        self.firms_dict = firms_dict
        # Add hons_project.zip to SparkContext
        self.spark.sparkContext.addPyFile("/opt/airflow/plugins/packages/hons_project.zip")
        self.spark.sparkContext.addPyFile("/opt/airflow/plugins/packages/PDCM/pipeline.py")
        self.spark.sparkContext.addPyFile("/opt/airflow/plugins/packages/PDCM/metadata.py")
        self.spark.sparkContext.addPyFile("/opt/airflow/plugins/packages/PDCM/vol_reader_fun.py")

        
        # --------------- Configure Database --------------- #
        # Adjust connection string for your environment
        db_url = "postgresql://pdcm:pdcm@pdcmmetastore_container:5432/pdcm"
        self.engine = create_engine(db_url, echo=True)
        self.SessionLocal = sessionmaker(bind=self.engine)


        # Create table if not exists
        # Debug: Check if table creation is successful
        try:
            Base.metadata.create_all(self.engine)
            print("[Debug] Tables created successfully.")
        except Exception as e:
            print(f"[Error] Failed to create tables: {e}")
        

        
    # ------------------- Helper Methods ------------------- #    
    @staticmethod
    def import_file(file_path):
        """
        Placeholder function for importing file content.
        Replace with the actual file reading logic.
        """
        with open(file_path, 'r', encoding='latin-1') as file:
            return file.read()
    @staticmethod
    def compute_file_hash(file_path, chunk_size=65536):
        """
        Compute an MD5 (or other) hash for file content to detect changes.
        """
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                md5.update(data)
        return md5.hexdigest()
    @staticmethod
    def get_file_modified_time(file_path):
        """
        Return the last modification time of a file as a Python datetime.
        """
        epoch_time = os.path.getmtime(file_path)
        return datetime.datetime.fromtimestamp(epoch_time)

    @staticmethod
    def in_memory_directory(path):
        directory_path = []
        year_list = os.listdir(path)
        for y in year_list:
            if y.endswith('.DS_Store'):
                continue
            year_path = os.path.join(path, y)
            file_list = os.listdir(year_path)
            for f in file_list:
                if f.endswith('.DS_Store'):
                    continue    
                file_path = os.path.join(year_path, f)
                directory_path.append(file_path)
        
        return directory_path
    @staticmethod
    def isJson(root_dir):
        """
        Walk through the directory. Return True if above 95% of the files are JSON files.
        """
        total_files = 0
        json_files = 0

        # Debugging: Check if the directory exists
        if not os.path.exists(root_dir):
            print(f"[isJson] Directory does not exist: {root_dir}")
            return False


        for current_root, dirs, files in os.walk(root_dir):
            for filename in files:
                if filename.endswith('.DS_Store'):
                    continue
                total_files += 1
                if filename.lower().endswith('.json'):
                    json_files += 1
            
        json_percentage = (json_files / total_files) * 100
        if json_percentage >= 95:
            return True
        else:
            return False
        
        
    def _scan_directory_and_update_db(self, root_directory, cik, symbol):
        """
        **Step 1: Pre-fetch Data from the Database**
        Scan directory and update metadata in PostgreSQL to identify new or changed files.
        """
        # if not os.path.exists(cik_path): ## Some distrinctive flag
            # cik_path =self.in_memory_directory(symbol_path)

        print(f"[file_aggregator] Processing parquet: {cik}: {symbol}")
        symbol = symbol.lower()
        cik_path = os.path.join(root_directory, cik)
        symbol_path = os.path.join(root_directory, symbol)

        isJson_flag = False
        
        session = self.SessionLocal()
        newly_added_or_changed = []

        try:
            if self.isJson(root_directory):
                isJson_flag = True
                # Gather all files in the directory
                all_files = self.in_memory_directory(symbol_path)
            else:
                # Gather all files in the directory
                all_files = [
                    os.path.join(cik_path, f)
                    for f in os.listdir(cik_path)
                    if os.path.isfile(os.path.join(cik_path, f)) and not f.endswith(".DS_Store")
                ]

            # Fetch metadata from PostgreSQL
            db_files_deleted = session.query(FileMetadata).filter(FileMetadata.is_deleted == True, FileMetadata.cik == cik).all()
            db_files_deleted_map = {record.file_path: record for record in db_files_deleted}
            db_files = session.query(FileMetadata).filter(FileMetadata.is_deleted == False, FileMetadata.cik == cik).all()
            db_file_map = {record.file_path: record for record in db_files}

            # Detect new and updated files
            for file_path in all_files:
                file_hash = self.compute_file_hash(file_path)
                last_modified = self.get_file_modified_time(file_path)
                existing_record = db_file_map.get(file_path)
                existing_record_deleted = db_files_deleted_map.get(file_path)
                if existing_record or existing_record_deleted:
                    if existing_record:
                        # Update existing record instead of inserting
                        existing_record.file_hash = file_hash
                        existing_record.last_modified = last_modified
                    else:
                        existing_record_deleted.file_hash = file_hash
                        existing_record_deleted.last_modified = last_modified
                        existing_record_deleted.is_deleted = False                
        
                else:
                # if file_path not in db_file_map:
                    # New file
                    new_record = FileMetadata(
                        file_path=file_path,
                        last_modified=last_modified,
                        file_hash=file_hash,
                        is_deleted=False,
                        cik = cik
                    )

                    session.add(new_record)
                    newly_added_or_changed.append(file_path)

            # Mark deleted files
            db_files_cik = session.query(FileMetadata).filter(FileMetadata.is_deleted == False, FileMetadata.cik == cik).all()
            existing_files = set(all_files)

            for record in db_files_cik:
                if record.file_path not in existing_files:
                    record.is_deleted = True
            
            session.flush()
            session.commit()

        except Exception as e:
            session.rollback()
            if isJson_flag:
                logging.error(f"Error scanning directory {symbol_path}: {e}")
            else:
                logging.error(f"Error scanning directory {cik_path}: {e}")
        finally:
            session.close()
        

        return newly_added_or_changed
    
    def import_json(self, file_path):
        """Reads and parses a JSON file."""
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    def txt_processing(self, cik, symbol, file_directories):
        new_data = []
        for file_path in file_directories:
            file_name = os.path.basename(file_path)
            date_str = file_name.split('.')[0]
            body = self.import_file(file_path)
            new_data.append((symbol, cik, date_str, body))
            
        return new_data

    
    def json_processing(self, cik, symbol, file_directories):
        new_data = []
        for file_path in file_directories:
            json_content = self.import_json(file_path)
            if not json_content:
                continue
            attributes = json_content.get("data", {}).get("attributes", {})
            
            if attributes:
                body = attributes.get("content", {})
                date_str = attributes.get("publishOn", {})
                date_str = date_str[:10]
                new_data.append((symbol, cik, date_str, body))
            else:
                body = json_content.get("transcript", {})
                date_str = json_content.get("date", {})
                date_str = date_str[:10]
                new_data.append((symbol, cik, date_str, body))
            
        return new_data
    
    def json_processing_summary(self, cik, symbol, file_directories):
        new_data = []
        for file_path in file_directories:
            json_content = self.import_json(file_path)
            if not json_content:
                continue
            attributes = json_content.get("data", {}).get("attributes", {})
            transcript = json_content.get("transcript", "")
            if attributes:
                summary_list = attributes.get("summary", [])
                if isinstance(summary_list, list):
                    body = " ".join(summary_list)
                else:
                    body = str(summary_list)
                    
                if body.strip():
                    date_str = attributes.get("publishOn", "")
                    date_str = date_str[:10] if date_str else ""
                    new_data.append((symbol, cik, date_str, body))


            if transcript:
                body = transcript
                if body.strip():
                    date_str = json_content.get("date", "")
                    date_str = date_str[:10] if date_str else ""
                    new_data.append((symbol, cik, date_str, body))
            
        return new_data

    
    # ------------------- file_aggregator (Main Entry) ------------------- #

    def file_aggregator(self):
        """
        Build parquet files for each parquet using Spark, detecting changes via DB metadata.
        Only process & write out parquet for newly added or changed files.
        Runs `_scan_directory_and_update_db` in parallel using ThreadPoolExecutor.
        """
        available_cores = multiprocessing.cpu_count()
        num_threads = min(available_cores, len(self.firms_dict))  # Limit threads to number of CIKs

        # Step 1: Run `_scan_directory_and_update_db` in parallel
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_cik = {
                executor.submit(self._scan_directory_and_update_db, self.data_folder, cik, symbol): (cik, symbol)
                for cik, symbol in self.firms_dict.items()
            }

            # Collect results
            scan_results = {}
            for future in as_completed(future_to_cik):
                cik, symbol = future_to_cik[future]
                try:
                    changed_files = future.result()
                    scan_results[(cik, symbol)] = changed_files
                except Exception as e:
                    print(f"[Error] Failed scanning CIK {cik}: {e}")
                    scan_results[(cik, symbol)] = []

        # Step 2: Process each firm sequentially after scanning
        for (cik, symbol), changed_files in scan_results.items():
            if not changed_files:
                print(f"[file_aggregator] No new or changed files for parquet: {cik}")
                continue

            if self.isJson(self.data_folder):
                new_data = self.json_processing(cik, symbol, changed_files)
            else:
                new_data = self.txt_processing(cik, symbol, changed_files)
                
            if not new_data:
                continue

            new_df = self.spark.createDataFrame(new_data, schema=self.columns)
            new_df = new_df.dropna(how="all", subset=new_df.columns)
            new_df = new_df.select(["Name", "CIK", "Date", "Body"])
            new_df = new_df.orderBy(col("Date"))  # Sort if needed

            output_path = os.path.join(self.output_folder, cik)
            new_df.coalesce(1).write.parquet(output_path, mode="append")

        print(f"[file_aggregator] parquet files saved/updated in: {self.output_folder}")
    

    def aggregate_data(self, files_path, firms_dict):
        firms_ciks = list(firms_dict.keys())
        folder = 'company_df'
        folder_path = os.path.join(files_path, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)
        
        csv_files = []
        for cik in firms_ciks:
            cik_folder = os.path.join(folder_path, cik)
            if not os.path.exists(cik_folder):
                print(f"No folder found for CIK: {cik}")
                continue
            files = [f for f in os.listdir(cik_folder) if f.endswith('.csv')]
            if not files:
                print(f"No CSV files found for CIK: {cik}")
                continue
            for file in tqdm(files):
                if file.endswith('.csv'):
                    csv_files.append(pd.read_csv(os.path.join(folder_path, file)))# Read all CSV files in the folder
        
        if csv_files:
            merged = pd.concat(csv_files).reset_index(drop=True)
            merged.to_csv(os.path.join(files_path, "SP500.csv"), index=False)
            print(f"Aggregated CSV written to {os.path.join(files_path, 'SP500.csv')}")
        else:
            print("No CSV files found to aggregate.")
            

        
                
    def process_filings_for_cik_spark(self, save_folder, start_date, end_date, firms_csv_file_path):
        """
        Orchestrates the processing of CIK files:
        1. Distributes tasks to Spark workers using run_process_for_cik.
        2. Collects metadata and Parquet file paths returned by workers.
        3. Optionally merges Parquet files for centralized output.
        """

        # Enable case sensitivity in Spark
        self.spark.conf.set("spark.sql.caseSensitive", "true")
        
        folder = 'company_df'
        folder_path = os.path.join(self.save_folder, folder)
        os.makedirs(folder_path, exist_ok=True)

        firms_ciks = list(self.firms_dict.keys())
        # Batch process adding and updating metadata in the database

        # 1) Distribute tasks to workers
        # rdd = self.spark.sparkContext.parallelize(firms_ciks)

        db_url = "postgresql://pdcm:pdcm@pdcmmetastore_container:5432/pdcm"
        
        results = []
        for cik in firms_ciks:
            result = run_process_for_cik(
                cik,
                save_folder,
                folder_path,
                start_date,
                end_date,
                db_url,
                firms_csv_file_path
            )
            results.append(result)
        print('results', results)


        # results = rdd.map(lambda cik: run_process_for_cik(
        #     cik,
        #     save_folder,
        #     folder_path,
        #     start_date,
        #     end_date,
        #     db_url,
        #     firms_csv_file_path
        # )).collect()
        # print('results', results)

        # Driver side: gather output file paths
        updated_files_paths = []
        for result in results:
            output_file = result["output_file"]
            if output_file:
                updated_files_paths.append(output_file)

        return updated_files_paths
    @staticmethod
    def convert_timestamps_to_ms(table):
        schema = table.schema
        new_columns = []
        for column_name in table.column_names:
            field = schema.field(column_name)
            column = table[column_name]
            # Check if column is a TIMESTAMP type
            if pa.types.is_timestamp(field.type):
                # Downcast to milliseconds
                column = pa.compute.cast(column, pa.timestamp('ms'))
            new_columns.append(column)
        return pa.Table.from_arrays(new_columns, schema=schema)
    

    def multi_stage_parquet_merge(self, save_path, start_date, end_date, batch_size=50):
        from vol_reader_fun import vol_reader2
        """
        Multi-stage merge of Parquet files to avoid loading everything into memory at once.
        1) Splits the file_paths into batches.
        2) Reads each batch, combines, writes an intermediate Parquet file.
        3) Reads all intermediate Parquet files to create a final merged table.
        
        Returns:
            final_table: pyarrow.Table (None if no data)
            empty_ciks: list of CIKs from any empty Parquet file encountered
        """
        intermedate_folder = os.path.join(save_path, 'intermediate')
        os.makedirs(intermedate_folder, exist_ok=True)
        
        # Store paths to intermediate Parquet files 
        intermediate_file_paths = []
        empty_ciks = []
        
        # --- FIRST PASS : Process in Batches ---

        # Helper to chunk the file_paths
        def chunker(seq, size):
            for pos in range(0, len(seq), size):
                yield seq[pos:pos + size]
        # print('file_path', file_paths)
        
        existing_files_path = os.path.join(save_path, 'processed')
        os.makedirs(existing_files_path, exist_ok=True)
        existing_files = os.listdir(existing_files_path)
        existing_files = [f for f in existing_files if f != '.DS_Store']
        existing_files = [os.path.join(existing_files_path, f) for f in existing_files]
        
        for chunk_index, chunk in enumerate(chunker(existing_files, batch_size)):
            tables_in_this_batch = []
            batch_ciks = []
            # Read each file in the chunk
            for file_path in chunk:
                table = pq.read_table(file_path)
                pattern = r"dtm_(\d{10})\.parquet"
                # Check if empty
                if table.num_rows == 0:
                    # Gather empty CIK from filename if possible
                    match = re.search(pattern, file_path)
                    if match:
                        empty_ciks.append(match.group(1))
                    continue
                # Gather CIK from filename if possible
                match = re.search(pattern, file_path)
                if match:
                    batch_ciks.append(match.group(1))
                    
                # Convert timestamps to milliseconds (for Spark compatibility)
                table = self.convert_timestamps_to_ms(table)
                tables_in_this_batch.append(table)
                
            # If we have any data for this batch, Write an intermediate Parquet file while preprocessing
            # Generate 3-day rolling returns and volatilities for the batch of CIKs
            if tables_in_this_batch:
                batch_table = pa.concat_tables(tables_in_this_batch)
                batch_df = batch_table.to_pandas()
                columns_to_drop = ['form', 'table', 'content', 'heading']
                batch_df = batch_df.drop(columns=columns_to_drop, errors='ignore')
                batch_df = batch_df.drop_duplicates(subset=["Date", "_cik", "_vol", "_ret"]).fillna(0.0)
                
                # Generate 3-day rolling returns and volatilities for the provided firms' CIKs
                x1, x2 = vol_reader2(batch_ciks, self.firms_dict, start_date, end_date, window=3, extra_end=True, extra_start=True)

                # Shift the data by one time step to align with the desired time window
                x1 = x1.shift(1)
                x2 = x2.shift(1)

                # Slice the data to only include values within the start_date and end_date range
                x1 = x1[start_date:end_date]
                x2 = x2[start_date:end_date]

                # Initialize a flag to indicate the first iteration for appending data
                first = True

                loop_counter = 0
                # Loop through each firm CIK to align and merge 3-day rolling return/volatility with the main DataFrame
                for cik in batch_ciks:
                    loop_counter += 1
                    print(f'Aligning with 3-day return/volatility {cik} & the number of documents aggreated: {loop_counter}')
                    
                    # Extract the 3-day rolling return and volatility data for the current CIK
                    x1c = x1[cik]
                    x2c = x2[cik]
                    
                    # Concatenate the return and volatility data into a single DataFrame
                    x = pd.concat([x1c, x2c], axis=1)
                    x.columns = ['n_ret', 'n_vol']  # Rename columns for clarity
                    
                    # Filter rows in the combined DataFrame corresponding to the current CIK
                    y = batch_df[batch_df['_cik'] == cik]
                    
                    # Set 'Date' as the index for both the main DataFrame and the return/volatility DataFrame
                    y.set_index('Date', inplace=True)
                    x.index = pd.to_datetime(x.index)
                    y.index = pd.to_datetime(y.index)
                    
                    # Join the return/volatility data (`x`) with the filtered DataFrame (`y`) on the 'Date' index
                    z = y.join(x)
                    
                    # Extract only the return and volatility columns from the joined data
                    zz = z[['n_ret', 'n_vol']]
                    
                    # On the first iteration, initialize the combined DataFrame; otherwise, append to it
                    if first:
                        df_add = zz
                        first = False
                    else:
                        df_add = pd.concat([df_add, zz], axis=0)

                # Reset the index of the combined DataFrame to include 'Date' as a regular column
                df_add.reset_index(inplace=True)
                batch_df.reset_index(drop=True, inplace=True)


                # Ensure that the index of the combined DataFrame matches the original `batch_df`
                assert all(batch_df.index == df_add.index), 'Do not merge!'

                # Make a copy of the original `batch_df` to prevent modifications to the original
                batch_df = batch_df.copy()

                # Add new columns for the 3-day rolling return and volatility to the combined DataFrame
                batch_df['_ret'] = df_add['n_ret']
                batch_df['_vol'] = df_add['n_vol']

            # Write intermediate file to disk
            intermediate_file_path = os.path.join(intermedate_folder, f"batch_{chunk_index}.parquet")
            batch_table = pa.Table.from_pandas(batch_df)
            pq.write_table(batch_table, intermediate_file_path)
            intermediate_file_paths.append(intermediate_file_path)
                    
        
        return intermediate_file_paths
    
    def convert_timestamps_to_strings(self, df_path):
        df = pd.read_parquet(df_path)
        # Convert timestamp columns to strings
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = df[col].astype(str)
        df.to_parquet(df_path, index=False)

    
    def concatenate_parquet_files(self, save_path, total_constituents_path, constituents_metadata_path, start_date, end_date):
        """
        Concatenate all intermediate Parquet files into a single Parquet file.
        """
        # Uncomment them to concatenate files without modification
        # existing_files_path = os.path.join(save_path, 'intermediate')
        # existing_files = os.listdir(existing_files_path)
        # existing_files = [f for f in existing_files if f != '.DS_Store']
        # intermediate_file_paths = [os.path.join(existing_files_path, f) for f in existing_files]
        
        intermediate_file_paths = self.multi_stage_parquet_merge(save_path, start_date, end_date)
        filtered_paths = []
        for file_path in intermediate_file_paths:
            file_path = self.filter_sp500(save_path, file_path, total_constituents_path, constituents_metadata_path)
            self.convert_timestamps_to_strings(file_path)
            filtered_paths.append(file_path)
                
        self.spark.conf.set("spark.sql.caseSensitive", "true")
        df = self.spark.read.parquet(*filtered_paths)
        df = df.fillna(0.0)
        dtm_save_path = os.path.join(save_path, 'dtm')
        os.makedirs(dtm_save_path, exist_ok=True)
        df.coalesce(1).write.parquet(dtm_save_path, mode='overwrite')
        print(f"Combined Parquet file saved to {save_path}")
        
    def filter_sp500(self, save_folder, file_path, total_constituents_path, constituents_metadata_path):
        """
        Filter out the SP500 whose firms are not active each year   
        """
        # hard-coded years where you are interested in
        start = 2006
        end = 2023
        #Temp
        match = re.search(r'batch_(\d+)', file_path)
        batch_number = int(match.group(1))
        save_folder = os.path.join(save_folder, 'filtered')
        os.makedirs(save_folder, exist_ok=True)
        

        # Load the data
        # Assuming sp500_constituents.csv has columns: 'Firm', 'EntryDate', 'ExitDate'
        df = pd.read_csv(constituents_metadata_path)

        # Convert date columns to datetime format
        df['start'] = pd.to_datetime(df['start'], errors='coerce')
        df['ending'] = pd.to_datetime(df['ending'], errors='coerce')
        df['nameendt'] = pd.to_datetime(df['nameendt'], errors='coerce')
        
        # Define the range of years we are interested in
        years = range(start, end + 1)

        # Dictionary to hold the yearly snapshots
        sp500_by_year = {}
        permno_to_ticker = {}
        for year in years:
            # Define the start and end of each year
            start_of_year = datetime.datetime(year, 1, 1)
            end_of_year = datetime.datetime(year, 12, 31)
            
            # Filter firms active during the year, ensuring they only appear once per year by permno
            active_firms = df[
                (df['start'] <= end_of_year) & 
                ((df['ending'].isna()) | (df['ending'] >= start_of_year))
            ]
            
            # Get the last entry(nameendt) for each permno, and remove permno duplicate except the last entry of active_firms
            active_firms = active_firms.sort_values(by=['permno', 'nameendt']).groupby('permno').last().reset_index()

            # Convert the resulting DataFrame of firms to a list of unique permnos
            permno_to_ticker = dict(zip(active_firms['permno'], active_firms['ticker']))

            # Store the list of active firms for the year
            sp500_by_year[year] = permno_to_ticker
            
        # Get CIK constituents for the SP500 from local cik meta data
        sp500_ciks_df = pd.read_csv(total_constituents_path)
        
        # Change pernmo to CIK
        reversed_dict = {}
        for year, firms in sp500_by_year.items():
            reversed_dict[year] = {ticker: pernmo for pernmo, ticker in sp500_by_year[year].items()}
            for _, ticker in sp500_by_year[year].items():
                if ticker in sp500_ciks_df["Symbol"].tolist():
                    cik = sp500_ciks_df[sp500_ciks_df["Symbol"] == ticker]["CIK"].values[0]
                    reversed_dict[year][ticker] = cik
        sp500_by_year = reversed_dict.copy()
        
        # Load the intermediate Parquet file to be filtered 
        sp500_dtm = pd.read_parquet(file_path)
        sp500_dtm["Date"] = pd.to_datetime(sp500_dtm["Date"], errors="coerce") 
        sp500_dtm['Year'] = sp500_dtm["Date"].dt.year

        # Create a set of valid CIK-Year pairs
        valid_pairs = set()
        for year, firms in sp500_by_year.items():
            for cik in firms.values():
                cik = str(cik).zfill(10)
                valid_pairs.add((year, cik))
        valid_years = [year for year, _ in valid_pairs]
        # Filter out the data based on valid CIK-Year pairs
        df_filtered = sp500_dtm[
            (~sp500_dtm["Year"].isin(valid_years)) |  # Keep rows where Year is not in valid_pairs
            (sp500_dtm.apply(lambda row: (row["Year"], row["_cik"]) in valid_pairs, axis=1))  # Filter only if Year exists in valid_pairs
        ]
        df_filtered = df_filtered.drop(columns=["Year"])
        save_folder = os.path.join(save_folder, f"batch_filtered_{batch_number}.parquet")
        df_filtered.to_parquet(save_folder, index=False)
        print(f"Filtered data saved to {save_folder}")
        return save_folder
        
