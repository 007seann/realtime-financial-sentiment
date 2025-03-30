
import hashlib
import datetime
import pandas as pd
import logging

from plugins.packages.PDCM.metadata import FileMetadata

import sys
import os
# Add the parent directory of hons_project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# print("Resolved path being added to sys.path:", os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Late imports of your FileMetadata model if needed



def compute_file_hash(file_path, chunk_size=65536):
    """Serializable helper function for computing file hash."""
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()

def get_file_modified_time(file_path):
    """Serializable helper function for getting file modification time."""
    epoch_time = os.path.getmtime(file_path)
    return datetime.datetime.fromtimestamp(epoch_time)


def _upsert_metadata(session, meta_dict, cik):
    """Helper to insert or update the FileMetadata row."""
    file_path = meta_dict["file_path"]
    record = session.query(FileMetadata).filter_by(file_path=file_path).first()
    if record:
        record.file_hash = meta_dict["file_hash"]
        record.last_modified = meta_dict["last_modified"]
        record.is_deleted = False
    else:
        new_record = FileMetadata(
            file_path=file_path,
            last_modified=meta_dict["last_modified"],
            file_hash=meta_dict["file_hash"],
            is_deleted=False,
            cik = cik
        )
        session.add(new_record)
    
def run_process_for_cik(cik, save_folder, folder_path, start_date, end_date, db_url, firms_csv_file_path):
    """
    Worker function invoked by Spark.
    1) Creates a SQLAlchemy session locally (no references to the driver session_maker).
    2) Checks PostgreSQL for file metadata for this CIK.
    3) Processes new/changed files, writes Parquet, updates metadata.
    """
    from packages.FTRM.annual_report_reader import reader
    from vol_reader_fun import vol_reader
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.exc import SQLAlchemyError


    engine = create_engine(db_url, echo=False)
    SessionLocal = sessionmaker(bind=engine)

    # # Load local firms data   

    firms_df = pd.read_csv(firms_csv_file_path, encoding='latin1')
    firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
    firms_dict = firms_df.set_index('Symbol')['CIK'].to_dict()
    firms_dict = {cik: symbol for symbol, cik in firms_dict.items()}

    # -- Build paths --
    cik_folder = os.path.join(folder_path, cik)
    if not os.path.exists(cik_folder):
        return {
            "cik": cik,
            "metadata": [],
            "output_file": None  # No data returned
        }

    session = SessionLocal()
    try:
        # 1. Fetch existing file metadata from PostgreSQL
        existing_files = session.query(FileMetadata).filter(FileMetadata.file_path.like(f"{cik_folder}%")).all()
        existing_metadata = {f.file_path: f for f in existing_files}
        # 2. Identify all parquet files
        all_files = [
            os.path.join(cik_folder, f)
            for f in os.listdir(cik_folder)
            if f.endswith('.parquet')
        ]

        metadata_records = []
        new_or_changed_files = []
        for file_path in all_files:
            file_hash = compute_file_hash(file_path)
            last_modified = get_file_modified_time(file_path)
            

            # If new or changed, mark for processing
            if (file_path not in existing_metadata
                or existing_metadata[file_path].file_hash != file_hash
                or existing_metadata[file_path].last_modified != last_modified):
                new_or_changed_files.append(file_path)

            metadata_records.append({
                "file_path": file_path,
                "file_hash": file_hash,
                "last_modified": last_modified
            })

        processed_dataframes = [
            result for file_path in new_or_changed_files
            if (result := reader(os.path.basename(file_path), file_loc=cik_folder)) is not None
        ]

        if not processed_dataframes:
            # No new data to write
            for m in metadata_records:
                _upsert_metadata(session, m, cik)
            session.commit()
            return {
                "cik": cik,
                "metadata": metadata_records,
                "output_file": None
            }
            
        # Combine and process the data
        combined = pd.concat(processed_dataframes)
        # Add volatility data
        vol_data = vol_reader(cik, firms_dict, start_date=start_date, end_date=end_date)
        combined.reset_index(inplace=True)
        combined = combined.copy()
        first_column_name = combined.columns[0]
        combined = pd.merge(combined.rename(columns={first_column_name: "Date"}), vol_data.reset_index(), how="inner", on="Date")
        
        # Add CIK column and reorder columns
        combined["_cik"] = cik
        columns_to_move = ['Date', '_cik', '_vol', '_ret', '_vol+1', '_ret+1']
        remaining_cols = [col for col in combined.columns if col not in columns_to_move]
        combined = combined[columns_to_move + remaining_cols]

        # Filter invalid rows and write to Parquet
        combined = combined[combined["_ret"].notna()]
        if processed_dataframes:
            # Update the existing dtm datasets
            existing_files = f"dtm_{cik}.parquet"
            existing_files_path = os.path.join(save_folder, 'processed', existing_files)
            if os.path.exists(existing_files_path):
                existing_files_df = pd.read_parquet(existing_files_path, engine='pyarrow')
                combined = pd.concat([existing_files_df, combined])
                combined.drop_duplicates(inplace=True)
                combined = combined.fillna(0.0)
            
        save_path = os.path.join(save_folder, 'processed')
        os.makedirs(save_path, exist_ok=True)
        out_file_path = os.path.join(save_path, f"dtm_{cik}.parquet")
        combined.to_parquet(out_file_path, index=False)

        # Update metadata
        for m in metadata_records:
            _upsert_metadata(session, m, cik)

        session.commit()

        return {
            "cik": cik,
            "metadata": metadata_records,
            "output_file": out_file_path
        }

    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[{cik}] DB Error: {e}")
        return {"cik": cik, "metadata": [], "output_file": None}
    except Exception as e:
        session.rollback()
        logging.error(f"[{cik}] Processing Error: {e}")
        return {"cik": cik, "metadata": [], "output_file": None}
    finally:
        session.close()
    
