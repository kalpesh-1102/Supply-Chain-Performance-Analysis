# code for raw data uploading the data to mysql

import os
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

# Configuration

DB_CONFIG = {
    "username": "root",
    "password": "password",
    "host": "localhost",
    "database": "database_name"
}

chunk_size = 5000
log_file = os.path.join("logs", "ingestion_db.log")

# Logging setup

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Mysql engine

def get_mysql_engine():
    """Create and return a sqlalchemy engine"""
    try:
        connection_str = (
            f"mysql+pymysql://{DB_CONFIG['username']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        )
        engine = create_engine(connection_str)
        logging.info("MySQL engine created successfully!")
        return engine
    except Exception as e:
        logging.error(f"Failed to create MYSQL engine!{e}")
        raise


# Upload function

def upload_to_sql(file_path:str, table_name:str, engine, chunk_size:int=5000):
    #Upload a single csv file to mysql in chunks
    logging.info(f"Starting upload for file: {file_path} > Table: {table_name}")
    first_chunk = True
    try:
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk.to_sql(
                name = table_name,
                con = engine,
                index = False,
                if_exists='replace' if first_chunk else 'append'
            )

            if first_chunk:
                logging.info(f"Table {table_name} created")
                first_chunk = False
        logging.info(f"Successfully uploaded: {file_path} > Table: {table_name}")
    
    except Exception as e:
        logging.error(f"Failed to upload {file_path} > {table_name}: {e}")

# Processing Function

def process_all_csv(folder_path:str, engine, chunk_size:int = 5000):
    # Process all csv file in the given folder
    if not os.path.exists(folder_path):
        logging.error(f"Folder does not exist: {folder_path}")
        return
    
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

    if not csv_files:
        logging.warning(f"No csv file found in the folder: {folder_path}")
        return
    
    for file in csv_files:
        table_name = os.path.splitext(file)[0]
        file_path = os.path.join(folder_path,file)
        upload_to_sql(file_path, table_name, engine, chunk_size)


# Main Function

def main():
    logging.info("CSV to mysql ingestion process started.")
    try:
        engine = get_mysql_engine()
        process_all_csv('data',engine,chunk_size)
        logging.info(f"All files processed successfully.")

    except Exception as e:
        logging.critical(f"Ingestion process failed! : {e}")

# Entry point 
if __name__ == "__main__":
    main()





