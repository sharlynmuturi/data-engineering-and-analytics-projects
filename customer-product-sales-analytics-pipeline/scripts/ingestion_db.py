import pandas as pd
import os
from sqlalchemy import create_engine
from pathlib import Path
import logging
import time

# Configuring logging to track ingestion process to capture info and error messages, append logs instead of overwriting
logging.basicConfig(
    filename='logs/sales.log',
    level=logging.DEBUG, 
    format='%(asctime)s %(levelname)s:%(message)s',
    filemode='a'
)

# Creating database engine (SQLite for lightweight storage)
engine = create_engine('sqlite:///sales.db')

def ingest_db(df, table_name, engine):
    """
    Loads DataFrame into a SQL table.
    Existing tables are replaced to ensure clean reloads.
    """
    df.to_sql(table_name, engine, if_exists='replace', index=False)


def load_raw_data():
    """
    Reads all CSV files from the data directory, skipping malformed rows safely
    ingests them into the database, and logs execution time.
    """
    start = time.time()

    for file in os.listdir('/content'):
        file_path = os.path.join('/content', file)
        if os.path.isfile(file_path) and file.endswith('.csv'):
            df = pd.read_csv(file_path, sep=';', encoding='latin1', on_bad_lines='skip')
            df.columns = df.columns.str.strip()

            logging.info(f'Ingesting {file} into database')
            ingest_db(df, file.replace('.csv', ''), engine)

    end = time.time()
    time_taken = (end - start) / 60

    logging.info('--------------- Ingestion Complete ----------------')
    logging.info(f'Time taken: {time_taken:.2f} minutes')

if __name__ == '__main__':
    load_raw_data()

