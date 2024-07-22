import pandas as pd
import logging
from sqlalchemy import create_engine # type: ignore
import os

# Set up logging to output detailed information
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_sample_csv(file_path):
    """Create a sample CSV file with placeholder data."""
    sample_data = {
        'id': [1, 2, 3],
        'title': ['Movie A', 'Movie B', 'Movie C'],
        'year': [2020, 2021, 2022]
    }
    df = pd.DataFrame(sample_data)
    df.to_csv(file_path, index=False)
    logging.info(f"Created sample CSV file: {file_path}")

def extract_data(file_path):
    """Extract data from CSV file into a pandas DataFrame."""
    try:
        if not os.path.exists(file_path):
            logging.info(f"File not found: {file_path}. Creating file with sample data.")
            create_sample_csv(file_path)
        df = pd.read_csv(file_path)
        logging.info(f"Data extracted successfully from {file_path}")
        return df
    except FileNotFoundError as e:
        logging.error(e)
        raise
    except Exception as e:
        logging.error(f"Error occurred during data extraction: {e}")
        raise

def load_data(df, db_path, table_name):
    """Load DataFrame into SQLite database."""
    try:
        db_dir = os.path.dirname(db_path.replace('sqlite:///', ''))
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            logging.info(f"Created database directory: {db_dir}")
        engine = create_engine(db_path)
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Data loaded successfully into table '{table_name}'")
    except Exception as e:
        logging.error(f"Error occurred during data loading: {e}")
        raise

if __name__ == "__main__":
    file_path = 'C:\\Users\\hurri\\OneDrive\\Desktop\\Data Engineering Projects\\data-engineering-project\\data\\movies.csv'
    db_path = 'sqlite:///C:\\Users\\hurri\\OneDrive\\Desktop\\Data Engineering Projects\\data-engineering-project\\data\\database.db'
    table_name = 'movies'

    try:
        logging.info("ETL process started.")
        data = extract_data(file_path)
        load_data(data, db_path, table_name)
        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred during ETL process: {e}")
