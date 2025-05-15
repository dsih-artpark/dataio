import os
import csv
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection parameters from .env
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

def connect_to_db():
    """Establish connection to the database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def insert_raw_datasets():
    """Insert raw datasets from TSV into the database."""
    conn = connect_to_db()
    cur = conn.cursor()
    
    try:
        # Read and process the TSV file
        with open('data_inserts/raw_datasets.tsv', 'r') as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t')
            
            for row in reader:
                try:
                    # Call the add_raw_dataset function
                    cur.execute(
                        "SELECT add_raw_dataset(%s, %s, %s, %s)",
                        (
                            row['rds_id'],
                            row['title'],
                            row['source'],
                            row['data_owner_name']
                        )
                    )
                    print(f"Successfully inserted dataset: {row['rds_id']}")
                except Exception as e:
                    print(f"Error inserting dataset {row['rds_id']}: {e}")
                    continue
        
        # Commit the transaction
        conn.commit()
        print("All datasets inserted successfully!")
        
    except Exception as e:
        print(f"Error processing TSV file: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    insert_raw_datasets()
