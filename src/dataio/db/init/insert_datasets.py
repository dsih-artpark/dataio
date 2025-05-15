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

def insert_datasets():
    """Insert datasets from ARTPARK Data Catalogue into the database."""
    conn = connect_to_db()
    cur = conn.cursor()
    
    try:
        # Read and process the CSV file
        with open('data_inserts/ARTPARK Data Catalogue - Catalogue v2_filtered.csv', 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                try:
                    # Get raw dataset IDs
                    if row['RDS ID (Stable)'] != '':
                        rds_ids = [rds_id.strip() for rds_id in row['RDS ID (Stable)'].split(',')]
                    else:
                        rds_ids = '{}'
                    # Get tag names if available
                    tag_names = '{}'

                    if len(row['DS ID (Stable)']) != 12:
                        print(f"Error: DS ID {row['DS ID (Stable)']} is not 12 characters long")
                        continue

                    # Call the add_dataset function
                    cur.execute(
                        """
                        SELECT add_dataset(
                            %s, -- raw_dataset_ids
                            %s, -- ds_id
                            %s, -- title
                            %s, -- collection_name
                            %s, -- data_owner_name
                            %s, -- concept_name
                            %s, -- description
                            %s, -- tag_names
                            %s, -- spatial_coverage
                            %s, -- spatial_resolution
                            %s, -- temporal_coverage
                            %s, -- temporal_resolution
                            %s, -- public_access_level
                            %s, -- notes
                            %s  -- supplementary_documents
                        )
                        """,
                        (
                            rds_ids,
                            row['DS ID (Stable)'],
                            row['Dataset Title'],
                            row['Collection'],
                            row['Data Owner'],
                            row['Concept Name'],
                            row['Contents'],
                            tag_names,
                            row['Spatial Coverage'],
                            row['Spatial Resolution'],
                            row['Temporal Coverage'],
                            row['Temporal Resolution'],
                            'VIEW' if row['Access Type'] == 'Public' else 'NONE',
                            row['Notes'],
                            row['Supplementary Documents']
                        )
                    )
                    print(f"Successfully inserted dataset: {row['DS ID (Stable)']}")
                except Exception as e:
                    print(f"Error inserting dataset {row['DS ID (Stable)']}: {e}")
                    continue
        
        # Commit the transaction
        conn.commit()
        print("All datasets inserted successfully!")
        
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    insert_datasets() 