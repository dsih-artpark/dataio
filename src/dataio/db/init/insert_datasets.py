import os
import csv
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
import secrets
import bcrypt

register_adapter(dict, Json)

# Load environment variables from .env file
load_dotenv()

# Database connection parameters from .env
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
REPO_DIR = os.getenv("REPO_DIR")


def connect_to_db():
    """Establish connection to the database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
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
        # with open(
        #     f"{REPO_DIR}/db/init/data_inserts/ARTPARK Data Catalogue - Catalogue v2_filtered.csv",
        #     "r",
        # ) as csvfile:
        #     reader = csv.DictReader(csvfile)

        #     # Insert data owners into data_owners table
        #     for row in reader:
        #         if row["Data Owner"].strip() != "":
        #             data_owner_name = row["Data Owner"].strip()
        #             cur.execute(
        #                 """
        #                 INSERT INTO data_owners (name, contact_person, contact_person_email)
        #                 VALUES (%s, %s, %s)
        #                 ON CONFLICT DO NOTHING
        #                 """,
        #                 (data_owner_name, None, None),
        #             )
        #             print(f"Inserted data owner: {data_owner_name}")

        # # Insert raw datasets into raw_datasets table
        # with open(f"{REPO_DIR}/db/init/data_inserts/raw_datasets.tsv", "r") as tsvfile:
        #     reader = csv.DictReader(tsvfile, delimiter="\t")

        #     for row in reader:
        #         try:
        #             # Call the add_raw_dataset function
        #             cur.execute(
        #                 "SELECT add_raw_dataset(%s, %s, %s, %s)",
        #                 (
        #                     row["rds_id"],
        #                     row["title"],
        #                     row["source"],
        #                     row["data_owner_name"],
        #                 ),
        #             )
        #             print(f"Successfully inserted dataset: {row['rds_id']}")
        #         except Exception as e:
        #             print(f"Error inserting dataset {row['rds_id']}: {e}")
        #             continue

        # print("All raw datasets inserted successfully!")

        # with open(
        #     f"{REPO_DIR}/db/init/data_inserts/ARTPARK Data Catalogue - Catalogue v2_filtered.csv",
        #     "r",
        # ) as csvfile:
        #     reader = csv.DictReader(csvfile)
        #     # Insert collections into collections table
        #     for row in reader:
        #         if row["Collection"].strip() != "":
        #             collection_name = row["Collection"].strip()
        #             category_name = row["Category (auto-populated)"].strip()
        #             collection_id = row["DS ID (Stable)"].strip()[:6]
        #             category_id = row["DS ID (Stable)"].strip()[:2]
        #             cur.execute(
        #                 """
        #                 INSERT INTO collections (collection_id, collection_name, category_name, category_id)
        #                 VALUES (%s, %s, %s, %s)
        #                 ON CONFLICT DO NOTHING
        #                 """,
        #                 (collection_id, collection_name, category_name, category_id),
        #             )
        #             print(f"Inserted collection: {collection_name}")

        # with open(
        #     f"{REPO_DIR}/db/init/data_inserts/ARTPARK Data Catalogue - Catalogue v2_filtered.csv",
        #     "r",
        # ) as csvfile:
        #     reader = csv.DictReader(csvfile)
        #     # Insert tags into tags table
        #     for row in reader:
        #         if row["Concept Name"].strip() != "":
        #             tag_names = [tag.strip() for tag in row["Concept Name"].split(",")]
        #             for tag_name in tag_names:
        #                 cur.execute(
        #                     """
        #                     INSERT INTO tags (tag_name)
        #                     VALUES (%s)
        #                     ON CONFLICT DO NOTHING
        #                 """,
        #                     (tag_name,),
        #                 )
        #             print(f"Inserted tag: {tag_name}")

        # insert regionids.csv into regions table
        with open(f"{REPO_DIR}/db/init/data_inserts/regionids.csv", "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                region_type = row["regionID"].split("_")[0].upper()

                cur.execute(
                    """INSERT INTO regions (region_id, region_name, parent_region_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (row["regionID"], row["regionName"], row["parentID"]),
                )

        # with open(
        #     f"{REPO_DIR}/db/init/data_inserts/ARTPARK Data Catalogue - Catalogue v2_filtered.csv",
        #     "r",
        # ) as csvfile:
        #     reader = csv.DictReader(csvfile)
        #     for row in reader:
        #         try:
        #             # Get raw dataset IDs
        #             if row["RDS ID (Stable)"].strip() != "":
        #                 rds_ids = [
        #                     rds_id.strip()
        #                     for rds_id in row["RDS ID (Stable)"].split(",")
        #                 ]
        #             else:
        #                 rds_ids = "{}"
        #             # Get tag names if available
        #             if row["Concept Name"].strip() != "":
        #                 tag_names = [
        #                     tag.strip() for tag in row["Concept Name"].split(",")
        #                 ]
        #             else:
        #                 tag_names = "{}"

        #             if len(row["DS ID (Stable)"]) != 12:
        #                 print(
        #                     f"Error: DS ID {row['DS ID (Stable)']} is not 12 characters long"
        #                 )
        #                 continue

        #             # TODO: temporal coverage start date & end_date, spatial resolution, temporal resolution
        #             # Data needs to be cleaned up first

        #             temporal_coverage_start_date = None
        #             temporal_coverage_end_date = None
        #             spatial_resolution = None
        #             temporal_resolution = None
        #             spatial_coverage_region_id = None
        #             # Notes and supplementary documents to additional_metadata
        #             additional_metadata = {}

        #             if row["Notes"].strip() != "":
        #                 additional_metadata["notes"] = row["Notes"].strip()
        #             if row["Supplementary Documents"].strip() != "":
        #                 additional_metadata["supplementary_documents"] = row[
        #                     "Supplementary Documents"
        #                 ].strip()

        #             # Call the add_dataset function
        #             cur.execute(
        #                 """
        #                 SELECT add_dataset(
        #                     %s, -- raw_dataset_ids
        #                     %s, -- ds_id
        #                     %s, -- title
        #                     %s, -- collection_name
        #                     %s, -- tags
        #                     %s, -- data_owner_name
        #                     %s, -- description
        #                     %s, -- spatial_coverage_region_id
        #                     %s, -- spatial_resolution
        #                     %s, -- temporal_coverage_start_date
        #                     %s, -- temporal_coverage_end_date
        #                     %s, -- temporal_resolution
        #                     %s, -- access_level
        #                     %s  -- additional_metadata
        #                 )
        #                 """,
        #                 (
        #                     rds_ids,
        #                     row["DS ID (Stable)"],
        #                     row["Dataset Title"],
        #                     row["Collection"],
        #                     tag_names,
        #                     row["Data Owner"],
        #                     row["Contents"],
        #                     spatial_coverage_region_id,
        #                     spatial_resolution,
        #                     temporal_coverage_start_date,
        #                     temporal_coverage_end_date,
        #                     temporal_resolution,
        #                     "VIEW" if row["Access Type"] == "Public" else "NONE",
        #                     additional_metadata,
        #                 ),
        #             )
        #             print(f"Successfully inserted dataset: {row['DS ID (Stable)']}")
        #         except Exception as e:
        #             print(f"Error inserting dataset {row['DS ID (Stable)']}: {e}")
        #             continue

        # Commit the transaction
        conn.commit()
        print("Insertions Completed. View above logs for details.")

    except Exception as e:
        print(f"Error processing CSV file: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    insert_datasets()
