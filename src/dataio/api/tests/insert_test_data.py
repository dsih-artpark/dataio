#!/usr/bin/env python3
"""
Simplified test data insertion script for permission system testing.
This script creates the basic test data defined in README.md.
"""

import os
import csv
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
import secrets
import bcrypt
import argparse

register_adapter(dict, Json)

# Load environment variables from .env file
load_dotenv(override=True)

# Database connection parameters from .env
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
REPO_DIR = os.getenv("REPO_DIR")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Insert test data for permission testing"
    )
    parser.add_argument("--all", action="store_true", help="Insert all test data")
    parser.add_argument(
        "--clean", action="store_true", help="Clean existing test data before inserting"
    )
    return parser.parse_args()


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


def generate_user(email, is_admin=False):
    """Generate a user with hashed and unhashed keys."""
    key = secrets.token_urlsafe()
    bytes = key.encode("utf-8")
    salt = bcrypt.gensalt()
    hash = bcrypt.hashpw(bytes, salt)
    return {
        "email": email,
        "key": hash,
        "is_group": False,
        "unhashed_key": key,
        "is_admin": is_admin,
    }


def clean_test_data():
    """Clean all existing data."""
    conn = connect_to_db()
    cur = conn.cursor()

    print("Cleaning all existing data...")

    # Truncate all tables in reverse dependency order
    truncate_queries = [
        "TRUNCATE TABLE user_permissions RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE user_groups RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE resource_group_members RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE resource_groups RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dataset_tags RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dataset_raw_datasets RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE datasets RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE collections RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE data_owners RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE raw_datasets RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE tags RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE regions RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE users RESTART IDENTITY CASCADE",
    ]

    for query in truncate_queries:
        try:
            cur.execute(query)
            print(f"Successfully truncated: {query.split()[2]}")
        except Exception as e:
            print(f"Warning: Could not truncate table - {e}")

    conn.commit()
    cur.close()
    conn.close()
    print("All data cleaned successfully!")


def insert_test_data():
    """Insert all test data."""
    conn = connect_to_db()
    cur = conn.cursor()

    print("Inserting test data...")

    # Insert data owners
    data_owners = [
        ("ARTPARK", "Admin User", "admin@artpark.in"),
        ("External Partner", "External Contact", "external@partner.org"),
        ("Public Agency", "Public Contact", "public@agency.gov"),
    ]

    for name, contact_person, contact_email in data_owners:
        cur.execute(
            """INSERT INTO data_owners (name, contact_person, contact_person_email) 
               VALUES (%s, %s, %s)""",
            (name, contact_person, contact_email),
        )

    # Insert collections
    collections = [
        ("TS0001", "Test Collection 1", "TESTCAT1", "Test Category 1"),
    ]

    for collection_id, collection_name, category_id, category_name in collections:
        cur.execute(
            """INSERT INTO collections (collection_id, collection_name, category_id, category_name) 
               VALUES (%s, %s, %s, %s)""",
            (collection_id, collection_name, category_id, category_name),
        )

    # Get collection and data owner IDs
    cur.execute("SELECT id FROM collections WHERE collection_id = 'TS0001'")
    collection_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM data_owners WHERE name = 'ARTPARK'")
    owner_id = cur.fetchone()[0]

    # Insert test users
    test_users = [
        generate_user("admin@artpark.in", is_admin=True),
        generate_user("analyst@artpark.in"),
        generate_user("public_user@artpark.in"),
        generate_user("external_collaborator@psu.edu"),
    ]

    for user in test_users:
        cur.execute(
            """INSERT INTO users (email, key, is_group, is_admin) 
               VALUES (%s, %s, %s, %s)""",
            (user["email"], user["key"], user["is_group"], user["is_admin"]),
        )

    # Insert test datasets with specific access levels
    datasets = [
        (
            "TS0001DS0001",
            "Public DOWNLOAD dataset",
            "DOWNLOAD",
            '{"tags": ["public", "download"]}',
        ),
        ("TS0001DS0002", "Public VIEW dataset", "VIEW", '{"tags": ["public", "view"]}'),
        ("TS0001DS0003", "Public VIEW dataset", "VIEW", '{"tags": ["public", "view"]}'),
        ("TS0001DS0004", "Public NONE dataset", "NONE", '{"tags": ["restricted"]}'),
        ("TS0001DS0005", "Admin-only dataset", "NONE", '{"tags": ["admin"]}'),
        (
            "TS0001DS0006",
            "Resource group dataset",
            "NONE",
            '{"tags": ["resource_group"]}',
        ),
        (
            "TS0001DS0007",
            "Resource group dataset",
            "NONE",
            '{"tags": ["resource_group"]}',
        ),
    ]

    for ds_id, title, access_level, metadata in datasets:
        cur.execute(
            """INSERT INTO datasets (ds_id, title, collection_id, data_owner_id, description, 
                                   spatial_resolution, temporal_resolution, access_level, additional_metadata) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                ds_id,
                title,
                collection_id,
                owner_id,
                f"Test dataset {ds_id}",
                "DISTRICT",
                "MONTH",
                access_level,
                metadata,
            ),
        )

    # Insert resource group
    cur.execute(
        """INSERT INTO resource_groups (resource_group_id, group_name) 
           VALUES (%s, %s)""",
        ("test_resource_group", "Test Resource Group"),
    )

    # Insert resource group members
    resource_members = [
        ("test_resource_group", "TS0001DS0006", "DATASET", '{"category": "test"}'),
        ("test_resource_group", "TS0001DS0007", "DATASET", '{"category": "test"}'),
    ]

    for group_id, resource_id, resource_type, resource_json in resource_members:
        cur.execute(
            """INSERT INTO resource_group_members (resource_group_id, resource_id, resource_type, resource_json) 
               VALUES (%s, %s, %s, %s)""",
            (group_id, resource_id, resource_type, resource_json),
        )

    # Insert individual permissions
    permissions = [
        # Analyst individual permissions
        ("analyst@artpark.in", "DATASET", "TS0001DS0002", "DOWNLOAD"),
        ("analyst@artpark.in", "DATASET", "TS0001DS0003", "DOWNLOAD"),
        ("analyst@artpark.in", "DATASET", "TS0001DS0004", "DOWNLOAD"),
        ("analyst@artpark.in", "GROUP", "test_resource_group", "DOWNLOAD"),
        # External collaborator individual permissions
        ("external_collaborator@psu.edu", "DATASET", "TS0001DS0003", "DOWNLOAD"),
        ("external_collaborator@psu.edu", "DATASET", "TS0001DS0004", "DOWNLOAD"),
    ]

    for user_email, resource_type, resource_id, permission in permissions:
        cur.execute(
            """INSERT INTO user_permissions (user_email, resource_type, resource_id, permission) 
               VALUES (%s, %s, %s, %s)""",
            (user_email, resource_type, resource_id, permission),
        )

    conn.commit()
    cur.close()
    conn.close()

    # Save unhashed keys for testing
    print("Saving unhashed keys for testing...")
    os.makedirs(f"{REPO_DIR}/api/tests", exist_ok=True)
    with open(f"{REPO_DIR}/api/tests/test_users.csv", "w") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=["email", "key", "is_group", "unhashed_key", "is_admin"]
        )
        writer.writeheader()
        writer.writerows(test_users)

    print("Test data inserted successfully!")


def verify_test_data():
    """Verify that test data was inserted correctly."""
    conn = connect_to_db()
    cur = conn.cursor()

    print("\nVerifying test data...")

    verification_queries = [
        ("Users", "SELECT COUNT(*) FROM users"),
        ("Datasets", "SELECT COUNT(*) FROM datasets"),
        ("Collections", "SELECT COUNT(*) FROM collections"),
        ("Data Owners", "SELECT COUNT(*) FROM data_owners"),
        ("User Permissions", "SELECT COUNT(*) FROM user_permissions"),
        ("Resource Groups", "SELECT COUNT(*) FROM resource_groups"),
        ("Resource Group Members", "SELECT COUNT(*) FROM resource_group_members"),
    ]

    for table_name, query in verification_queries:
        cur.execute(query)
        count = cur.fetchone()[0]
        print(f"{table_name}: {count} records")

    cur.close()
    conn.close()
    print("Test data verification completed!")


def main():
    args = parse_args()

    if args.clean:
        clean_test_data()

    if args.all:
        insert_test_data()
        verify_test_data()

    print("\nTest data setup completed!")
    print("Use the generated test_users.csv file to get unhashed API keys for testing.")


if __name__ == "__main__":
    main()
