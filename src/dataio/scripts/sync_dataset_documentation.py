#!/usr/bin/env python3
"""
Sync dataset documentation (README.md and metadata.yaml) from S3 file server to database.

This script fetches README.md and metadata.yaml files from the S3 filestore
and caches their contents in the datasets table for faster access.

Usage:
    # Sync all datasets
    uv run python -m dataio.scripts.sync_dataset_documentation

    # Sync specific dataset
    uv run python -m dataio.scripts.sync_dataset_documentation --dataset DS_EXAMPLE01

    # Dry run (show what would be synced)
    uv run python -m dataio.scripts.sync_dataset_documentation --dry-run
"""

import argparse
import logging
from datetime import datetime
from typing import Optional
import os
import sys

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_database_url() -> str:
    """Build database URL from environment variables."""
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")
    database = os.getenv("DB_NAME", "catalogue")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def get_s3_client():
    """Initialize S3 client."""
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    s3 = session.resource("s3")
    bucket = s3.Bucket(os.getenv("AWS_BUCKET_NAME"))
    return bucket


def fetch_file_from_s3(bucket, dataset_id: str, filename: str) -> Optional[str]:
    """
    Fetch a file from S3 for a dataset.

    Looks in both STANDARDISED and PREPROCESSED versions.
    Returns the file content as string, or None if not found.
    """
    for version_type in ["STANDARDISED", "PREPROCESSED"]:
        key = f"filestore/{version_type}/{dataset_id}/{filename}"
        try:
            obj = bucket.Object(key)
            content = obj.get()["Body"].read().decode("utf-8")
            logger.debug(f"Found {filename} at {key}")
            return content
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                continue
            logger.warning(f"Error fetching {key}: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error fetching {key}: {e}")

    return None


def sync_dataset_documentation(
    db_session,
    bucket,
    dataset_id: str,
    dry_run: bool = False
) -> dict:
    """
    Sync documentation for a single dataset.

    Returns dict with sync results.
    """
    result = {
        "ds_id": dataset_id,
        "readme_found": False,
        "data_dictionary_found": False,
        "updated": False,
        "error": None,
    }

    try:
        # Fetch README.md
        readme_content = fetch_file_from_s3(bucket, dataset_id, "README.md")
        if readme_content:
            result["readme_found"] = True
            logger.info(f"  Found README.md ({len(readme_content)} chars)")

        # Fetch metadata.yaml (data dictionary)
        data_dict_content = fetch_file_from_s3(bucket, dataset_id, "metadata.yaml")
        if data_dict_content:
            result["data_dictionary_found"] = True
            logger.info(f"  Found metadata.yaml ({len(data_dict_content)} chars)")

        # Update database if any content found
        if readme_content or data_dict_content:
            if not dry_run:
                update_query = text("""
                    UPDATE datasets
                    SET readme_md = :readme,
                        data_dictionary_yaml = :data_dict,
                        documentation_synced_at = :synced_at
                    WHERE ds_id = :ds_id
                """)
                db_session.execute(update_query, {
                    "readme": readme_content,
                    "data_dict": data_dict_content,
                    "synced_at": datetime.utcnow(),
                    "ds_id": dataset_id,
                })
                db_session.commit()
                result["updated"] = True
                logger.info(f"  Updated database")
            else:
                logger.info(f"  [DRY RUN] Would update database")
                result["updated"] = True
        else:
            logger.info(f"  No documentation files found")

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"  Error: {e}")
        db_session.rollback()

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Sync dataset documentation from S3 to database"
    )
    parser.add_argument(
        "--dataset", "-d",
        help="Sync only this dataset ID (e.g., DS_EXAMPLE01)"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Show what would be synced without making changes"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize database connection
    logger.info("Connecting to database...")
    engine = create_engine(get_database_url())
    Session = sessionmaker(bind=engine)
    db_session = Session()

    # Initialize S3 client
    logger.info("Connecting to S3...")
    try:
        bucket = get_s3_client()
    except Exception as e:
        logger.error(f"Failed to connect to S3: {e}")
        sys.exit(1)

    # Get datasets to sync
    if args.dataset:
        # Sync specific dataset
        datasets = [(args.dataset,)]
        logger.info(f"Syncing documentation for dataset: {args.dataset}")
    else:
        # Get all dataset IDs
        result = db_session.execute(text("SELECT ds_id FROM datasets ORDER BY ds_id"))
        datasets = result.fetchall()
        logger.info(f"Found {len(datasets)} datasets to sync")

    if args.dry_run:
        logger.info("=== DRY RUN MODE ===")

    # Sync each dataset
    results = {
        "total": len(datasets),
        "readme_found": 0,
        "data_dict_found": 0,
        "updated": 0,
        "errors": 0,
    }

    for (ds_id,) in datasets:
        logger.info(f"Processing {ds_id}...")
        result = sync_dataset_documentation(db_session, bucket, ds_id, args.dry_run)

        if result["readme_found"]:
            results["readme_found"] += 1
        if result["data_dictionary_found"]:
            results["data_dict_found"] += 1
        if result["updated"]:
            results["updated"] += 1
        if result["error"]:
            results["errors"] += 1

    # Print summary
    logger.info("=" * 50)
    logger.info("SYNC SUMMARY")
    logger.info(f"  Total datasets:       {results['total']}")
    logger.info(f"  READMEs found:        {results['readme_found']}")
    logger.info(f"  Data dictionaries:    {results['data_dict_found']}")
    logger.info(f"  Datasets updated:     {results['updated']}")
    logger.info(f"  Errors:               {results['errors']}")

    db_session.close()

    if results["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
