#!/usr/bin/env python3
"""
Sync dataset documentation (README.md and manifest files) from S3 file server to database.

This script fetches README.md and manifest files from the S3 filestore
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
import os
import sys

import boto3
import dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from dataio.api.services.base_service import get_aws_access_key_id
from dataio.api.services.dataset_documentation_sync_service import (
    get_dataset_documentation_status,
    sync_dataset_documentation,
)

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
        aws_access_key_id=get_aws_access_key_id(),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    s3 = session.resource("s3")
    bucket = s3.Bucket(os.getenv("AWS_BUCKET_NAME"))
    return bucket


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
        try:
            status = get_dataset_documentation_status(db_session, bucket, ds_id)
            result = sync_dataset_documentation(
                db_session,
                bucket,
                ds_id,
                dry_run=args.dry_run,
            )
        except Exception as e:
            db_session.rollback()
            result = {
                "ds_id": ds_id,
                "updated": False,
                "error": str(e),
                "has_remote_documentation": False,
            }
            status = {"changed_fields": []}
            logger.error(f"  Error: {e}")

        if "readme_md" in status["changed_fields"] or result.get("has_remote_documentation"):
            results["readme_found"] += 1
        if any(
            field in status["changed_fields"]
            for field in ("data_dictionary_json", "manifest_yaml", "manifest_json")
        ) or result.get("has_remote_documentation"):
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
