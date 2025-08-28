#!/bin/bash

source .env

# Database connection details - set these or use environment variables
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-catalogue}"
DB_USER="${DB_USER:-postgres}"

# Create local directory for dumps if it doesn't exist
mkdir -p "$(dirname "$LOCAL_SCHEMA_DUMP_LOCATION")"
mkdir -p "$(dirname "$LOCAL_DATA_DUMP_LOCATION")"

# Generate timestamp in ISO 8601 format
TIMESTAMP=$(date +"%Y-%m-%dT%H:%M:%S")

# Get the migrations directory from environment variables
MIGRATIONS_DIR="${MIGRATIONS_DIR:-${HOME}/dataio/src/dataio/db/migrations}"


# Ensure the directory exists
if [ ! -d "$MIGRATIONS_DIR" ]; then
  echo "Migrations directory does not exist: $MIGRATIONS_DIR. Please set the MIGRATIONS_DIR environment variable to the correct path."
  exit 1
fi

# Find the latest migration file number 
latest_migration=$(find "$MIGRATIONS_DIR" -name "*.sql" -type f | 
                  grep -E '^.*/[0-9]{3}_.*\.sql$' |
                  sort -V |
                  tail -n 1 |
                  sed -E 's/^.*\/([0-9]{3})_.*\.sql$/\1/')

# Check if we found any migration files
if [ -z "$latest_migration" ]; then
  echo "Error: No migration files found in $MIGRATIONS_DIR"
  exit 1
fi

# Convert to integer (remove leading zeros)
latest_version=$(echo "$latest_migration" | sed 's/^0*//')

# Format the schema version tag (ensuring 3 digits with leading zeros)
SCHEMA_VERSION="schema_v$(printf "%03d" "$latest_version")"

# Generate timestamp in ISO 8601 format
TIMESTAMP=$(date +"%Y-%m-%dT%H:%M:%S")

echo "Latest migration: $latest_migration"
echo "Schema version: $SCHEMA_VERSION"

# Dump schema and data separately       
echo "Dumping schema to $LOCAL_SCHEMA_DUMP_LOCATION..."
pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
  --schema-only --no-owner --no-acl > "$LOCAL_SCHEMA_DUMP_LOCATION"

echo "Dumping data to $LOCAL_DATA_DUMP_LOCATION..."
pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
  --data-only --no-owner --no-acl > "$LOCAL_DATA_DUMP_LOCATION"

# Copy schema to repository location if needed
cp "$LOCAL_SCHEMA_DUMP_LOCATION" "$REPO_SCHEMA_DUMP_LOCATION"

# Create S3 filenames with timestamps
S3_SCHEMA_FILENAME="schema.sql"
S3_DATA_FILENAME="data.sql"

# Upload to S3 with tags
# Extract bucket name and prefix from S3 URI
if [[ "$DB_DUMP_S3_URI" =~ s3://([^/]+)(.*) ]]; then
    S3_BUCKET="${BASH_REMATCH[1]}"
    S3_PREFIX="${BASH_REMATCH[2]}"
    # Remove leading slash from prefix if present
    S3_PREFIX="${S3_PREFIX#/}"
    S3_PREFIX="${S3_PREFIX%/}"
else
    echo "Error: Invalid S3 URI format. Expected s3://bucket/path"
    exit 1
fi

# Debug - print extracted values
echo "S3 URI: $DB_DUMP_S3_URI"
echo "Bucket: $S3_BUCKET"
echo "Prefix: $S3_PREFIX"

# Use these variables in the commands
echo "Uploading Schema to S3..."
aws s3api put-object \
  --bucket "$S3_BUCKET" \
  --key "${S3_PREFIX}/${SCHEMA_VERSION}/${S3_SCHEMA_FILENAME}" \
  --body "$LOCAL_SCHEMA_DUMP_LOCATION" \
  --tagging "schema_version=$SCHEMA_VERSION&timestamp=$TIMESTAMP" \
  --profile dataio-vm

echo "Uploading Data to S3..."
aws s3api put-object \
  --bucket "$S3_BUCKET" \
  --key "${S3_PREFIX}/${SCHEMA_VERSION}/${S3_DATA_FILENAME}" \
  --body "$LOCAL_DATA_DUMP_LOCATION" \
  --tagging "schema_version=$SCHEMA_VERSION&timestamp=$TIMESTAMP" --profile dataio-vm

# Verify uploads
echo "Verifying Tags on S3..."
echo "Schema: $S3_BUCKET/$S3_PREFIX/$S3_SCHEMA_FILENAME"
aws s3api get-object-tagging --bucket "$S3_BUCKET" --key "${S3_PREFIX}/${SCHEMA_VERSION}/${S3_SCHEMA_FILENAME}" --output table --profile dataio-vm
echo "Data: $S3_BUCKET/$S3_PREFIX/$S3_DATA_FILENAME"
aws s3api get-object-tagging --bucket "$S3_BUCKET" --key "${S3_PREFIX}/${SCHEMA_VERSION}/${S3_DATA_FILENAME}" --output table --profile dataio-vm