#!/bin/bash

# Check if schema file path is provided
if [ $# -eq 0 ]; then
    echo "Error: Please provide the path to schema.sql"
    echo "Usage: $0 <path_to_schema.sql>"
    exit 1
fi

SCHEMA_PATH="$1"

# Check if schema file exists
if [ ! -f "$SCHEMA_PATH" ]; then
    echo "Error: Schema file not found at $SCHEMA_PATH"
    exit 1
fi

source .env
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME < "$SCHEMA_PATH"