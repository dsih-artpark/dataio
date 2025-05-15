#!/bin/bash

# Check if data file path is provided
if [ $# -eq 0 ]; then
    echo "Error: Please provide the path to data.sql"
    echo "Usage: $0 <path_to_data.sql>"
    exit 1
fi

DATA_PATH="$1"

# Check if data file exists
if [ ! -f "$DATA_PATH" ]; then
    echo "Error: Data file not found at $DATA_PATH"
    exit 1
fi

source .env
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME < "$DATA_PATH"