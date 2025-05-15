source .env
createdb -U $DB_USER -h $DB_HOST -p $DB_PORT -T template0 $DB_NAME
bash ./src/dataio/db/restore/restore_schema.sh $SCHEMA_FILE_TO_RESTORE
bash ./src/dataio/db/restore/restore_data.sh $DATA_FILE_TO_RESTORE

# This currentlly restores the schema and data to the database from local.
# TODO: Add a script to restore the schema and data from S3, taking the schema version as an argument.