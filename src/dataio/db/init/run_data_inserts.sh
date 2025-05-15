source .env
psql -U postgres -d "$DB_NAME" -h "$DB_HOST" -f data_inserts/collections.sql
psql -U postgres -d "$DB_NAME" -h "$DB_HOST" -f data_inserts/concepts.sql
psql -U postgres -d "$DB_NAME" -h "$DB_HOST" -f data_inserts/data_owners.sql
