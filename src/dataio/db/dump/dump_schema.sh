source .env
pg_dump -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME --schema-only > schema.sql