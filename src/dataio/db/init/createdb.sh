source .env
createdb -U postgres -U $DB_USER -h $DB_HOST -p $DB_PORT -T template0 $DB_NAME