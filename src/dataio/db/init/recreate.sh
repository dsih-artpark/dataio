set -euo pipefail

source .env

# Drop database if it exists
dropdb -U $DB_USER -h $DB_HOST -p $DB_PORT --if-exists $DB_NAME

createdb -U $DB_USER -h $DB_HOST -p $DB_PORT -T template0 $DB_NAME

# Apply all migrations in order
for migration in $(ls -v "$MIGRATIONS_DIR"/*.sql); do
    echo "Applying migration: $migration"
    psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -v  "ON_ERROR_STOP=1" -f "$migration"
done
