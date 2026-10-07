set -euo pipefail

source .env

dropdb -U $DB_USER -h $DB_HOST -p $DB_PORT --if-exists $DB_NAME

createdb -U $DB_USER -h $DB_HOST -p $DB_PORT -T template0 $DB_NAME

# Apply all forward migrations in order (rollback files sit in the same
# folder and would undo each migration right after it is applied)
for migration in $(ls -v "$MIGRATIONS_DIR"/*.sql); do
    case "$migration" in *_rollback.sql) continue ;; esac
    echo "Applying migration: $migration"
    psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -v "ON_ERROR_STOP=1" -f  "$migration"
done

python src/dataio/db/init/insert_datasets.py
python src/dataio/db/init/users.py -ugra