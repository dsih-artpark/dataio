set -euo pipefail

source .env
psql -U postgres -d "$DB_NAME" -h "$DB_HOST" -f "$REPO_DIR/db/init/data_inserts/collections.sql"
psql -U postgres -d "$DB_NAME" -h "$DB_HOST" -f "$REPO_DIR/db/init/data_inserts/data_owners.sql"
