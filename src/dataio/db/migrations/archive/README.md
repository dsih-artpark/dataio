Legacy SQL files moved here are intentionally excluded from the active migration chain.

Why:
- they were never recorded in `db_migration_history` in production
- they may conflict with tracked migration numbering
- `uv run all-migrations` only considers tracked forward migrations in the active migrations directory
