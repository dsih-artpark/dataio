Legacy SQL files moved here are intentionally excluded from the active migration chain.

Why:
- they were never recorded in `db_migration_history` in production
- they may conflict with tracked migration numbering
- `uv run all-migrations` only considers tracked forward migrations in the active migrations directory

Related note:
- active migration files `009` through `012` originally predated the tracked
  migration convention
- they have now been patched in place to register with `add_migration(...)` so
  environments missing them in `db_migration_history` can apply them safely on
  the next runner execution
