# SQL Migrations

The migration runner only applies SQL files in this directory that:

- are not rollback files
- match the numbered filename pattern like `014_example.sql`
- contain `SELECT add_migration(...)`

## Important Note About 009-012

`009_user_suspension.sql`, `010_otp_purpose_constraint.sql`,
`011_invitation_magic_links.sql`, and `012_chat_sessions.sql` were added as
legacy SQL files without `add_migration(...)`.

They are now patched to be proper runner-tracked migrations.

Why this matters:

- environments that already applied `013` and `014` can still be missing
  `009-012` in `db_migration_history`
- the runner applies any tracked migration whose number is absent from history,
  so the newly tracked `009-012` will be picked up on the next run
- the SQL in these files is idempotent enough to apply safely in environments
  where some or all of the underlying schema already exists

Use the runner-managed chain as the source of truth for deployments.
