-- Migration 023: Let users be deleted when they own drafts or ID reservations
-- 017, 018 and 020 created foreign keys to users(email) with no ON DELETE
-- rule, so deleting any admin who ever reserved an ID or created/reviewed a
-- manifest draft failed with a foreign-key violation. Switch them to
-- ON DELETE SET NULL: the drafts and reservations are kept (a reservation
-- must survive so its ID is not handed out again), only the author is lost.

BEGIN;

ALTER TABLE dataset_manifest_drafts ALTER COLUMN created_by DROP NOT NULL;
ALTER TABLE reserved_dataset_ids ALTER COLUMN reserved_by DROP NOT NULL;
ALTER TABLE reserved_raw_dataset_ids ALTER COLUMN reserved_by DROP NOT NULL;

-- Constraint names are looked up rather than assumed, in case an environment
-- created them under a non-default name.
DO $$
DECLARE
    target RECORD;
    fk_name TEXT;
BEGIN
    FOR target IN
        SELECT * FROM (VALUES
            ('dataset_manifest_drafts', 'created_by'),
            ('dataset_manifest_drafts', 'reviewed_by'),
            ('reserved_dataset_ids', 'reserved_by'),
            ('reserved_raw_dataset_ids', 'reserved_by')
        ) AS t(table_name, column_name)
    LOOP
        FOR fk_name IN
            SELECT con.conname
            FROM pg_constraint con
            JOIN pg_attribute att
                ON att.attrelid = con.conrelid AND att.attnum = ANY (con.conkey)
            WHERE con.contype = 'f'
              AND con.conrelid = target.table_name::regclass
              AND con.confrelid = 'users'::regclass
              AND att.attname = target.column_name
        LOOP
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', target.table_name, fk_name);
        END LOOP;

        EXECUTE format(
            'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES users(email) ON DELETE SET NULL',
            target.table_name,
            target.table_name || '_' || target.column_name || '_fkey',
            target.column_name
        );
    END LOOP;
END;
$$;

SELECT add_migration(23, '023_user_fk_on_delete');

COMMIT;
