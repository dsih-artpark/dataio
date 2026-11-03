-- Migration 029: dataset licence
-- Adds a free-text licence to each dataset (e.g. "CC BY 4.0"), shown on the
-- public catalogue cards and dataset page and filterable there. NULL means the
-- licence hasn't been recorded yet; the site shows "Licence: not yet specified".

BEGIN;

ALTER TABLE datasets ADD COLUMN IF NOT EXISTS licence TEXT;

COMMENT ON COLUMN datasets.licence IS
    'Licence the data is shared under (e.g. CC BY 4.0). NULL = not yet specified.';

SELECT add_migration(29, '029_dataset_licence');

COMMIT;
