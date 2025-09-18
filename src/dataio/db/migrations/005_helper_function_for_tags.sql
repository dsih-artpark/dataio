BEGIN;

CREATE OR REPLACE FUNCTION add_tag_to_dataset(
    p_ds_id VARCHAR,
    p_tag_str TEXT
) RETURNS VOID AS $$
DECLARE
    v_dataset_id INTEGER;
    v_tag_id INTEGER;
BEGIN
    -- Get dataset ID from ds_id
    SELECT id INTO v_dataset_id
    FROM datasets
    WHERE ds_id = p_ds_id;

    IF v_dataset_id IS NULL THEN
        RAISE EXCEPTION 'Dataset with ds_id % not found', p_ds_id;
    END IF;

    -- Get or create tag ID
    SELECT id INTO v_tag_id
    FROM tags
    WHERE tag_name = p_tag_str;

    -- If tag doesn't exist, create it
    IF v_tag_id IS NULL THEN
        INSERT INTO tags (tag_name) VALUES (p_tag_str)
        RETURNING id INTO v_tag_id;
    END IF;

    -- Insert into dataset_tags if not already exists
    INSERT INTO dataset_tags (dataset_id, tag_id)
    VALUES (v_dataset_id, v_tag_id)
    ON CONFLICT (dataset_id, tag_id) DO NOTHING;

END;
$$ LANGUAGE plpgsql;

SELECT add_migration(5, '005_helper_function_for_tags');

COMMIT;