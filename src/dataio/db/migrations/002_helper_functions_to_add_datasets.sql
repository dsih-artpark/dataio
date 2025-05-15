BEGIN;

CREATE OR REPLACE FUNCTION add_dataset(
    p_raw_dataset_ids VARCHAR[],
    p_ds_id VARCHAR,
    p_title TEXT,
    p_collection_name TEXT,
    p_data_owner_name TEXT,
    p_concept_name TEXT,
    p_description TEXT,
    p_tag_names TEXT[],
    p_spatial_coverage TEXT,
    p_spatial_resolution TEXT,
    p_temporal_coverage TEXT,
    p_temporal_resolution TEXT,
    p_public_access_level access_level,
    p_notes TEXT,
    p_supplementary_documents TEXT
) RETURNS VOID AS $$
DECLARE
    v_raw_dataset_ids INTEGER[];
    v_collection_id INTEGER;
    v_data_owner_id INTEGER;
    v_concept_id INTEGER;
    v_tag_ids INTEGER[];
    v_dataset_id INTEGER;
BEGIN
    -- Convert raw_dataset_ids from VARCHAR[] to their corresponding INTEGER[]
    IF p_raw_dataset_ids IS NOT NULL AND array_length(p_raw_dataset_ids, 1) > 0 THEN
        SELECT array_agg(id) INTO v_raw_dataset_ids
        FROM raw_datasets
        WHERE rds_id = ANY(p_raw_dataset_ids);
    ELSE
        v_raw_dataset_ids := '{}'::INTEGER[];
    END IF;

    -- Get collection_id
    SELECT id INTO v_collection_id
    FROM collections
    WHERE collection_name = p_collection_name;

    -- Get data_owner_id
    SELECT id INTO v_data_owner_id
    FROM data_owners
    WHERE name = p_data_owner_name;

    -- Get concept_id
    SELECT id INTO v_concept_id
    FROM concepts
    WHERE concept_name = p_concept_name;

    -- Convert tag_names to actual IDs if not empty
    IF p_tag_names IS NOT NULL AND array_length(p_tag_names, 1) > 0 THEN
        SELECT array_agg(id) INTO v_tag_ids
        FROM tags
        WHERE tag_name = ANY(p_tag_names);
    ELSE
        v_tag_ids := '{}'::INTEGER[];
    END IF;

    -- Insert into datasets
    INSERT INTO datasets (
        ds_id,
        title,
        collection_id,
        data_owner_id,
        concept_id,
        description,
        tag_ids,
        spatial_coverage,
        spatial_resolution,
        temporal_coverage,
        temporal_resolution,
        public_access_level,
        notes,
        supplementary_documents
    ) VALUES (
        p_ds_id,
        p_title,
        v_collection_id,
        v_data_owner_id,
        v_concept_id,
        p_description,
        v_tag_ids,
        p_spatial_coverage,
        p_spatial_resolution,
        p_temporal_coverage,
        p_temporal_resolution,
        p_public_access_level,
        p_notes,
        p_supplementary_documents
    ) RETURNING id INTO v_dataset_id;

    -- Create relationships in datasets_raw_datasets table
    IF v_raw_dataset_ids IS NOT NULL AND array_length(v_raw_dataset_ids, 1) > 0 THEN
        INSERT INTO dataset_raw_datasets (dataset_id, raw_dataset_id)
        SELECT v_dataset_id, unnest(v_raw_dataset_ids);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_raw_dataset(
    p_rds_id VARCHAR,
    p_title TEXT,
    p_source TEXT,
    p_data_owner_name TEXT
) RETURNS VOID AS $$
DECLARE
    v_data_owner_id INTEGER;
BEGIN
    -- Get data_owner_id
    SELECT id INTO v_data_owner_id
    FROM data_owners
    WHERE name = p_data_owner_name;

    -- Insert into raw_datasets
    INSERT INTO raw_datasets (
        rds_id,
        title,
        source,
        data_owner_id
    ) VALUES (
        p_rds_id,
        p_title,
        p_source,
        v_data_owner_id
    );
END;
$$ LANGUAGE plpgsql;

SELECT add_migration(2, '002_helper_functions_to_add_datasets.sql');

COMMIT;