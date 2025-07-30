BEGIN;

CREATE OR REPLACE FUNCTION add_dataset(
    p_raw_dataset_ids VARCHAR[],
    p_ds_id VARCHAR,
    p_title TEXT,
    p_collection_name TEXT,
    p_tags VARCHAR[],
    p_data_owner_name TEXT,
    p_description TEXT,
    p_spatial_coverage_region_id TEXT,
    p_spatial_resolution spatial_resolution,
    p_temporal_coverage_start_date date,
    p_temporal_coverage_end_date date,
    p_temporal_resolution temporal_resolution,
    p_access_level access_level,
    p_additional_metadata jsonb
) RETURNS VOID AS $$
DECLARE
    v_raw_dataset_ids INTEGER[];
    v_collection_id INTEGER;
    v_tag_ids INTEGER[];
    v_data_owner_id INTEGER;
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

    -- Convert tags from VARCHAR[] to their corresponding INTEGER[]
    IF p_tags IS NOT NULL AND array_length(p_tags, 1) > 0 THEN
        SELECT array_agg(id) INTO v_tag_ids
        FROM tags
        WHERE tag_name = ANY(p_tags);
    ELSE
        v_tag_ids := '{}'::INTEGER[];
    END IF;
    -- Get collection_id
    SELECT id INTO v_collection_id
    FROM collections
    WHERE collection_name = p_collection_name;

    -- Get data_owner_id
    SELECT id INTO v_data_owner_id
    FROM data_owners
    WHERE name = p_data_owner_name;

    -- Insert into datasets
    INSERT INTO datasets (
        ds_id,
        title,
        collection_id,
        data_owner_id,
        description,
        spatial_coverage_region_id,
        spatial_resolution,
        temporal_coverage_start_date,
        temporal_coverage_end_date,
        temporal_resolution,
        access_level,
        additional_metadata
    ) VALUES (
        p_ds_id,
        p_title,
        v_collection_id,
        v_data_owner_id,
        p_description,
        p_spatial_coverage_region_id,
        p_spatial_resolution,
        p_temporal_coverage_start_date,
        p_temporal_coverage_end_date,
        p_temporal_resolution,
        p_access_level,
        p_additional_metadata
    ) RETURNING id INTO v_dataset_id;

    -- Create relationships in datasets_raw_datasets table
    IF v_raw_dataset_ids IS NOT NULL AND array_length(v_raw_dataset_ids, 1) > 0 THEN
        INSERT INTO dataset_raw_datasets (dataset_id, raw_dataset_id)
        SELECT v_dataset_id, unnest(v_raw_dataset_ids);
    END IF;

    -- Create relationships in dataset_tags table
    IF v_tag_ids IS NOT NULL AND array_length(v_tag_ids, 1) > 0 THEN
        INSERT INTO dataset_tags (dataset_id, tag_id)
        SELECT v_dataset_id, unnest(v_tag_ids);
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

SELECT add_migration(2, '002_helper_functions_to_add_datasets');

COMMIT;