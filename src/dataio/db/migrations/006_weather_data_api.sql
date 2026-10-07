BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_enum enum_value
        JOIN pg_type enum_type
            ON enum_type.oid = enum_value.enumtypid
        WHERE enum_type.typname = 'resource_type'
          AND enum_value.enumlabel = 'WEATHER_DATA_API'
    ) THEN
        ALTER TYPE resource_type ADD VALUE 'WEATHER_DATA_API';
    END IF;
END;
$$;

SELECT add_migration(6, '006_weather_data_api');
COMMIT;
