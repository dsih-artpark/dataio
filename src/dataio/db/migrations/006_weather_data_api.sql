BEGIN;

ALTER TYPE resource_type ADD VALUE 'WEATHER_DATA_API';

SELECT add_migration(6, '006_weather_data_api');
COMMIT;