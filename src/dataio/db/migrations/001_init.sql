BEGIN;

-- DB Version Tracking Stuff

create table if not exists db_migration_history (
	id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	migration_number integer not null unique, 
	migration_name text not null
);

CREATE OR REPLACE FUNCTION add_migration(p_migration_number INTEGER, p_migration_name TEXT)
RETURNS VOID AS $$
BEGIN
    INSERT INTO db_migration_history (migration_number, migration_name)
    VALUES (p_migration_number, p_migration_name);
EXCEPTION WHEN unique_violation THEN
    RAISE EXCEPTION 'Migration number % already exists!', p_migration_number;
END;
$$ LANGUAGE plpgsql;

-- Datasets stuff

create type updation_frequency as enum('ONE_TIME', 'YEARLY', 'MONTHLY', 'WEEKLY', 'DAILY', 'HOURLY', 'REAL_TIME', 'ADHOC');

create type version_type as enum('PREPROCESSED', 'STANDARDISED');

create type access_level as enum('NONE', 'VIEW', 'DOWNLOAD');

create type temporal_resolution as enum('NONE', 'YEAR', 'MONTH', 'WEEK', 'DATE', 'HOUR', 'MINUTE', 'SECOND');

create type spatial_resolution as enum('COUNTRY', 'STATE', 'UT', 'DISTRICT', 'SUBDISTRICT', 'MUNICIPALITY', 'VILLAGE', 'WARD', 'PRABHAG', 'ULB', 'LAT_LONG', 'OTHER');

create table if not exists collections (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	collection_id text not null,
	collection_name text not null unique,
	category_name text not null,
    category_id text not null
);

create table if not exists data_owners (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name text not null unique,
    contact_person text,
    contact_person_email text
);

create table if not exists raw_datasets (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    rds_id VARCHAR(50) not null unique,
    title text not null,
    source text not null
);

create table if not exists tags (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    tag_name text not null unique
);

-- TODO: Spatial Coverage linked to RegionIDs

create table if not exists regions (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    region_id text not null unique,
    region_name text not null,
    parent_region_id text
);

CREATE TABLE if not exists datasets (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    ds_id VARCHAR(50) not null unique,
    title TEXT not null,
    collection_id integer not null,
    data_owner_id integer not null,
    description TEXT,
    spatial_coverage_region_id text,
    spatial_resolution spatial_resolution,
    temporal_coverage_start_date date,
    temporal_coverage_end_date date,
    temporal_resolution temporal_resolution,
    access_level access_level not null,
    additional_metadata jsonb,
    foreign key (collection_id) references collections (id),
    foreign key (data_owner_id) references data_owners (id),
    foreign key (spatial_coverage_region_id) references regions (region_id)
);

create table if not exists dataset_raw_datasets (
    dataset_id integer not null,
    raw_dataset_id integer not null,
    foreign key (dataset_id) references datasets (id),
    foreign key (raw_dataset_id) references raw_datasets (id),
    primary key (dataset_id, raw_dataset_id)
);

create table if not exists dataset_tags (
    dataset_id integer not null,
    tag_id integer not null,
    foreign key (dataset_id) references datasets (id),
    foreign key (tag_id) references tags (id),
    primary key (dataset_id, tag_id)
);

CREATE OR REPLACE FUNCTION TR_insert_dataset()
RETURNS TRIGGER AS $$
BEGIN
    IF length(NEW.ds_id) != 12 THEN
        RAISE EXCEPTION 'ds_id must be 12 characters long';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insert_dataset
BEFORE INSERT OR UPDATE ON datasets
FOR EACH ROW
EXECUTE FUNCTION TR_insert_dataset();

create view datasets_full_view as 
select d.id, array_agg(rd.rds_id) as rds_ids, d.ds_id, d.title, c.collection_id, c.collection_name, c.category_id, c.category_name, 
do2."name" as data_owner_name, do2.contact_person as data_owner_contact_person, do2.contact_person_email as data_owner_contact_person_email,
d.description, array_agg(t.tag_name) as tags,
r.region_name as spatial_coverage, d.spatial_resolution, d.temporal_coverage_start_date, d.temporal_coverage_end_date, d.temporal_resolution,
d.access_level, d.additional_metadata from datasets d left join collections c on d.collection_id = c.id left join data_owners do2 
on d.data_owner_id = do2.id 
left join dataset_raw_datasets drd on d.id = drd.dataset_id
left join raw_datasets rd on rd.id = drd.raw_dataset_id
left join dataset_tags dt on dt.dataset_id = d.id
left join tags t on t.id = dt.tag_id
left join regions r on r.region_id = d.spatial_coverage_region_id
group by d.id, d.ds_id, d.title, c.collection_id, c.collection_name, c.category_id, c.category_name, 
data_owner_name, data_owner_contact_person, data_owner_contact_person_email,
d.description, spatial_coverage, d.spatial_resolution, d.temporal_coverage_start_date, d.temporal_coverage_end_date, d.temporal_resolution,
d.access_level, d.additional_metadata;

SELECT add_migration(1, '001_datasets');

COMMIT;