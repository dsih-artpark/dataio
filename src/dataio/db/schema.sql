--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5 (Ubuntu 17.5-1.pgdg22.04+1)
-- Dumped by pg_dump version 17.5 (Ubuntu 17.5-1.pgdg22.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: access_level; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.access_level AS ENUM (
    'NONE',
    'VIEW',
    'DOWNLOAD'
);


--
-- Name: updation_frequency; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.updation_frequency AS ENUM (
    'ONE_TIME',
    'YEARLY',
    'MONTHLY',
    'WEEKLY',
    'DAILY',
    'HOURLY',
    'REAL_TIME',
    'ADHOC'
);


--
-- Name: version_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.version_type AS ENUM (
    'PREPROCESSED',
    'STANDARDISED'
);


--
-- Name: add_dataset(character varying[], character varying, text, text, character varying[], text, text, text, text, text, text, public.access_level, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.add_dataset(p_raw_dataset_ids character varying[], p_ds_id character varying, p_title text, p_collection_name text, p_tags character varying[], p_data_owner_name text, p_description text, p_spatial_coverage text, p_spatial_resolution text, p_temporal_coverage text, p_temporal_resolution text, p_public_access_level public.access_level, p_notes text, p_supplementary_documents text) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
        p_description,
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

    -- Create relationships in dataset_tags table
    IF v_tag_ids IS NOT NULL AND array_length(v_tag_ids, 1) > 0 THEN
        INSERT INTO dataset_tags (dataset_id, tag_id)
        SELECT v_dataset_id, unnest(v_tag_ids);
    END IF;
    
END;
$$;


--
-- Name: add_migration(integer, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.add_migration(p_migration_number integer, p_migration_name text) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO db_migration_history (migration_number, migration_name)
    VALUES (p_migration_number, p_migration_name);
EXCEPTION WHEN unique_violation THEN
    RAISE EXCEPTION 'Migration number % already exists!', p_migration_number;
END;
$$;


--
-- Name: add_raw_dataset(character varying, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.add_raw_dataset(p_rds_id character varying, p_title text, p_source text, p_data_owner_name text) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: tr_insert_dataset(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.tr_insert_dataset() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF length(NEW.ds_id) != 12 THEN
        RAISE EXCEPTION 'ds_id must be 12 characters long';
    END IF;
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: collections; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.collections (
    id integer NOT NULL,
    collection_id text NOT NULL,
    collection_name text NOT NULL,
    category_name text NOT NULL,
    category_id text NOT NULL
);


--
-- Name: collections_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.collections ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.collections_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: data_owners; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.data_owners (
    id integer NOT NULL,
    name text NOT NULL,
    contact_person text,
    contact_person_email text
);


--
-- Name: data_owners_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.data_owners ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.data_owners_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: dataset_raw_datasets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dataset_raw_datasets (
    dataset_id integer NOT NULL,
    raw_dataset_id integer NOT NULL
);


--
-- Name: dataset_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dataset_tags (
    dataset_id integer NOT NULL,
    tag_id integer NOT NULL
);


--
-- Name: dataset_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dataset_versions (
    dataset_id integer NOT NULL,
    version_id text NOT NULL,
    version_title text NOT NULL,
    type public.version_type NOT NULL,
    last_modified_date date NOT NULL,
    updation_frequency public.updation_frequency NOT NULL,
    public_access_level public.access_level NOT NULL
);


--
-- Name: datasets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.datasets (
    id integer NOT NULL,
    ds_id character varying(50) NOT NULL,
    title text NOT NULL,
    collection_id integer NOT NULL,
    data_owner_id integer NOT NULL,
    description text,
    spatial_coverage text,
    spatial_resolution text,
    temporal_coverage text,
    temporal_resolution text,
    public_access_level public.access_level NOT NULL,
    notes text,
    supplementary_documents text
);


--
-- Name: raw_datasets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_datasets (
    id integer NOT NULL,
    rds_id character varying(50) NOT NULL,
    title text NOT NULL,
    source text NOT NULL,
    data_owner_id integer NOT NULL
);


--
-- Name: tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tags (
    id integer NOT NULL,
    tag_name text NOT NULL
);


--
-- Name: datasets_full_view; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.datasets_full_view AS
 SELECT d.id,
    array_agg(rd.rds_id) AS rds_ids,
    d.ds_id,
    d.title,
    c.collection_id,
    c.collection_name,
    c.category_id,
    c.category_name,
    do2.name AS data_owner_name,
    do2.contact_person AS data_owner_contact_person,
    do2.contact_person_email AS data_owner_contact_person_email,
    d.description,
    array_agg(t.tag_name) AS tags,
    d.spatial_coverage,
    d.spatial_resolution,
    d.temporal_coverage,
    d.temporal_resolution,
    d.public_access_level,
    d.notes,
    d.supplementary_documents
   FROM ((((((public.datasets d
     LEFT JOIN public.collections c ON ((d.collection_id = c.id)))
     LEFT JOIN public.data_owners do2 ON ((d.data_owner_id = do2.id)))
     LEFT JOIN public.dataset_raw_datasets drd ON ((d.id = drd.dataset_id)))
     LEFT JOIN public.raw_datasets rd ON ((rd.id = drd.raw_dataset_id)))
     LEFT JOIN public.dataset_tags dt ON ((dt.dataset_id = d.id)))
     LEFT JOIN public.tags t ON ((t.id = dt.tag_id)))
  GROUP BY d.id, d.ds_id, d.title, c.collection_id, c.collection_name, c.category_id, c.category_name, do2.name, do2.contact_person, do2.contact_person_email, d.description, d.spatial_coverage, d.spatial_resolution, d.temporal_coverage, d.temporal_resolution, d.public_access_level, d.notes, d.supplementary_documents;


--
-- Name: datasets_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.datasets ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.datasets_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: db_migration_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.db_migration_history (
    id integer NOT NULL,
    migration_number integer NOT NULL,
    migration_name text NOT NULL
);


--
-- Name: db_migration_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.db_migration_history ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.db_migration_history_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_datasets_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.raw_datasets ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.raw_datasets_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: tags_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.tags ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.tags_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: collections collections_collection_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collections
    ADD CONSTRAINT collections_collection_name_key UNIQUE (collection_name);


--
-- Name: collections collections_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collections
    ADD CONSTRAINT collections_pkey PRIMARY KEY (id);


--
-- Name: data_owners data_owners_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.data_owners
    ADD CONSTRAINT data_owners_name_key UNIQUE (name);


--
-- Name: data_owners data_owners_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.data_owners
    ADD CONSTRAINT data_owners_pkey PRIMARY KEY (id);


--
-- Name: dataset_raw_datasets dataset_raw_datasets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_raw_datasets
    ADD CONSTRAINT dataset_raw_datasets_pkey PRIMARY KEY (dataset_id, raw_dataset_id);


--
-- Name: dataset_tags dataset_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_tags
    ADD CONSTRAINT dataset_tags_pkey PRIMARY KEY (dataset_id, tag_id);


--
-- Name: dataset_versions dataset_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_versions
    ADD CONSTRAINT dataset_versions_pkey PRIMARY KEY (version_id);


--
-- Name: datasets datasets_ds_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_ds_id_key UNIQUE (ds_id);


--
-- Name: datasets datasets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_pkey PRIMARY KEY (id);


--
-- Name: db_migration_history db_migration_history_migration_number_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.db_migration_history
    ADD CONSTRAINT db_migration_history_migration_number_key UNIQUE (migration_number);


--
-- Name: db_migration_history db_migration_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.db_migration_history
    ADD CONSTRAINT db_migration_history_pkey PRIMARY KEY (id);


--
-- Name: raw_datasets raw_datasets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_datasets
    ADD CONSTRAINT raw_datasets_pkey PRIMARY KEY (id);


--
-- Name: raw_datasets raw_datasets_rds_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_datasets
    ADD CONSTRAINT raw_datasets_rds_id_key UNIQUE (rds_id);


--
-- Name: tags tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_pkey PRIMARY KEY (id);


--
-- Name: tags tags_tag_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_tag_name_key UNIQUE (tag_name);


--
-- Name: datasets insert_dataset; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER insert_dataset BEFORE INSERT OR UPDATE ON public.datasets FOR EACH ROW EXECUTE FUNCTION public.tr_insert_dataset();


--
-- Name: dataset_raw_datasets dataset_raw_datasets_dataset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_raw_datasets
    ADD CONSTRAINT dataset_raw_datasets_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES public.datasets(id);


--
-- Name: dataset_raw_datasets dataset_raw_datasets_raw_dataset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_raw_datasets
    ADD CONSTRAINT dataset_raw_datasets_raw_dataset_id_fkey FOREIGN KEY (raw_dataset_id) REFERENCES public.raw_datasets(id);


--
-- Name: dataset_tags dataset_tags_dataset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_tags
    ADD CONSTRAINT dataset_tags_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES public.datasets(id);


--
-- Name: dataset_tags dataset_tags_tag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_tags
    ADD CONSTRAINT dataset_tags_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES public.tags(id);


--
-- Name: dataset_versions dataset_versions_dataset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_versions
    ADD CONSTRAINT dataset_versions_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES public.datasets(id);


--
-- Name: datasets datasets_collection_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_collection_id_fkey FOREIGN KEY (collection_id) REFERENCES public.collections(id);


--
-- Name: datasets datasets_data_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_data_owner_id_fkey FOREIGN KEY (data_owner_id) REFERENCES public.data_owners(id);


--
-- Name: raw_datasets raw_datasets_data_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_datasets
    ADD CONSTRAINT raw_datasets_data_owner_id_fkey FOREIGN KEY (data_owner_id) REFERENCES public.data_owners(id);


--
-- PostgreSQL database dump complete
--

