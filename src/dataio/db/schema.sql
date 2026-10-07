--
-- PostgreSQL database dump
--

-- Dumped from database version 16.14
-- Dumped by pg_dump version 16.14

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
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
-- Name: dataset_manifest_draft_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.dataset_manifest_draft_status AS ENUM (
    'pending',
    'approved',
    'rejected',
    'flagged'
);


--
-- Name: resource_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.resource_type AS ENUM (
    'DATASET',
    'GROUP',
    'BUCKET',
    'WEATHER_DATA_API'
);


--
-- Name: spatial_resolution; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.spatial_resolution AS ENUM (
    'COUNTRY',
    'STATE',
    'UT',
    'DISTRICT',
    'SUBDISTRICT',
    'MUNICIPALITY',
    'VILLAGE',
    'WARD',
    'PRABHAG',
    'ULB',
    'LAT_LONG',
    'OTHER'
);


--
-- Name: temporal_resolution; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.temporal_resolution AS ENUM (
    'NONE',
    'YEAR',
    'MONTH',
    'WEEK',
    'DATE',
    'HOUR',
    'MINUTE',
    'SECOND'
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
-- Name: add_dataset(character varying[], character varying, text, text, character varying[], text, text, text, public.spatial_resolution, date, date, public.temporal_resolution, public.access_level, jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.add_dataset(p_raw_dataset_ids character varying[], p_ds_id character varying, p_title text, p_collection_name text, p_tags character varying[], p_data_owner_name text, p_description text, p_spatial_coverage_region_id text, p_spatial_resolution public.spatial_resolution, p_temporal_coverage_start_date date, p_temporal_coverage_end_date date, p_temporal_resolution public.temporal_resolution, p_access_level public.access_level, p_additional_metadata jsonb) RETURNS void
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
-- Name: add_resource_group_member(text, text, public.resource_type, jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.add_resource_group_member(resource_group_name text, resource_id text, resource_type public.resource_type, resource_json jsonb) RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
    v_resource_group_id text;
begin
    -- get id from resource_group_name
    select resource_group_id into v_resource_group_id
    from resource_groups
    where group_name = resource_group_name;

    if v_resource_group_id is null then
        raise exception 'Resource group % not found', resource_group_name;
    end if;

    insert into resource_group_members (resource_group_id, resource_id, resource_type, resource_json) 
    values (v_resource_group_id, resource_id, resource_type, resource_json);
end;
$$;


--
-- Name: add_tag_to_dataset(character varying, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.add_tag_to_dataset(p_ds_id character varying, p_tag_str text) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: cleanup_expired_auth_data(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.cleanup_expired_auth_data() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Delete expired OTP tokens
    DELETE FROM otp_tokens WHERE expires_at < NOW();

    -- Delete expired WebAuthn challenges
    DELETE FROM webauthn_challenges WHERE expires_at < NOW();

    -- Delete expired sessions and revoked sessions older than 30 days
    DELETE FROM sessions
    WHERE expires_at < NOW()
       OR (revoked_at IS NOT NULL AND revoked_at < NOW() - INTERVAL '30 days');
END;
$$;


--
-- Name: cleanup_expired_magic_links(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.cleanup_expired_magic_links() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    DELETE FROM magic_link_tokens
    WHERE expires_at < NOW() - INTERVAL '1 day';
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


--
-- Name: update_chat_session_updated_at(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.update_chat_session_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE chat_sessions SET updated_at = NOW() WHERE id = NEW.session_id;
    RETURN NEW;
END;
$$;


--
-- Name: validate_group_membership(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_group_membership() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Check if group_email refers to a group
    IF NOT EXISTS (
        SELECT 1 FROM users 
        WHERE email = NEW.group_email 
        AND is_group = TRUE
    ) THEN
        RAISE EXCEPTION 'group_email must reference a group';
    END IF;

    -- Check if user_email refers to a non-group user
    IF NOT EXISTS (
        SELECT 1 FROM users 
        WHERE email = NEW.user_email 
        AND is_group = FALSE
    ) THEN
        RAISE EXCEPTION 'user_email must reference a non-group user';
    END IF;

    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: auth_audit_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.auth_audit_logs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    event_type text NOT NULL,
    outcome text NOT NULL,
    actor_email text,
    target_email text,
    ip_address text,
    user_agent text,
    details jsonb,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: auth_rate_limits; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.auth_rate_limits (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    action text NOT NULL,
    subject text NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    window_started_at timestamp without time zone DEFAULT now() NOT NULL,
    blocked_until timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: chat_messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_messages (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    session_id uuid NOT NULL,
    role text NOT NULL,
    content text NOT NULL,
    tool_calls jsonb,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT chat_messages_role_check CHECK ((role = ANY (ARRAY['user'::text, 'assistant'::text, 'system'::text])))
);


--
-- Name: chat_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_sessions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_email text NOT NULL,
    title text,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    deleted_at timestamp without time zone
);


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
-- Name: dataset_downloads; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dataset_downloads (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_email text NOT NULL,
    dataset_id text NOT NULL,
    access_channel text DEFAULT 'WEB'::text NOT NULL,
    ip_address text,
    user_agent text,
    downloaded_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dataset_manifest_drafts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dataset_manifest_drafts (
    draft_id uuid DEFAULT gen_random_uuid() NOT NULL,
    dataset_id text,
    collection_id text NOT NULL,
    category_id text NOT NULL,
    source_csv_path text NOT NULL,
    digitization_log_path text,
    status public.dataset_manifest_draft_status DEFAULT 'pending'::public.dataset_manifest_draft_status NOT NULL,
    draft_yaml text NOT NULL,
    draft_json jsonb NOT NULL,
    flagged_fields jsonb DEFAULT '[]'::jsonb NOT NULL,
    reviewer_notes jsonb DEFAULT '[]'::jsonb NOT NULL,
    validation_result jsonb,
    llm_model_id text,
    llm_prompt_tokens integer,
    llm_completion_tokens integer,
    created_by text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    reviewed_by text,
    reviewed_at timestamp without time zone,
    superseded_by_draft_id uuid,
    raw_dataset_id text,
    import_started_at timestamp without time zone,
    imported_at timestamp without time zone,
    imported_by text,
    import_result jsonb
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
-- Name: datasets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.datasets (
    id integer NOT NULL,
    ds_id character varying(50) NOT NULL,
    title text NOT NULL,
    collection_id integer NOT NULL,
    data_owner_id integer NOT NULL,
    description text,
    spatial_coverage_region_id text,
    spatial_resolution public.spatial_resolution,
    temporal_coverage_start_date date,
    temporal_coverage_end_date date,
    temporal_resolution public.temporal_resolution,
    access_level public.access_level NOT NULL,
    additional_metadata jsonb,
    readme_md text,
    data_dictionary_json text,
    documentation_synced_at timestamp with time zone,
    manifest_yaml text,
    manifest_json jsonb,
    manifest_updated_at timestamp with time zone,
    manifest_updated_by text
);


--
-- Name: COLUMN datasets.readme_md; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.datasets.readme_md IS 'Cached README.md content from file server';


--
-- Name: COLUMN datasets.data_dictionary_json; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.datasets.data_dictionary_json IS 'Cached metadata.json (data dictionary) content from file server';


--
-- Name: COLUMN datasets.documentation_synced_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.datasets.documentation_synced_at IS 'Timestamp of last documentation sync from file server';


--
-- Name: COLUMN datasets.manifest_yaml; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.datasets.manifest_yaml IS 'Cached canonical manifest.yaml content from filestore';


--
-- Name: COLUMN datasets.manifest_json; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.datasets.manifest_json IS 'Cached normalized manifest JSON derived from manifest.yaml';


--
-- Name: COLUMN datasets.manifest_updated_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.datasets.manifest_updated_at IS 'Timestamp of last direct manifest update';


--
-- Name: COLUMN datasets.manifest_updated_by; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.datasets.manifest_updated_by IS 'Actor email that last updated the manifest cache';


--
-- Name: raw_datasets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_datasets (
    id integer NOT NULL,
    rds_id character varying(50) NOT NULL,
    title text NOT NULL,
    source text NOT NULL
);


--
-- Name: regions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.regions (
    id integer NOT NULL,
    region_id text NOT NULL,
    region_name text NOT NULL,
    parent_region_id text
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
    array_agg(DISTINCT rd.rds_id) AS rds_ids,
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
    array_agg(DISTINCT t.tag_name) AS tags,
    r.region_name AS spatial_coverage,
    d.spatial_resolution,
    d.temporal_coverage_start_date,
    d.temporal_coverage_end_date,
    d.temporal_resolution,
    d.access_level,
    d.additional_metadata,
    d.readme_md,
    d.data_dictionary_json,
    d.manifest_yaml,
    d.manifest_json,
    d.manifest_updated_at,
    d.manifest_updated_by,
    d.documentation_synced_at
   FROM (((((((public.datasets d
     LEFT JOIN public.collections c ON ((d.collection_id = c.id)))
     LEFT JOIN public.data_owners do2 ON ((d.data_owner_id = do2.id)))
     LEFT JOIN public.dataset_raw_datasets drd ON ((d.id = drd.dataset_id)))
     LEFT JOIN public.raw_datasets rd ON ((rd.id = drd.raw_dataset_id)))
     LEFT JOIN public.dataset_tags dt ON ((dt.dataset_id = d.id)))
     LEFT JOIN public.tags t ON ((t.id = dt.tag_id)))
     LEFT JOIN public.regions r ON ((r.region_id = d.spatial_coverage_region_id)))
  GROUP BY d.id, d.ds_id, d.title, c.collection_id, c.collection_name, c.category_id, c.category_name, do2.name, do2.contact_person, do2.contact_person_email, d.description, r.region_name, d.spatial_resolution, d.temporal_coverage_start_date, d.temporal_coverage_end_date, d.temporal_resolution, d.access_level, d.additional_metadata, d.readme_md, d.data_dictionary_json, d.manifest_yaml, d.manifest_json, d.manifest_updated_at, d.manifest_updated_by, d.documentation_synced_at;


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
-- Name: magic_link_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.magic_link_tokens (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    email text NOT NULL,
    token text NOT NULL,
    purpose text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    used_at timestamp with time zone,
    invited_by text
);


--
-- Name: COLUMN magic_link_tokens.invited_by; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.magic_link_tokens.invited_by IS 'Email of admin who sent the invitation (for purpose=invitation)';


--
-- Name: oauth_identities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.oauth_identities (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    provider text NOT NULL,
    provider_user_id text NOT NULL,
    user_email text NOT NULL,
    provider_email text,
    provider_email_verified boolean DEFAULT false NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    last_login_at timestamp without time zone
);


--
-- Name: otp_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.otp_tokens (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    email text NOT NULL,
    code text NOT NULL,
    purpose text NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    expires_at timestamp without time zone NOT NULL,
    used_at timestamp without time zone,
    attempts integer DEFAULT 0 NOT NULL,
    CONSTRAINT otp_tokens_purpose_check CHECK (((purpose = ANY (ARRAY['login'::text, 'verify_email'::text, 'invite'::text, 'registration'::text, 'account_deletion'::text])) OR (purpose ~~ 'dataset\_deletion:_%'::text)))
);


--
-- Name: rate_limit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.rate_limit (
    user_email text NOT NULL,
    number_of_attempts integer DEFAULT 0 NOT NULL,
    max_limit_per_minute integer DEFAULT 5 NOT NULL,
    last_access_timestamp timestamp without time zone DEFAULT now() NOT NULL,
    access_point text NOT NULL
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
-- Name: regions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.regions ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.regions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: reserved_dataset_ids; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.reserved_dataset_ids (
    id integer NOT NULL,
    ds_id text NOT NULL,
    collection_id text,
    note text,
    reserved_by text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: reserved_dataset_ids_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.reserved_dataset_ids_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: reserved_dataset_ids_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.reserved_dataset_ids_id_seq OWNED BY public.reserved_dataset_ids.id;


--
-- Name: reserved_raw_dataset_ids; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.reserved_raw_dataset_ids (
    id integer NOT NULL,
    rds_id text NOT NULL,
    category_id text,
    note text,
    reserved_by text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: reserved_raw_dataset_ids_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.reserved_raw_dataset_ids_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: reserved_raw_dataset_ids_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.reserved_raw_dataset_ids_id_seq OWNED BY public.reserved_raw_dataset_ids.id;


--
-- Name: resource_group_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resource_group_members (
    resource_group_id text NOT NULL,
    resource_id text NOT NULL,
    resource_json jsonb,
    resource_type public.resource_type NOT NULL
);


--
-- Name: resource_groups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resource_groups (
    id integer NOT NULL,
    resource_group_id text NOT NULL,
    group_name text NOT NULL
);


--
-- Name: resource_groups_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.resource_groups_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: resource_groups_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.resource_groups_id_seq OWNED BY public.resource_groups.id;


--
-- Name: sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sessions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_email text NOT NULL,
    refresh_token text,
    user_agent text,
    ip_address text,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    expires_at timestamp without time zone NOT NULL,
    revoked_at timestamp without time zone,
    refresh_token_jti_hash text,
    last_seen_at timestamp without time zone DEFAULT now() NOT NULL
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
-- Name: user_api_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_api_keys (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_email text NOT NULL,
    key_hash text NOT NULL,
    key_prefix text NOT NULL,
    name text NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    last_used_at timestamp without time zone,
    expires_at timestamp without time zone,
    revoked_at timestamp without time zone
);


--
-- Name: user_groups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_groups (
    group_email text NOT NULL,
    user_email text NOT NULL
);


--
-- Name: user_permissions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_permissions (
    user_email text NOT NULL,
    resource_type public.resource_type NOT NULL,
    resource_id text NOT NULL,
    permission public.access_level NOT NULL
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    email text NOT NULL,
    key text,
    is_group boolean DEFAULT false NOT NULL,
    is_admin boolean DEFAULT false NOT NULL,
    email_verified boolean DEFAULT false NOT NULL,
    last_login timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    display_name text,
    suspended_at timestamp without time zone,
    suspended_by text,
    verification_status text DEFAULT 'verified'::text,
    registered_at timestamp with time zone,
    verified_at timestamp with time zone,
    verified_by text,
    CONSTRAINT valid_user_group CHECK ((((is_group = true) AND (key IS NULL)) OR (is_group = false)))
);


--
-- Name: webauthn_challenges; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.webauthn_challenges (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_email text NOT NULL,
    challenge text NOT NULL,
    purpose text NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    expires_at timestamp without time zone NOT NULL,
    CONSTRAINT webauthn_challenges_purpose_check CHECK ((purpose = ANY (ARRAY['registration'::text, 'authentication'::text])))
);


--
-- Name: webauthn_credentials; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.webauthn_credentials (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_email text NOT NULL,
    credential_id text NOT NULL,
    public_key bytea NOT NULL,
    sign_count integer DEFAULT 0 NOT NULL,
    device_name text,
    transports text[],
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    last_used_at timestamp without time zone
);


--
-- Name: reserved_dataset_ids id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reserved_dataset_ids ALTER COLUMN id SET DEFAULT nextval('public.reserved_dataset_ids_id_seq'::regclass);


--
-- Name: reserved_raw_dataset_ids id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reserved_raw_dataset_ids ALTER COLUMN id SET DEFAULT nextval('public.reserved_raw_dataset_ids_id_seq'::regclass);


--
-- Name: resource_groups id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_groups ALTER COLUMN id SET DEFAULT nextval('public.resource_groups_id_seq'::regclass);


--
-- Name: auth_audit_logs auth_audit_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auth_audit_logs
    ADD CONSTRAINT auth_audit_logs_pkey PRIMARY KEY (id);


--
-- Name: auth_rate_limits auth_rate_limits_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auth_rate_limits
    ADD CONSTRAINT auth_rate_limits_pkey PRIMARY KEY (id);


--
-- Name: chat_messages chat_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_messages
    ADD CONSTRAINT chat_messages_pkey PRIMARY KEY (id);


--
-- Name: chat_sessions chat_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_sessions
    ADD CONSTRAINT chat_sessions_pkey PRIMARY KEY (id);


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
-- Name: dataset_downloads dataset_downloads_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_downloads
    ADD CONSTRAINT dataset_downloads_pkey PRIMARY KEY (id);


--
-- Name: dataset_manifest_drafts dataset_manifest_drafts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_manifest_drafts
    ADD CONSTRAINT dataset_manifest_drafts_pkey PRIMARY KEY (draft_id);


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
-- Name: magic_link_tokens magic_link_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.magic_link_tokens
    ADD CONSTRAINT magic_link_tokens_pkey PRIMARY KEY (id);


--
-- Name: magic_link_tokens magic_link_tokens_token_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.magic_link_tokens
    ADD CONSTRAINT magic_link_tokens_token_key UNIQUE (token);


--
-- Name: oauth_identities oauth_identities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.oauth_identities
    ADD CONSTRAINT oauth_identities_pkey PRIMARY KEY (id);


--
-- Name: otp_tokens otp_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.otp_tokens
    ADD CONSTRAINT otp_tokens_pkey PRIMARY KEY (id);


--
-- Name: rate_limit rate_limit_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rate_limit
    ADD CONSTRAINT rate_limit_pkey PRIMARY KEY (user_email, access_point);


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
-- Name: regions regions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.regions
    ADD CONSTRAINT regions_pkey PRIMARY KEY (id);


--
-- Name: regions regions_region_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.regions
    ADD CONSTRAINT regions_region_id_key UNIQUE (region_id);


--
-- Name: reserved_dataset_ids reserved_dataset_ids_ds_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reserved_dataset_ids
    ADD CONSTRAINT reserved_dataset_ids_ds_id_key UNIQUE (ds_id);


--
-- Name: reserved_dataset_ids reserved_dataset_ids_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reserved_dataset_ids
    ADD CONSTRAINT reserved_dataset_ids_pkey PRIMARY KEY (id);


--
-- Name: reserved_raw_dataset_ids reserved_raw_dataset_ids_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reserved_raw_dataset_ids
    ADD CONSTRAINT reserved_raw_dataset_ids_pkey PRIMARY KEY (id);


--
-- Name: reserved_raw_dataset_ids reserved_raw_dataset_ids_rds_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reserved_raw_dataset_ids
    ADD CONSTRAINT reserved_raw_dataset_ids_rds_id_key UNIQUE (rds_id);


--
-- Name: resource_group_members resource_group_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_group_members
    ADD CONSTRAINT resource_group_members_pkey PRIMARY KEY (resource_group_id, resource_id);


--
-- Name: resource_groups resource_groups_group_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_groups
    ADD CONSTRAINT resource_groups_group_name_key UNIQUE (group_name);


--
-- Name: resource_groups resource_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_groups
    ADD CONSTRAINT resource_groups_pkey PRIMARY KEY (id);


--
-- Name: resource_groups resource_groups_resource_group_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_groups
    ADD CONSTRAINT resource_groups_resource_group_id_key UNIQUE (resource_group_id);


--
-- Name: sessions sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (id);


--
-- Name: sessions sessions_refresh_token_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_refresh_token_key UNIQUE (refresh_token);


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
-- Name: user_api_keys user_api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_api_keys
    ADD CONSTRAINT user_api_keys_pkey PRIMARY KEY (id);


--
-- Name: user_groups user_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_groups
    ADD CONSTRAINT user_groups_pkey PRIMARY KEY (group_email, user_email);


--
-- Name: user_permissions user_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_permissions
    ADD CONSTRAINT user_permissions_pkey PRIMARY KEY (user_email, resource_type, resource_id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (email);


--
-- Name: webauthn_challenges webauthn_challenges_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webauthn_challenges
    ADD CONSTRAINT webauthn_challenges_pkey PRIMARY KEY (id);


--
-- Name: webauthn_credentials webauthn_credentials_credential_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webauthn_credentials
    ADD CONSTRAINT webauthn_credentials_credential_id_key UNIQUE (credential_id);


--
-- Name: webauthn_credentials webauthn_credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webauthn_credentials
    ADD CONSTRAINT webauthn_credentials_pkey PRIMARY KEY (id);


--
-- Name: idx_auth_audit_logs_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_auth_audit_logs_created_at ON public.auth_audit_logs USING btree (created_at DESC);


--
-- Name: idx_auth_audit_logs_event_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_auth_audit_logs_event_type ON public.auth_audit_logs USING btree (event_type);


--
-- Name: idx_auth_audit_logs_target_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_auth_audit_logs_target_email ON public.auth_audit_logs USING btree (target_email);


--
-- Name: idx_auth_rate_limits_action_subject; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_auth_rate_limits_action_subject ON public.auth_rate_limits USING btree (action, subject);


--
-- Name: idx_auth_rate_limits_blocked_until; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_auth_rate_limits_blocked_until ON public.auth_rate_limits USING btree (blocked_until);


--
-- Name: idx_chat_messages_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_messages_created_at ON public.chat_messages USING btree (created_at);


--
-- Name: idx_chat_messages_session_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_messages_session_id ON public.chat_messages USING btree (session_id);


--
-- Name: idx_chat_sessions_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_sessions_updated_at ON public.chat_sessions USING btree (updated_at DESC);


--
-- Name: idx_chat_sessions_user_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_sessions_user_email ON public.chat_sessions USING btree (user_email);


--
-- Name: idx_dataset_downloads_dataset_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dataset_downloads_dataset_id ON public.dataset_downloads USING btree (dataset_id);


--
-- Name: idx_dataset_downloads_downloaded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dataset_downloads_downloaded_at ON public.dataset_downloads USING btree (downloaded_at);


--
-- Name: idx_dataset_downloads_user_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dataset_downloads_user_email ON public.dataset_downloads USING btree (user_email);


--
-- Name: idx_dataset_manifest_drafts_dataset_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dataset_manifest_drafts_dataset_id ON public.dataset_manifest_drafts USING btree (dataset_id);


--
-- Name: idx_dataset_manifest_drafts_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dataset_manifest_drafts_status ON public.dataset_manifest_drafts USING btree (status);


--
-- Name: idx_magic_link_tokens_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_magic_link_tokens_email ON public.magic_link_tokens USING btree (email);


--
-- Name: idx_magic_link_tokens_expires; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_magic_link_tokens_expires ON public.magic_link_tokens USING btree (expires_at);


--
-- Name: idx_magic_link_tokens_invited_by; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_magic_link_tokens_invited_by ON public.magic_link_tokens USING btree (invited_by) WHERE (invited_by IS NOT NULL);


--
-- Name: idx_magic_link_tokens_token; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_magic_link_tokens_token ON public.magic_link_tokens USING btree (token);


--
-- Name: idx_oauth_identities_provider_user; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_oauth_identities_provider_user ON public.oauth_identities USING btree (provider, provider_user_id);


--
-- Name: idx_oauth_identities_user_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_oauth_identities_user_email ON public.oauth_identities USING btree (user_email);


--
-- Name: idx_otp_tokens_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_otp_tokens_email ON public.otp_tokens USING btree (email);


--
-- Name: idx_otp_tokens_expires_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_otp_tokens_expires_at ON public.otp_tokens USING btree (expires_at);


--
-- Name: idx_reserved_dataset_ids_collection_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_reserved_dataset_ids_collection_id ON public.reserved_dataset_ids USING btree (collection_id);


--
-- Name: idx_reserved_raw_dataset_ids_category_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_reserved_raw_dataset_ids_category_id ON public.reserved_raw_dataset_ids USING btree (category_id);


--
-- Name: idx_sessions_expires_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sessions_expires_at ON public.sessions USING btree (expires_at);


--
-- Name: idx_sessions_refresh_token; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sessions_refresh_token ON public.sessions USING btree (refresh_token);


--
-- Name: idx_sessions_refresh_token_jti_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_sessions_refresh_token_jti_hash ON public.sessions USING btree (refresh_token_jti_hash) WHERE (refresh_token_jti_hash IS NOT NULL);


--
-- Name: idx_sessions_user_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sessions_user_active ON public.sessions USING btree (user_email, revoked_at, expires_at);


--
-- Name: idx_sessions_user_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sessions_user_email ON public.sessions USING btree (user_email);


--
-- Name: idx_user_api_keys_key_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_api_keys_key_hash ON public.user_api_keys USING btree (key_hash);


--
-- Name: idx_user_api_keys_user_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_api_keys_user_email ON public.user_api_keys USING btree (user_email);


--
-- Name: idx_users_suspended_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_users_suspended_at ON public.users USING btree (suspended_at) WHERE (suspended_at IS NOT NULL);


--
-- Name: idx_users_verification_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_users_verification_status ON public.users USING btree (verification_status);


--
-- Name: idx_webauthn_challenges_expires_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webauthn_challenges_expires_at ON public.webauthn_challenges USING btree (expires_at);


--
-- Name: idx_webauthn_challenges_user_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webauthn_challenges_user_email ON public.webauthn_challenges USING btree (user_email);


--
-- Name: idx_webauthn_credentials_credential_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webauthn_credentials_credential_id ON public.webauthn_credentials USING btree (credential_id);


--
-- Name: idx_webauthn_credentials_user_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webauthn_credentials_user_email ON public.webauthn_credentials USING btree (user_email);


--
-- Name: datasets insert_dataset; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER insert_dataset BEFORE INSERT OR UPDATE ON public.datasets FOR EACH ROW EXECUTE FUNCTION public.tr_insert_dataset();


--
-- Name: chat_messages trigger_update_chat_session_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trigger_update_chat_session_updated_at AFTER INSERT ON public.chat_messages FOR EACH ROW EXECUTE FUNCTION public.update_chat_session_updated_at();


--
-- Name: user_groups validate_group_membership_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER validate_group_membership_trigger BEFORE INSERT OR UPDATE ON public.user_groups FOR EACH ROW EXECUTE FUNCTION public.validate_group_membership();


--
-- Name: chat_messages chat_messages_session_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_messages
    ADD CONSTRAINT chat_messages_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.chat_sessions(id) ON DELETE CASCADE;


--
-- Name: chat_sessions chat_sessions_user_email_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_sessions
    ADD CONSTRAINT chat_sessions_user_email_fkey FOREIGN KEY (user_email) REFERENCES public.users(email) ON DELETE CASCADE;


--
-- Name: dataset_downloads dataset_downloads_user_email_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_downloads
    ADD CONSTRAINT dataset_downloads_user_email_fkey FOREIGN KEY (user_email) REFERENCES public.users(email) ON DELETE CASCADE;


--
-- Name: dataset_manifest_drafts dataset_manifest_drafts_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_manifest_drafts
    ADD CONSTRAINT dataset_manifest_drafts_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(email) ON DELETE SET NULL;


--
-- Name: dataset_manifest_drafts dataset_manifest_drafts_imported_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_manifest_drafts
    ADD CONSTRAINT dataset_manifest_drafts_imported_by_fkey FOREIGN KEY (imported_by) REFERENCES public.users(email) ON DELETE SET NULL;


--
-- Name: dataset_manifest_drafts dataset_manifest_drafts_reviewed_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_manifest_drafts
    ADD CONSTRAINT dataset_manifest_drafts_reviewed_by_fkey FOREIGN KEY (reviewed_by) REFERENCES public.users(email) ON DELETE SET NULL;


--
-- Name: dataset_manifest_drafts dataset_manifest_drafts_superseded_by_draft_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_manifest_drafts
    ADD CONSTRAINT dataset_manifest_drafts_superseded_by_draft_id_fkey FOREIGN KEY (superseded_by_draft_id) REFERENCES public.dataset_manifest_drafts(draft_id);


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
-- Name: datasets datasets_spatial_coverage_region_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_spatial_coverage_region_id_fkey FOREIGN KEY (spatial_coverage_region_id) REFERENCES public.regions(region_id);


--
-- Name: oauth_identities oauth_identities_user_email_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.oauth_identities
    ADD CONSTRAINT oauth_identities_user_email_fkey FOREIGN KEY (user_email) REFERENCES public.users(email) ON DELETE CASCADE;


--
-- Name: reserved_dataset_ids reserved_dataset_ids_reserved_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reserved_dataset_ids
    ADD CONSTRAINT reserved_dataset_ids_reserved_by_fkey FOREIGN KEY (reserved_by) REFERENCES public.users(email) ON DELETE SET NULL;


--
-- Name: reserved_raw_dataset_ids reserved_raw_dataset_ids_reserved_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reserved_raw_dataset_ids
    ADD CONSTRAINT reserved_raw_dataset_ids_reserved_by_fkey FOREIGN KEY (reserved_by) REFERENCES public.users(email) ON DELETE SET NULL;


--
-- Name: resource_group_members resource_group_members_resource_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_group_members
    ADD CONSTRAINT resource_group_members_resource_group_id_fkey FOREIGN KEY (resource_group_id) REFERENCES public.resource_groups(resource_group_id);


--
-- Name: sessions sessions_user_email_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_user_email_fkey FOREIGN KEY (user_email) REFERENCES public.users(email) ON DELETE CASCADE;


--
-- Name: user_api_keys user_api_keys_user_email_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_api_keys
    ADD CONSTRAINT user_api_keys_user_email_fkey FOREIGN KEY (user_email) REFERENCES public.users(email) ON DELETE CASCADE;


--
-- Name: user_groups user_groups_group_email_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_groups
    ADD CONSTRAINT user_groups_group_email_fkey FOREIGN KEY (group_email) REFERENCES public.users(email);


--
-- Name: user_groups user_groups_user_email_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_groups
    ADD CONSTRAINT user_groups_user_email_fkey FOREIGN KEY (user_email) REFERENCES public.users(email);


--
-- Name: user_permissions user_permissions_user_email_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_permissions
    ADD CONSTRAINT user_permissions_user_email_fkey FOREIGN KEY (user_email) REFERENCES public.users(email);


--
-- Name: webauthn_credentials webauthn_credentials_user_email_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webauthn_credentials
    ADD CONSTRAINT webauthn_credentials_user_email_fkey FOREIGN KEY (user_email) REFERENCES public.users(email) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

