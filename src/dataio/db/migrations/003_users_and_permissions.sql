BEGIN;

CREATE TABLE users (
    email TEXT PRIMARY KEY,
    key TEXT,
    is_group BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT valid_user_group CHECK (
        (is_group = TRUE AND key IS NULL) OR
        (is_group = FALSE AND key is not null)
    ),
    is_admin BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE user_groups (
    group_email TEXT REFERENCES users(email),
    user_email TEXT REFERENCES users(email),
    PRIMARY KEY (group_email, user_email)
);

create type resource_type as enum ('DATASET', 'GROUP', 'BUCKET');

CREATE TABLE user_permissions (
    user_email TEXT REFERENCES users(email),
    resource_type resource_type not null,
    resource_id text not null,
    permission access_level not null,
    PRIMARY KEY (user_email, resource_type, resource_id)
);

create table resource_groups (
    id serial primary key,
    resource_group_id text not null unique,
    group_name text not null unique
);

create table resource_group_members (
    resource_group_id text references resource_groups(resource_group_id),
    resource_id text not null,
    resource_json jsonb,
    resource_type resource_type not null,
    PRIMARY KEY (resource_group_id, resource_id)
);

create or replace function add_resource_group_member(resource_group_name text, resource_id text, resource_type resource_type, resource_json jsonb)
returns void as $$
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
$$ language plpgsql;


-- Function to validate group membership
CREATE OR REPLACE FUNCTION validate_group_membership()
RETURNS TRIGGER AS $$
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
$$ LANGUAGE plpgsql;

-- Trigger for user_groups
CREATE TRIGGER validate_group_membership_trigger
    BEFORE INSERT OR UPDATE ON user_groups
    FOR EACH ROW
    EXECUTE FUNCTION validate_group_membership();

SELECT add_migration(3, '003_users_and_permissions');


COMMIT;