BEGIN;

CREATE TABLE rate_limit (
    user_email TEXT NOT NULL,
    number_of_attempts INT NOT NULL DEFAULT 0,
    max_limit_per_minute INT NOT NULL DEFAULT 5,
    last_access_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    access_point TEXT NOT NULL,
    PRIMARY KEY (user_email, access_point)
);

SELECT add_migration(4, '004_rate_limit_table');

COMMIT;