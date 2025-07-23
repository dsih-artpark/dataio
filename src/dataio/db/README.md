# dataio - Database Module

This directory has all the information to update and recreate the database at the core of the dataio system.


Schemas are versioned and committed to github. Data is versioned, tagged, and dumped to S3.

## Updating the Database

To update the database, you need to:
1. Create a new migration script.
2. Update the schema.
3. Update the data dump on the S3 bucket, with the a tag. The tag should be as follows:
```
key: schema_v00x
value: YYYY-MM-DDTHH:MM:SS
```

## Recreating the Database

There are two ways to recreate the database.

1. Access the latest data dump (or relevant data dump tied to the schema version of your choice). Then run the restore_from_local.sh script:
   ```bash
   bash ./src/dataio/db/restore/restore_from_local.sh
   ```

2. Initialise a fresh database by running the following command.
   ```bash
   bash ./src/dataio/db/init/recreate.sh
   ```
   This drops existing db, creates new db, runs migration scripts in order.
   Use
   ```bash
   bash ./src/dataio/db/init/recreate_full.sh
   ```
   if you want to do the data inserts as well.
   keep the data_inserts folder inside ../db/init


If you want to restore the database to a specific previous version, checkout the versioned schema from the previous commit, and run the restore script.

## Starting the API
```
uv run fastapi dev src/dataio/api
```

## Development Guidelines

Use transaction blocks (BEGIN; COMMIT;) in the migration scripts - so that it's never applied half way in case of any errors.

Please commit the migration script & schema.sql in the same commit (mention in commit message - if this is not followed for any case).

Put the migration script number and name in the commit message.

**Please never commit data dumps or data inserts.**

## TO DO
- [ ] Allow restore from S3.
- [ ] Ensure that the schema dump is up to date with the latest schema.
- [ ] Explore if python scripts can be used to generate the schema and data dumps instead of shell, to allow for more flexibility.