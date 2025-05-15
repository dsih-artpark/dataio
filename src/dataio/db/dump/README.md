# Database Dump Scripts

This directory contains scripts for dumping PostgreSQL database schema and data, and uploading them to S3.

## Scripts Overview

### 1. `dump_schema.sh`
Dumps only the database schema (structure) without data. This is useful for version controlling database structure changes.

Usage:
```bash
bash dump_schema.sh > schema.sql
```

### 2. `dump_data.sh`
Dumps only the database data without schema. This is useful for backing up data separately from schema.

Usage:
```bash
bash dump_data.sh > data.sql
```

### 3. `dump_to_s3.sh`
A comprehensive script that:
- Dumps both schema and data
- Uploads them to S3
- Requires AWS credentials and S3 bucket configuration
- Gets the latest migration number from the migrations directory
- Tags the schema and data with the migration number and current timestamp before uploading to S3

Usage:
```bash
bash dump_to_s3.sh
```

## Prerequisites

1. PostgreSQL client tools installed and in PATH
2. For S3 uploads:
   - AWS CLI installed and configured
   - Appropriate AWS credentials
   - S3 bucket configured

## Environment Variables

The scripts use the following environment variables (typically set in `.env`):
- `DB_NAME`: Name of the database to dump
- `DB_DUMP_S3_URI`: S3 URI for the database dump (for `dump_to_s3.sh`). Bucket and Prefix are extracted from the URI.
## Best Practices

1. Always dump schema and data separately for better version control
2. Regularly backup your database using `dump_to_s3.sh`
3. Keep your `.env` file secure and never commit it to version control
4. Test restores periodically to ensure backups are valid
