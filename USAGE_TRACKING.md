# Usage Tracking for DataIO

This document describes the SQLite-based usage tracking system implemented for DataIO API.

## Overview

The usage tracking system automatically logs all API requests to a SQLite database with comprehensive information about each request. This provides detailed analytics and monitoring capabilities for the DataIO platform.

## Features

- ✅ **Automatic Request Tracking** - All API requests are logged automatically
- ✅ **SQLite Database** - Fast, reliable storage with built-in querying capabilities
- ✅ **Method Detection** - Automatically detects API vs SDK vs CLI usage
- ✅ **Resource Extraction** - Extracts dataset IDs and region IDs from endpoints
- ✅ **User Identification** - Links requests to authenticated users
- ✅ **Admin API Endpoints** - RESTful API for accessing usage statistics
- ✅ **CLI Commands** - Rich command-line interface for usage analysis
- ✅ **Data Export** - Export usage data to CSV format
- ✅ **Data Cleanup** - Automatic cleanup of old data to manage database size

## Database Schema

The system uses a SQLite database with the following schema:

```sql
CREATE TABLE usage_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    datetime TEXT NOT NULL,                    -- ISO timestamp with timezone
    user_email TEXT NOT NULL,                  -- User email from API key
    method_type TEXT NOT NULL,                 -- API, SDK, or CLI
    endpoint TEXT NOT NULL,                    -- API endpoint called
    http_method TEXT NOT NULL,                 -- GET, POST, etc.
    dataset_id TEXT,                           -- Dataset ID (if applicable)
    bucket_type TEXT,                          -- STANDARDISED/PREPROCESSED
    region_id TEXT,                            -- Region ID (for shapefiles)
    user_agent TEXT,                           -- Client user agent
    ip_address TEXT,                           -- Client IP address
    response_status INTEGER DEFAULT 200,       -- HTTP response status
    created_at TEXT DEFAULT CURRENT_TIMESTAMP  -- Record creation time
);
```

## Configuration

The usage tracking database path can be configured using the environment variable:

```bash
export USAGE_TRACKING_DB_PATH="/path/to/usage_tracking.db"
```

If not set, it defaults to `usage_tracking.db` in the current working directory.

## Usage

### Automatic Tracking

Once the API server is running with the middleware enabled, all requests are automatically tracked. The SQLite database will be created automatically with proper schema and indexes.

### Admin API Endpoints

All admin endpoints require admin authentication via API key.

#### Get Usage Statistics

```bash
# Get overall usage statistics
curl -H "X-API-Key: your_admin_key" \
     "https://dataio.artpark.ai/api/v1/admin/usage/stats"

# Get statistics for specific user
curl -H "X-API-Key: your_admin_key" \
     "https://dataio.artpark.ai/api/v1/admin/usage/stats?user=user@example.com&days=7"

# Get statistics for last 30 days
curl -H "X-API-Key: your_admin_key" \
     "https://dataio.artpark.ai/api/v1/admin/usage/stats?days=30"
```

**Response:**

```json
{
  "total_requests": 1250,
  "success_rate": 98.4,
  "by_method": {
    "SDK": 800,
    "CLI": 300,
    "API": 150
  },
  "by_endpoint": {
    "/api/v1/datasets": 500,
    "/api/v1/datasets/TS0001DS0001/STANDARDISED/tables": 200,
    "/api/v1/shapefiles": 100
  },
  "by_user": {
    "user1@example.com": 400,
    "user2@example.com": 300
  },
  "by_dataset": {
    "TS0001DS0001": 150,
    "TS0001DS0002": 100
  },
  "by_status": {
    "200": 1230,
    "404": 15,
    "500": 5
  }
}
```

#### Get User Activity

```bash
# Get detailed activity for a specific user
curl -H "X-API-Key: your_admin_key" \
     "https://dataio.artpark.ai/api/v1/admin/usage/user/user@example.com?days=7"
```

#### Get Dataset Usage

```bash
# Get usage statistics for a specific dataset
curl -H "X-API-Key: your_admin_key" \
     "https://dataio.artpark.ai/api/v1/admin/usage/dataset/TS0001DS0001?days=30"
```

#### Export Usage Data

```bash
# Export all usage data
curl -X POST -H "X-API-Key: your_admin_key" \
     "https://dataio.artpark.ai/api/v1/admin/usage/export"

# Export data for specific user
curl -X POST -H "X-API-Key: your_admin_key" \
     "https://dataio.artpark.ai/api/v1/admin/usage/export?user=user@example.com&days=7"
```

#### Cleanup Old Data

```bash
# Clean up data older than 365 days
curl -X POST -H "X-API-Key: your_admin_key" \
     "https://dataio.artpark.ai/api/v1/admin/usage/cleanup?days_to_keep=365"
```

### CLI Commands

#### View Usage Statistics

```bash
# Show overall usage statistics
uv run dataio usage stats

# Show statistics for a specific user
uv run dataio usage stats --user user@example.com

# Show statistics for the last 7 days
uv run dataio usage stats --days 7

# Use a custom database file
uv run dataio usage stats --db /path/to/custom_usage.db
```

#### View User Activity

```bash
# Show detailed activity for a specific user
uv run dataio usage user user@example.com

# Show activity for last 7 days
uv run dataio usage user user@example.com --days 7
```

#### View Dataset Usage

```bash
# Show usage statistics for a specific dataset
uv run dataio usage dataset TS0001DS0001

# Show usage for last 30 days
uv run dataio usage dataset TS0001DS0001 --days 30
```

#### Export Usage Data

```bash
# Export all usage data
uv run dataio usage export

# Export data for specific user
uv run dataio usage export --user user@example.com

# Export data for last 7 days
uv run dataio usage export --days 7

# Export to custom file
uv run dataio usage export --output my_usage_export.csv
```

#### Database Management

```bash
# Show database information
uv run dataio usage info

# Clean up old data (with confirmation)
uv run dataio usage cleanup --days 365 --confirm
```

## Method Type Detection

The system automatically detects how requests are made:

- **CLI**: User agent contains "dataio" and "cli"
- **SDK**: User agent contains "python" or "requests"
- **API**: All other requests (direct API calls, Postman, etc.)

## Resource Extraction

The system automatically extracts relevant information from API endpoints:

- **Dataset Endpoints**: `/api/v1/datasets/{dataset_id}/{bucket_type}/tables`
  - Extracts `dataset_id` and `bucket_type`
- **Shapefile Endpoints**: `/api/v1/shapefiles/{region_id}`
  - Extracts `region_id`
- **Region Endpoints**: `/api/v1/regions/{region_id}/children`
  - Extracts `region_id`

## Performance Considerations

- **SQLite Database**: Fast, reliable, and requires no additional infrastructure
- **Indexed Queries**: Database includes indexes on user_email, datetime, dataset_id, and endpoint
- **Asynchronous Logging**: Usage logging doesn't block API responses
- **Automatic Cleanup**: Built-in data retention management
- **Minimal Overhead**: Middleware adds minimal latency to requests

## Security and Privacy

- **User Identification**: User emails are logged for tracking purposes
- **IP Address Logging**: IP addresses are logged for security analysis
- **Admin-Only Access**: Usage statistics are only accessible to admin users
- **Local Storage**: All data is stored locally in SQLite format
- **Data Retention**: Consider implementing data retention policies for production use

## Monitoring and Alerts

The system provides rich data for monitoring:

- **Success Rates**: Track API health and error rates
- **Usage Patterns**: Identify peak usage times and popular datasets
- **User Behavior**: Understand how users interact with the API
- **Performance Metrics**: Monitor response times and error rates
- **Resource Usage**: Track which datasets are most popular

## Troubleshooting

### Database not being created

- Ensure the API server has write permissions in the target directory
- Check that the middleware is properly loaded in main.py
- Verify the USAGE_TRACKING_DB_PATH environment variable is set correctly

### Missing user information

- Ensure API key authentication is working properly
- Check that the auth middleware is storing user information in request state
- Verify that the usage tracking middleware is running after the auth middleware

### Performance issues

- Monitor database file size and consider cleanup
- Check database indexes are being used effectively
- Consider archiving old data to separate files

### CLI commands not working

- Ensure the usage tracking database exists
- Check file permissions on the database file
- Verify the database schema is correct

## Example Queries

### SQL Queries for Advanced Analysis

```sql
-- Top 10 users by request count
SELECT user_email, COUNT(*) as request_count
FROM usage_logs
WHERE datetime > datetime('now', '-30 days')
GROUP BY user_email
ORDER BY request_count DESC
LIMIT 10;

-- Most popular datasets
SELECT dataset_id, COUNT(*) as download_count
FROM usage_logs
WHERE dataset_id IS NOT NULL
  AND datetime > datetime('now', '-30 days')
GROUP BY dataset_id
ORDER BY download_count DESC
LIMIT 10;

-- API health over time
SELECT DATE(datetime) as date,
       COUNT(*) as total_requests,
       SUM(CASE WHEN response_status >= 200 AND response_status < 300 THEN 1 ELSE 0 END) as successful_requests,
       ROUND(100.0 * SUM(CASE WHEN response_status >= 200 AND response_status < 300 THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM usage_logs
WHERE datetime > datetime('now', '-7 days')
GROUP BY DATE(datetime)
ORDER BY date;

-- Method type distribution
SELECT method_type, COUNT(*) as count, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM usage_logs), 2) as percentage
FROM usage_logs
WHERE datetime > datetime('now', '-30 days')
GROUP BY method_type
ORDER BY count DESC;
```

## Migration from CSV

If you were previously using CSV-based tracking, you can migrate your data:

```python
import sqlite3
import csv
from datetime import datetime

# Create new SQLite database
conn = sqlite3.connect('usage_tracking.db')
cursor = conn.cursor()

# Create table (same as above)
cursor.execute('''
    CREATE TABLE usage_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datetime TEXT NOT NULL,
        user_email TEXT NOT NULL,
        method_type TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        http_method TEXT NOT NULL,
        dataset_id TEXT,
        bucket_type TEXT,
        region_id TEXT,
        user_agent TEXT,
        ip_address TEXT,
        response_status INTEGER DEFAULT 200,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
''')

# Read CSV and insert data
with open('old_usage_tracking.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cursor.execute('''
            INSERT INTO usage_logs
            (datetime, user_email, method_type, endpoint, http_method,
             dataset_id, bucket_type, region_id, user_agent, ip_address, response_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['datetime'], row['user'], row['method_type'], row['endpoint'],
            row['http_method'], row['dataset_id'], row['bucket_type'],
            row['region_id'], row['user_agent'], row['ip_address'],
            int(row['response_status'])
        ))

conn.commit()
conn.close()
```

## Future Enhancements

Potential future improvements to the usage tracking system:

- **Real-time Dashboards**: Web-based dashboard for real-time monitoring
- **Alerting System**: Automated alerts for unusual usage patterns
- **Advanced Analytics**: Machine learning for usage pattern analysis
- **API Rate Limiting**: Integration with rate limiting based on usage patterns
- **Data Visualization**: Charts and graphs for usage trends
- **Multi-tenant Support**: Separate tracking for different organizations
- **Performance Metrics**: Response time tracking and analysis
