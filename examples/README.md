# DataIO Examples

This directory contains example scripts demonstrating various features of the DataIO platform.

## Usage Tracking Example

### `usage_tracking_example.py`

Demonstrates the usage tracking system by:

1. **Creating a sample database** with usage tracking data
2. **Logging sample requests** with different user types and endpoints
3. **Querying usage statistics** to show various analytics
4. **Exporting data to CSV** for external analysis
5. **Showing database information** and file sizes

### Running the Example

```bash
# From the project root directory
cd examples
python usage_tracking_example.py
```

### What You'll See

The script will:

- Create a SQLite database with sample usage data
- Show comprehensive usage statistics
- Display user activity and dataset usage
- Export data to CSV format
- Provide information about the generated files

### Output Files

After running the example, you'll have:

- `example_usage_tracking.db` - SQLite database with usage data
- `example_usage_export.csv` - CSV export of the usage data

### Using the Generated Data

You can then use the CLI commands to explore the data:

```bash
# View statistics
uv run dataio usage stats --db examples/example_usage_tracking.db

# View user activity
uv run dataio usage user user1@example.com --db examples/example_usage_tracking.db

# View dataset usage
uv run dataio usage dataset TS0001DS0001 --db examples/example_usage_tracking.db

# Export to different format
uv run dataio usage export --db examples/example_usage_tracking.db --output my_export.csv
```

### Database Queries

You can also query the database directly with SQLite:

```bash
# Open the database
sqlite3 examples/example_usage_tracking.db

# Run some queries
SELECT * FROM usage_logs LIMIT 5;
SELECT user_email, COUNT(*) FROM usage_logs GROUP BY user_email;
SELECT method_type, COUNT(*) FROM usage_logs GROUP BY method_type;
```

This example provides a hands-on way to understand how the usage tracking system works and what kind of analytics it can provide.
