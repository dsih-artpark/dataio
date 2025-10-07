#!/usr/bin/env python3
"""
Example script demonstrating the DataIO usage tracking system.

This script shows how to:
1. Use the usage tracking service directly
2. Query usage statistics
3. Export data to CSV
4. Clean up old data

Run this script after starting the API server to see usage tracking in action.
"""

import os
import sys
import time

# Add the src directory to the path so we can import dataio modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dataio.api.services.usage_tracking_service import UsageTrackingService


def main():
    """Demonstrate usage tracking functionality."""

    print("🔍 DataIO Usage Tracking Example")
    print("=" * 50)

    # Initialize the usage tracking service
    db_path = "example_usage_tracking.db"
    usage_service = UsageTrackingService(db_path)

    print(f"📊 Initialized usage tracking database: {db_path}")

    # Simulate some usage data
    print("\n📝 Simulating some usage data...")

    # Simulate different types of requests
    sample_requests = [
        {
            "user_email": "user1@example.com",
            "method_type": "SDK",
            "endpoint": "/api/v1/datasets",
            "http_method": "GET",
            "dataset_id": None,
            "bucket_type": None,
            "region_id": None,
            "user_agent": "python-requests/2.28.1",
            "ip_address": "192.168.1.100",
            "response_status": 200,
        },
        {
            "user_email": "user1@example.com",
            "method_type": "SDK",
            "endpoint": "/api/v1/datasets/TS0001DS0001/STANDARDISED/tables",
            "http_method": "GET",
            "dataset_id": "TS0001DS0001",
            "bucket_type": "STANDARDISED",
            "region_id": None,
            "user_agent": "python-requests/2.28.1",
            "ip_address": "192.168.1.100",
            "response_status": 200,
        },
        {
            "user_email": "user2@example.com",
            "method_type": "CLI",
            "endpoint": "/api/v1/datasets/TS0001DS0002/STANDARDISED/tables",
            "http_method": "GET",
            "dataset_id": "TS0001DS0002",
            "bucket_type": "STANDARDISED",
            "region_id": None,
            "user_agent": "dataio-cli/1.0.0",
            "ip_address": "192.168.1.101",
            "response_status": 200,
        },
        {
            "user_email": "user3@example.com",
            "method_type": "API",
            "endpoint": "/api/v1/shapefiles/state_36",
            "http_method": "GET",
            "dataset_id": None,
            "bucket_type": None,
            "region_id": "state_36",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "ip_address": "192.168.1.102",
            "response_status": 200,
        },
        {
            "user_email": "user1@example.com",
            "method_type": "SDK",
            "endpoint": "/api/v1/datasets/TS0001DS0003/PREPROCESSED/tables",
            "http_method": "GET",
            "dataset_id": "TS0001DS0003",
            "bucket_type": "PREPROCESSED",
            "region_id": None,
            "user_agent": "python-requests/2.28.1",
            "ip_address": "192.168.1.100",
            "response_status": 403,
        },
    ]

    # Log the sample requests
    for i, request in enumerate(sample_requests):
        usage_service.log_usage(**request)
        print(
            f"  ✅ Logged request {i + 1}: {request['method_type']} - {request['endpoint']}"
        )
        time.sleep(0.1)  # Small delay to ensure different timestamps

    print("\n📈 Usage Statistics")
    print("-" * 30)

    # Get overall usage statistics
    stats = usage_service.get_usage_stats()

    print(f"Total Requests: {stats['total_requests']}")
    print(f"Success Rate: {stats['success_rate']}%")

    print("\nBy Method Type:")
    for method, count in stats["by_method"].items():
        print(f"  {method}: {count}")

    print("\nBy Endpoint:")
    for endpoint, count in stats["by_endpoint"].items():
        print(f"  {endpoint}: {count}")

    print("\nBy User:")
    for user, count in stats["by_user"].items():
        print(f"  {user}: {count}")

    print("\nBy Dataset:")
    for dataset, count in stats["by_dataset"].items():
        print(f"  {dataset}: {count}")

    print("\nBy Response Status:")
    for status, count in stats["by_status"].items():
        print(f"  {status}: {count}")

    # Get user-specific activity
    print("\n👤 User Activity for user1@example.com")
    print("-" * 40)

    user_activity = usage_service.get_user_activity("user1@example.com")
    print(f"Total Requests: {user_activity['total_requests']}")
    print("Recent Activity:")
    for activity in user_activity["recent_activity"][:3]:
        print(
            f"  {activity['datetime'][:19]} - {activity['method_type']} - {activity['endpoint']} - Status: {activity['response_status']}"
        )

    # Get dataset-specific usage
    print("\n📊 Dataset Usage for TS0001DS0001")
    print("-" * 35)

    dataset_usage = usage_service.get_dataset_usage("TS0001DS0001")
    print(f"Total Downloads: {dataset_usage['total_downloads']}")
    print(f"Unique Users: {dataset_usage['unique_users']}")
    print("By Method:")
    for method, count in dataset_usage["by_method"].items():
        print(f"  {method}: {count}")

    # Export to CSV
    print("\n💾 Exporting data to CSV...")
    csv_file = "example_usage_export.csv"
    exported_count = usage_service.export_to_csv(csv_file)
    print(f"Exported {exported_count} records to {csv_file}")

    # Show file size
    if os.path.exists(csv_file):
        file_size = os.path.getsize(csv_file)
        print(f"CSV file size: {file_size} bytes")

    print("\n🧹 Database Information")
    print("-" * 25)

    # Show database info
    import sqlite3

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM usage_logs")
    total_records = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(datetime), MAX(datetime) FROM usage_logs")
    date_range = cursor.fetchone()

    cursor.execute("SELECT COUNT(DISTINCT user_email) FROM usage_logs")
    unique_users = cursor.fetchone()[0]

    conn.close()

    print(f"Total Records: {total_records}")
    print(f"Unique Users: {unique_users}")
    if date_range[0] and date_range[1]:
        print(f"Date Range: {date_range[0][:19]} to {date_range[1][:19]}")

    print("\n✅ Example completed successfully!")
    print(f"📁 Database file: {db_path}")
    print(f"📄 CSV export: {csv_file}")
    print("\nYou can now:")
    print(f"  - View the database with: sqlite3 {db_path}")
    print("  - Open the CSV file in Excel or any spreadsheet application")
    print(f"  - Use the CLI commands: uv run dataio usage stats --db {db_path}")
    print("  - Use the admin API endpoints to query this data")


if __name__ == "__main__":
    main()
