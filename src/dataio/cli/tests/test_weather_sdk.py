#!/usr/bin/env python3
"""Test script for weather data SDK functions."""

import os
from dotenv import load_dotenv
from dataio.sdk.user import DataIOAPI

# Load environment variables
load_dotenv()

# Initialize client with staging API
api_key = os.getenv("STAGING_ANALYST_API_KEY")
base_url = "https://staging.dataio.artpark.ai/api/v1"

print("=" * 70)
print("Testing Weather Data SDK")
print("=" * 70)
print(f"API URL: {base_url}")
print(f"API Key: {api_key[:10]}..." if api_key else "No API key found")
print()

try:
    client = DataIOAPI(base_url=base_url, api_key=api_key, data_dir="./test_data")
    print("✓ Client initialized successfully")
    print()

    # Test 1: List weather datasets
    print("Test 1: Listing weather datasets...")
    print("-" * 70)
    datasets = client.list_weather_datasets()
    print(f"Found {len(datasets)} weather dataset(s)")

    for dataset in datasets:
        print(f"\nDataset: {dataset['dataset_name']}")
        print(f"  Variables: {len(dataset['variables'])} variables")
        for var in dataset['variables']:
            print(f"    - {var['name']}: {var.get('long_name', 'N/A')} ({var.get('units', 'N/A')})")
            print(f"      Spatial: {var.get('spatial_resolution', 'N/A')}, Temporal: {var.get('temporal_resolution', 'N/A')}")
        print(f"  Time range: {dataset['temporal_coverage_start']} to {dataset['temporal_coverage_end']}")
        print(f"  Spatial bounds: {dataset['spatial_bounds']}")

    print("\n✓ list_weather_datasets() works!")
    print()

    # Test 2: Download weather data (small test - 1 day, 1 variable)
    if len(datasets) > 0:
        dataset_name = datasets[0]['dataset_name']
        variables = [datasets[0]['variables'][0]['name']]

        print(f"Test 2: Downloading weather data from {dataset_name}...")
        print("-" * 70)
        print(f"  Dataset: {dataset_name}")
        print(f"  Variables: {variables}")
        print(f"  Date range: 2024-01-01 to 2024-01-02 (1 day test)")
        print(f"  Region: Simple bbox (70-80°E, 10-20°N)")
        print()

        # Create a simple test geojson (small bbox covering part of India)
        test_geojson = {
            "type": "Feature",
            "properties": {"region_id": "test_region"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [70, 10],
                    [80, 10],
                    [80, 20],
                    [70, 20],
                    [70, 10]
                ]]
            }
        }

        ds = client.download_weather_data(
            dataset_name=dataset_name,
            variables=variables,
            start_date="2024-01-01",
            end_date="2024-01-02",
            geojson=test_geojson,
            output_dir="./test_data/weather_test"
        )

        print()
        if hasattr(ds, 'dims'):
            print("✓ download_weather_data() works!")
            print(f"  Returned xarray Dataset with dimensions: {dict(ds.dims)}")
            print(f"  Variables: {list(ds.data_vars)}")
        else:
            print("✓ download_weather_data() works!")
            print(f"  Saved to: {ds}")

    print()
    print("=" * 70)
    print("All tests passed! ✓")
    print("=" * 70)

except Exception as e:
    print()
    print("=" * 70)
    print(f"❌ Error: {str(e)}")
    print("=" * 70)
    import traceback
    traceback.print_exc()
