# Examples: Weather Data Access

This page contains examples of using DataIO to access and download weather data with spatial and temporal filtering.

## Setting up the Client

```python
from dataio import DataIOAPI

# Method 1: Using environment variables (recommended)
client = DataIOAPI()

# Method 2: Passing credentials directly
client = DataIOAPI(
    base_url="https://dataio.artpark.ai/api/v1",
    api_key="your_api_key_here",
    data_dir="data"
)
```

For Method 1, create a `.env` file in your project root:

```bash
DATAIO_API_BASE_URL=https://dataio.artpark.ai/api/v1
DATAIO_API_KEY=your_api_key_here
DATAIO_DATA_DIR=data
```

## Discovering Available Weather Datasets

List all available weather datasets to see what's accessible:

```python
# List weather datasets
datasets = client.list_weather_datasets()

print(f"Found {len(datasets)} weather dataset(s)\n")

for dataset in datasets:
    print(f"Dataset: {dataset['dataset_name']}")
    print(f"Time coverage: {dataset['temporal_coverage_start']} to {dataset['temporal_coverage_end']}")
    print(f"Spatial bounds: {dataset['spatial_bounds']}")
    print(f"\nAvailable variables:")

    for var in dataset['variables']:
        print(f"  {var['name']:<5} - {var['long_name']}")
        print(f"         Units: {var['units']}")
        print(f"         Resolution: {var['spatial_resolution']} (spatial), {var['temporal_resolution']} (temporal)")
    print("-" * 70)
```

**Example output:**
```
Found 1 weather dataset(s)

Dataset: era5_sfc
Time coverage: 2000-01-01T00:00:00 to 2025-11-01T06:00:00
Spatial bounds: {'min_lat': 5.0, 'max_lat': 45.0, 'min_lon': 40.0, 'max_lon': 110.0}

Available variables:
  d2m   - 2 metre dewpoint temperature
         Units: K
         Resolution: 0.2500 degrees (spatial), hourly (temporal)
  t2m   - 2 metre temperature
         Units: K
         Resolution: 0.2500 degrees (spatial), hourly (temporal)
  tp    - Total precipitation
         Units: m
         Resolution: 0.2500 degrees (spatial), hourly (temporal)
```

## Example 1: Download Weather Data for a State

Download temperature and precipitation data for Karnataka using the region ID:

```python
# Download weather data for Karnataka (state_29)
ds = client.download_weather_data(
    dataset_name="era5_sfc",
    variables=["t2m", "tp"],  # Temperature and precipitation
    start_date="2024-01-01",
    end_date="2024-01-31",
    geojson="state_29"  # Karnataka region ID - shapefile fetched automatically
)

print(f"Dataset dimensions: {dict(ds.dims)}")
print(f"Dataset variables: {list(ds.data_vars)}")

# Access temperature data
temperature = ds['t2m']
print(f"\nTemperature statistics:")
print(f"  Shape: {temperature.shape}")
print(f"  Mean: {temperature.mean().values:.2f} K ({temperature.mean().values - 273.15:.2f} °C)")
print(f"  Max: {temperature.max().values:.2f} K ({temperature.max().values - 273.15:.2f} °C)")
print(f"  Min: {temperature.min().values:.2f} K ({temperature.min().values - 273.15:.2f} °C)")

# Access precipitation data (convert from meters to mm)
precipitation = ds['tp'] * 1000
print(f"\nPrecipitation statistics:")
print(f"  Total: {precipitation.sum().values:.2f} mm")
print(f"  Mean per timestep: {precipitation.mean().values:.4f} mm")
```

**Example output:**
```
Weather data saved to: ./data/weather/era5_sfc/era5_sfc_t2m_tp_20240101_20240131_state_29.nc
Dataset dimensions: {'valid_time': 744, 'latitude': 65, 'longitude': 89}
Dataset variables: ['t2m', 'tp']

Temperature statistics:
  Shape: (744, 65, 89)
  Mean: 297.45 K (24.30 °C)
  Max: 308.92 K (35.77 °C)
  Min: 283.15 K (10.00 °C)

Precipitation statistics:
  Total: 12456.78 mm
  Mean per timestep: 0.2892 mm
```

## Example 2: Download for a Custom Bounding Box

Create a custom bounding box for a specific area of interest:

```python
# Define a custom bounding box for Western Ghats
western_ghats_bbox = {
    "type": "Feature",
    "properties": {"region_id": "western_ghats"},
    "geometry": {
        "type": "Polygon",
        "coordinates": [[
            [73.0, 8.0],   # Southwest corner
            [77.0, 8.0],   # Southeast corner
            [77.0, 16.0],  # Northeast corner
            [73.0, 16.0],  # Northwest corner
            [73.0, 8.0]    # Close the polygon
        ]]
    }
}

# Download data
ds = client.download_weather_data(
    dataset_name="era5_sfc",
    variables=["t2m", "d2m", "tp"],
    start_date="2024-06-01",
    end_date="2024-06-30",
    geojson=western_ghats_bbox,
    output_dir="./weather_data/monsoon_analysis"
)

print(f"Downloaded data covering:")
print(f"  Latitude range: {float(ds.latitude.min())}° to {float(ds.latitude.max())}°")
print(f"  Longitude range: {float(ds.longitude.min())}° to {float(ds.longitude.max())}°")
print(f"  Time points: {len(ds.valid_time)}")
print(f"  Variables: {list(ds.data_vars)}")
```

## Example 3: Using an Existing GeoJSON File

If you have a GeoJSON file for your region:

```python
# Download using a geojson file path
ds = client.download_weather_data(
    dataset_name="era5_sfc",
    variables=["t2m", "d2m"],
    start_date="2024-01-01",
    end_date="2024-01-07",
    geojson="path/to/my_region.geojson"
)

print(f"Weather data downloaded successfully!")
print(f"File saved with {len(ds.valid_time)} timesteps")
```

## Example 4: Time Series Analysis

Download and analyze temperature trends over time:

```python
import matplotlib.pyplot as plt

# Download monthly data
ds = client.download_weather_data(
    dataset_name="era5_sfc",
    variables=["t2m"],
    start_date="2024-01-01",
    end_date="2024-12-31",
    geojson="state_29"  # Karnataka
)

# Convert temperature to Celsius
ds['t2m_celsius'] = ds['t2m'] - 273.15

# Calculate daily mean temperature (spatial and temporal average)
daily_temp = ds['t2m_celsius'].resample(valid_time='1D').mean()
spatial_mean = daily_temp.mean(dim=['latitude', 'longitude'])

# Plot time series
plt.figure(figsize=(14, 6))
spatial_mean.plot()
plt.title('Daily Mean Temperature - Karnataka (2024)')
plt.xlabel('Date')
plt.ylabel('Temperature (°C)')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('karnataka_temperature_2024.png', dpi=300)
print("Temperature plot saved to karnataka_temperature_2024.png")

# Calculate monthly statistics
monthly_stats = {
    'mean': spatial_mean.resample(valid_time='1ME').mean().values,
    'max': spatial_mean.resample(valid_time='1ME').max().values,
    'min': spatial_mean.resample(valid_time='1ME').min().values
}

print("\nMonthly Temperature Statistics (°C):")
print(f"{'Month':<10} {'Mean':>8} {'Max':>8} {'Min':>8}")
print("-" * 40)
for i, month in enumerate(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']):
    print(f"{month:<10} {monthly_stats['mean'][i]:>8.2f} {monthly_stats['max'][i]:>8.2f} {monthly_stats['min'][i]:>8.2f}")
```

## Example 5: Precipitation Analysis

Analyze rainfall patterns:

```python
# Download monsoon season data
ds = client.download_weather_data(
    dataset_name="era5_sfc",
    variables=["tp"],
    start_date="2024-06-01",
    end_date="2024-09-30",
    geojson="state_29"  # Karnataka
)

# Convert precipitation from meters to millimeters
ds['tp_mm'] = ds['tp'] * 1000

# Calculate daily total precipitation
daily_precip = ds['tp_mm'].resample(valid_time='1D').sum()

# Calculate spatial average
spatial_mean_precip = daily_precip.mean(dim=['latitude', 'longitude'])

# Find wettest and driest days
wettest_day = spatial_mean_precip.idxmax().values
driest_day = spatial_mean_precip.idxmin().values

print(f"Monsoon Season Precipitation Analysis (June-September 2024)")
print(f"=" * 60)
print(f"Total rainfall: {spatial_mean_precip.sum().values:.2f} mm")
print(f"Average daily rainfall: {spatial_mean_precip.mean().values:.2f} mm")
print(f"Wettest day: {str(wettest_day)[:10]} - {float(spatial_mean_precip.sel(valid_time=wettest_day)):.2f} mm")
print(f"Driest day: {str(driest_day)[:10]} - {float(spatial_mean_precip.sel(valid_time=driest_day)):.2f} mm")

# Count rainy days (> 2.5 mm)
rainy_days = (spatial_mean_precip > 2.5).sum().values
print(f"Number of rainy days (>2.5mm): {rainy_days}")
```

## Example 6: Extracting Data for Specific Locations

Extract weather data for specific cities:

```python
# Download data for Karnataka region
ds = client.download_weather_data(
    dataset_name="era5_sfc",
    variables=["t2m", "d2m", "tp"],
    start_date="2024-01-01",
    end_date="2024-01-31",
    geojson="state_29"
)

# Define city coordinates
cities = {
    'Bangalore': (12.97, 77.59),
    'Mysore': (12.29, 76.64),
    'Mangalore': (12.91, 74.85),
    'Hubli': (15.36, 75.13)
}

# Extract data for each city
import pandas as pd

city_data = {}
for city_name, (lat, lon) in cities.items():
    city_ds = ds.sel(
        latitude=lat,
        longitude=lon,
        method='nearest'
    )

    # Create dataframe
    df = city_ds.to_dataframe().reset_index()
    df['t2m_celsius'] = df['t2m'] - 273.15
    df['d2m_celsius'] = df['d2m'] - 273.15
    df['tp_mm'] = df['tp'] * 1000

    city_data[city_name] = df

    # Calculate summary statistics
    print(f"\n{city_name}:")
    print(f"  Avg Temperature: {df['t2m_celsius'].mean():.2f} °C")
    print(f"  Avg Dewpoint: {df['d2m_celsius'].mean():.2f} °C")
    print(f"  Total Precipitation: {df['tp_mm'].sum():.2f} mm")

    # Save to CSV
    output_file = f"{city_name.lower()}_weather_jan2024.csv"
    df[['valid_time', 't2m_celsius', 'd2m_celsius', 'tp_mm']].to_csv(
        output_file,
        index=False
    )
    print(f"  Data saved to: {output_file}")
```

## Example 7: Spatial Visualization

Create spatial maps of weather data:

```python
import matplotlib.pyplot as plt

# Download a short time period for visualization
ds = client.download_weather_data(
    dataset_name="era5_sfc",
    variables=["t2m", "tp"],
    start_date="2024-01-15",
    end_date="2024-01-15",
    geojson="state_29"
)

# Convert units
ds['t2m_celsius'] = ds['t2m'] - 273.15
ds['tp_mm'] = ds['tp'] * 1000

# Calculate daily average
temp_avg = ds['t2m_celsius'].mean(dim='valid_time')
precip_total = ds['tp_mm'].sum(dim='valid_time')

# Create subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Plot temperature
temp_avg.plot(ax=ax1, cmap='RdYlBu_r')
ax1.set_title('Average Temperature - Karnataka (2024-01-15)')
ax1.set_xlabel('Longitude')
ax1.set_ylabel('Latitude')

# Plot precipitation
precip_total.plot(ax=ax2, cmap='Blues')
ax2.set_title('Total Precipitation - Karnataka (2024-01-15)')
ax2.set_xlabel('Longitude')
ax2.set_ylabel('Latitude')

plt.tight_layout()
plt.savefig('karnataka_weather_map_20240115.png', dpi=300)
print("Weather map saved to karnataka_weather_map_20240115.png")
```

## Example 8: Batch Download for Multiple Regions

Download weather data for multiple states:

```python
# Define multiple states
states = {
    'Karnataka': 'state_29',
    'Tamil Nadu': 'state_33',
    'Kerala': 'state_32',
    'Andhra Pradesh': 'state_28'
}

# Download data for each state
for state_name, region_id in states.items():
    print(f"\nDownloading data for {state_name}...")

    try:
        ds = client.download_weather_data(
            dataset_name="era5_sfc",
            variables=["t2m", "tp"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            geojson=region_id,
            output_dir=f"./weather_data/{state_name.lower().replace(' ', '_')}"
        )

        # Quick summary
        temp_mean = (ds['t2m'].mean().values - 273.15)
        precip_total = (ds['tp'].sum().values * 1000)

        print(f"  ✓ Downloaded successfully")
        print(f"  Average temperature: {temp_mean:.2f} °C")
        print(f"  Total precipitation: {precip_total:.2f} mm")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
```

## Working with Large Datasets

For large downloads, consider processing data in chunks:

```python
import pandas as pd

# Download year of data month by month
months = pd.date_range('2024-01-01', '2024-12-31', freq='MS')

monthly_results = []

for start_date in months:
    end_date = start_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)

    print(f"Downloading {start_date.strftime('%B %Y')}...")

    ds = client.download_weather_data(
        dataset_name="era5_sfc",
        variables=["t2m"],
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        geojson="state_29"
    )

    # Calculate monthly average
    monthly_avg = (ds['t2m'].mean().values - 273.15)
    monthly_results.append({
        'month': start_date.strftime('%B'),
        'avg_temp': monthly_avg
    })

    print(f"  Average temperature: {monthly_avg:.2f} °C")

# Create summary dataframe
summary_df = pd.DataFrame(monthly_results)
print("\nAnnual Temperature Summary:")
print(summary_df)
```

## Tips

- Start with small time ranges (1-7 days) when exploring new datasets
- Use region IDs for convenience: `geojson="state_29"` instead of loading shapefiles
- Check data size: hourly data for a month at 0.25° resolution can be several hundred MB
- Convert units: Temperature K to °C: `temp_c = temp_k - 273.15`, Precipitation m to mm: `precip_mm = precip_m * 1000`
- Use `resample()` for temporal aggregation: `daily = ds.resample(valid_time='1D').mean()`
- Extract point data with `sel()`: `point = ds.sel(latitude=12.97, longitude=77.59, method='nearest')`
