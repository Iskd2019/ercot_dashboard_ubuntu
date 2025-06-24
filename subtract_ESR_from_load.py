import pandas as pd
import json

# Load demand data
with open("ercot_15min_aligned_avg.json", "r") as f:
    demand_data = json.load(f)

# Load totalCharging data
with open("ercot_total_charging_15mins.json", "r") as f:
    charging_data = json.load(f)

# Convert to DataFrames
df_demand = pd.DataFrame(demand_data)
df_charging = pd.DataFrame(charging_data)

# Convert timestamps to datetime
df_demand['timestamp'] = pd.to_datetime(df_demand['timestamp'])
df_charging['timestamp'] = pd.to_datetime(df_charging['timestamp'])

# Merge on timestamp
df_merged = pd.merge(df_demand, df_charging, on='timestamp', how='left')

# Add totalCharging to demand (fill NaN with 0)
df_merged['demand'] = df_merged['demand'] + df_merged['totalCharging'].fillna(0)

# Drop the totalCharging column
df_merged.drop(columns=['totalCharging'], inplace=True)

# Format timestamps back to original format
df_merged['timestamp'] = df_merged['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S%z')

# Save to JSON
output_path = "ercot_demand_plus_totalCharging.json"
with open(output_path, "w") as f:
    json.dump(df_merged.to_dict(orient='records'), f, indent=2)

print(f"✅ Combined data saved to {output_path}")