
import pandas as pd
import json

# Load JSON file
input_path = "ercot_total_charging.json"  # Update path if needed
with open(input_path, "r") as f:
    data = json.load(f)

# Convert to DataFrame and parse timestamps
df = pd.DataFrame(data)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort and reset index
df = df.sort_values('timestamp').reset_index(drop=True)

# Group every 3 rows (i.e., 15 minutes) and compute the mean
df['group'] = df.index // 3
df['totalCharging'] = df.groupby('group')['totalCharging'].transform('mean').round(3)
df.drop(columns='group', inplace=True)

# Convert timestamp back to desired format: 'YYYY-MM-DD HH:MM:SS-0500'
df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S%z')

# Save the smoothed data to a new JSON file
output_path = "ercot_total_charging_15mins.json"
df.to_json(output_path, orient='records', indent=2)

print(f"✅ 15 mins data saved to: {output_path}")
