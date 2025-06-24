import json
import pandas as pd
from datetime import datetime

def process_ercot_json(input_file, output_file):
    # Load JSON data
    with open(input_file, 'r') as f:
        data = json.load(f)

    # Convert to DataFrame
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Align timestamps to start of 15-minute intervals
    def align_15min_group(ts):
        minute = ts.minute
        aligned_minute = (minute // 15) * 15
        return ts.replace(minute=aligned_minute, second=0, microsecond=0)

    df['interval_start'] = df['timestamp'].apply(align_15min_group)

    # Separate forecast and actual for independent grouping
    actual_df = df[df['forecast'] == 0].copy()
    forecast_df = df[df['forecast'] == 1].copy()

    # Compute average demand for each 15-minute block (actuals)
    actual_grouped = actual_df.groupby('interval_start')['demand'].mean().reset_index()
    actual_grouped['forecast'] = 0

    # Compute average demand for each 15-minute block (forecasted)
    forecast_grouped = forecast_df.groupby('interval_start')['demand'].mean().reset_index()
    forecast_grouped['forecast'] = 1

    # Combine grouped results
    grouped = pd.concat([actual_grouped, forecast_grouped])

    # Map averaged demand back to each row
    def get_avg_demand(row):
        match = grouped[
            (grouped['interval_start'] == row['interval_start']) &
            (grouped['forecast'] == row['forecast'])
        ]
        return match['demand'].values[0] if not match.empty else row['demand']

    df['demand'] = df.apply(get_avg_demand, axis=1).round(2)

    # Drop helper column
    df.drop(columns=['interval_start'], inplace=True)

    # Convert to list of dicts
    output_data = df.to_dict(orient='records')

    # Save to output file
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2, default=str)

# Example usage
if __name__ == '__main__':
    input_path = 'ercot_reduced_supply_demand.json'
    output_path = 'ercot_15min_aligned_avg.json'
    process_ercot_json(input_path, output_path)
    print(f"✅ 15 mins interval JSON saved as {output_path}")
