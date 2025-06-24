import json
import psycopg2
from config import DB_CONFIG
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

db_conn = psycopg2.connect(**DB_CONFIG)
cursor = db_conn.cursor()

# Load JSON
with open('7_days_ercot_forecast_filtered.json', 'r') as f:
    data = json.load(f)

# Helper to convert DeliveryDate + HourEnding → datetime object
def convert_to_datetime(delivery_date_str, hour_ending_str):
    dt_date = datetime.strptime(delivery_date_str, '%m/%d/%Y')
    hour = int(hour_ending_str.split(':')[0])
    if hour == 24:
        dt_date += timedelta(days=1)
        hour = 0
    return dt_date.replace(hour=hour)

# Current timestamp for create/write fields
now = datetime.now()

# Prepare data for batch insert
records = []
for item in data:
    dt = convert_to_datetime(item['DeliveryDate'], item['HourEnding'])
    timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    name_str = f"{timestamp_str} Forecast"

    record = (
        1,  # create_uid
        1,  # write_uid
        name_str,
        timestamp_str,     # timestamp as string
        now,               # create_date
        now,               # write_date
        item['SystemTotal']
    )
    records.append(record)

# Optional: truncate existing records
#cursor.execute('TRUNCATE TABLE ercot_seven_day_forecast;')
'''
insert_sql = """
INSERT INTO ercot_seven_day_forecast (
    create_uid, write_uid, name, timestamp,
    create_date, write_date, system_total
) VALUES %s
"""
'''

insert_sql = """
INSERT INTO ercot_seven_day_forecast (
    create_uid, write_uid, name, timestamp,
    create_date, write_date, system_total
) VALUES %s
ON CONFLICT (timestamp) DO UPDATE SET
    system_total = EXCLUDED.system_total,
    write_date = EXCLUDED.write_date
"""

execute_values(cursor, insert_sql, records)

db_conn.commit()
cursor.close()
db_conn.close()

print("✅ Updated ERCOT 7-day forecast data pushed to production DB!")