import json
import psycopg2
from config import DB_CONFIG
from psycopg2.extras import execute_values
from datetime import datetime

# Load the JSON result
with open("ercot_demand_plus_totalCharging.json", "r") as f:
    records = json.load(f)

now = datetime.now()

# Prepare data for insert
data_to_insert = []
for row in records:
    dt_obj = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S%z')
    timestamp_str = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
    name_str = f"{timestamp_str} Forecast"

    record = (
        row['forecast'],
        1,                  # create_uid
        1,                  # write_uid
        name_str,
        timestamp_str,
        now,                # create_date
        now,                # write_date
        row['demand']
    )
    data_to_insert.append(record)

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

sql = """
INSERT INTO ercot_demand_plus_totalcharging (
    forecast, create_uid, write_uid, name,
    timestamp, create_date, write_date, demand
)
VALUES %s
ON CONFLICT ("timestamp") DO UPDATE SET
    forecast = EXCLUDED.forecast,
    write_uid = EXCLUDED.write_uid,
    name = EXCLUDED.name,
    demand = EXCLUDED.demand,
    write_date = EXCLUDED.write_date;
"""

execute_values(cur, sql, data_to_insert)
conn.commit()
cur.close()
conn.close()

print("✅ ercot_demand_plus_totalcharging pushed with upsert.")