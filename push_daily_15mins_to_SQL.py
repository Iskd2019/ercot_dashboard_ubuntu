import json
import psycopg2
from config import DB_CONFIG
from psycopg2.extras import execute_values
from datetime import datetime

# PostgreSQL connection
db_conn = psycopg2.connect(**DB_CONFIG)
cursor = db_conn.cursor()

# Load JSON
with open('ercot_15min_aligned_avg.json', 'r') as f:
    data = json.load(f)

now = datetime.now()
records = []

for item in data:
    # 解析原始 timestamp（带或不带时区都行）
    dt = datetime.strptime(item['timestamp'], '%Y-%m-%d %H:%M:%S%z').replace(tzinfo=None)
    timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    name_str = f"{timestamp_str} Forecast"

    record = (
        item['forecast'],       # forecast
        1,                      # create_uid
        1,                      # write_uid
        name_str,
        timestamp_str,          # timestamp as string
        now,                    # create_date
        now,                    # write_date
        item['demand']
    )
    records.append(record)
insert_sql = """
INSERT INTO ercot_supply_demand_15min (
    forecast, create_uid, write_uid, name,
    timestamp, create_date, write_date, demand
) VALUES %s
ON CONFLICT ("timestamp") DO UPDATE SET
    forecast = EXCLUDED.forecast,
    write_uid = EXCLUDED.write_uid,
    name = EXCLUDED.name,
    demand = EXCLUDED.demand,
    write_date = EXCLUDED.write_date;
"""

execute_values(cursor, insert_sql, records)

db_conn.commit()
cursor.close()
db_conn.close()

print("✅ ERCOT 15-min supply/demand data pushed with upsert.")