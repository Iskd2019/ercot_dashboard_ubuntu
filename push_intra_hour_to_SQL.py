import json
import psycopg2
from config import DB_CONFIG
from psycopg2.extras import execute_values
from datetime import datetime
from pytz import timezone

# PostgreSQL connection
db_conn = psycopg2.connect(**DB_CONFIG)
cursor = db_conn.cursor()

# Load JSON
with open('Intra_Hour_ercot_filtered_forecast.json', 'r') as f:
    data = json.load(f)

local_tz = timezone('America/Chicago')
now = datetime.now()

# Prepare data
records = []
for item in data:
    # Parse and format timestamp string
    dt_local = local_tz.localize(datetime.strptime(item['IntervalEnding'], '%m/%d/%Y %H:%M'))
    interval_ending_str = dt_local.strftime('%Y-%m-%d %H:%M:%S')
    name_str = f"{interval_ending_str} {item['Model']} Forecast"

    record = (
        1,                         # create_uid
        1,                         # write_uid
        name_str,
        interval_ending_str,      # interval_ending as string
        item['Model'],
        now,                      # create_date
        now,                      # write_date
        item['SystemTotal']
    )
    records.append(record)

cursor.execute('TRUNCATE TABLE ercot_intra_hour_forecast;')

# 建议给表添加 UNIQUE 约束：ON (interval_ending, model)
insert_sql = """
INSERT INTO ercot_intra_hour_forecast (
    create_uid, write_uid, name, interval_ending,
    model, create_date, write_date, system_total
) VALUES %s
ON CONFLICT (interval_ending, model) DO UPDATE SET
    system_total = EXCLUDED.system_total,
    write_date = EXCLUDED.write_date
"""

# 如果你不希望更新，只跳过已存在项，可改为 DO NOTHING

execute_values(cursor, insert_sql, records)

db_conn.commit()
cursor.close()
db_conn.close()

print("✅ Intra-Hour forecast pushed with upsert!")