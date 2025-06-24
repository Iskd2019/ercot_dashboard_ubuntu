import json
import psycopg2
from config import DB_CONFIG
from psycopg2.extras import execute_values
from datetime import datetime

# PostgreSQL connection
db_conn = psycopg2.connect(**DB_CONFIG)
cursor = db_conn.cursor()

# Load JSON
with open('ercot_reduced_supply_demand.json', 'r') as f:
    data = json.load(f)

# Prepare data
records = [
    (
        datetime.strptime(item['timestamp'], '%Y-%m-%d %H:%M:%S%z'),
        item['demand'],
        item['forecast']
    )
    for item in data
]

# Insert with upsert (to avoid duplicates)
insert_sql = """
INSERT INTO ercot_supply_demand (timestamp, demand, forecast)
VALUES %s
ON CONFLICT (timestamp) DO UPDATE SET
    demand = EXCLUDED.demand,
    forecast = EXCLUDED.forecast;
"""

execute_values(cursor, insert_sql, records)

db_conn.commit()
cursor.close()
db_conn.close()

print("✅ ERCOT supply/demand data inserted!")