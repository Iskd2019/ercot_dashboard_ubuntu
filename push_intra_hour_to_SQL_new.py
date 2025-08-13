import json
import math
from collections import OrderedDict
from datetime import datetime
from pytz import timezone
import psycopg2
from psycopg2.extras import execute_values
from config import DB_CONFIG

# --- Settings ---
LOCAL_TZ = timezone('America/Chicago')
TABLE = 'ercot_intra_hour_forecast'
USE_TIMESTAMPTZ = True  # set False if your column is timestamp without time zone

# --- Load JSON ---
with open('Intra_Hour_ercot_filtered_forecast.json', 'r') as f:
    data = json.load(f)

# --- Dedupe & clean ---
# Keep one row per (IntervalEnding, Model); prefer later row, and always prefer numeric over NaN
dedup = OrderedDict()
for item in data:
    key = (item['IntervalEnding'], item['Model'])
    sys_total = item.get('SystemTotal')
    is_nan = (sys_total is None) or (isinstance(sys_total, float) and math.isnan(sys_total))

    if key not in dedup:
        dedup[key] = item
    else:
        prev = dedup[key]
        prev_val = prev.get('SystemTotal')
        prev_is_nan = (prev_val is None) or (isinstance(prev_val, float) and math.isnan(prev_val))
        # Prefer non-NaN; if both non-NaN, keep the later one in the file
        if prev_is_nan and not is_nan:
            dedup[key] = item
        elif not prev_is_nan and not is_nan:
            dedup[key] = item
        # else keep prev (both NaN)

now_ct = datetime.now(LOCAL_TZ)
records = []
for (interval_str, model), item in dedup.items():
    sys_total = item.get('SystemTotal')
    if sys_total is None or (isinstance(sys_total, float) and math.isnan(sys_total)):
        continue  # drop NaN rows entirely

    dt_local = LOCAL_TZ.localize(datetime.strptime(interval_str, '%m/%d/%Y %H:%M'))
    dt_for_db = dt_local if USE_TIMESTAMPTZ else dt_local.replace(tzinfo=None)

    name_str = f"{dt_local.strftime('%Y-%m-%d %H:%M:%S')} {model} Forecast"
    records.append((
        1,              # create_uid
        1,              # write_uid
        name_str,       # name
        dt_for_db,      # interval_ending (datetime)
        model,          # model
        now_ct,         # create_date
        now_ct,         # write_date
        float(sys_total)  # system_total
    ))

if not records:
    print("Nothing to insert (after dedupe/NaN filtering).")
    raise SystemExit(0)

insert_sql = f"""
INSERT INTO {TABLE} (
  create_uid, write_uid, name, interval_ending,
  model, create_date, write_date, system_total
) VALUES %s
ON CONFLICT (interval_ending, model) DO UPDATE SET
  system_total = EXCLUDED.system_total,
  write_date   = EXCLUDED.write_date,
  name         = EXCLUDED.name
"""

with psycopg2.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:
        # Keep the truncate you want
        cur.execute(f'TRUNCATE TABLE {TABLE};')
        execute_values(cur, insert_sql, records)

print(f"✅ Truncated and upserted {len(records)} rows into {TABLE}.")