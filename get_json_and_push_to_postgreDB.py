import subprocess
#一次运行所有files
files = [
    "import JSON from 7 days forecast.py",
    "import JSON from intra hour forecast.py",
    "import JSON from supply_demand.py",
    "import_JSON_from_ESR.py",
    "push 7 day JSON to PostSQL.py",
    "push_daily_json_to_SQL.py",
    "push_intra_hour_to_SQL.py",
    "get_15_mins_average.py",
    "push_daily_15mins_to_SQL.py",
    "get_15_mins_ESR.py",
    "subtract_ESR_from_load.py",
    "push_ercot_demand_plus_totalCharging_to_SQL.py"
]

for file in files:
    subprocess.run(["python", file])