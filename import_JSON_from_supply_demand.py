import requests
import json

# URL of the JSON data
json_url = "https://www.ercot.com/api/1/services/read/dashboards/supply-demand.json"

# Fetch the JSON data
response = requests.get(json_url)

if response.status_code == 200:
    data = response.json()

    if 'data' in data:
        reduced_data = []

        for entry in data['data']:
            reduced_entry = {
                "timestamp": entry.get("timestamp"),
                "demand": entry.get("demand"),
                "forecast": entry.get("forecast")
            }
            reduced_data.append(reduced_entry)

        # Save the reduced data to a new JSON file
        with open("ercot_reduced_supply_demand.json", "w") as f:
            json.dump(reduced_data, f, indent=2)

        print("✅ Reduced data saved to ercot_reduced_supply_demand.json")
    else:
        print("❌ 'data' key not found in the JSON response")
else:
    print(f"❌ Failed to fetch data: {response.status_code}")