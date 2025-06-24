import requests
import json

url = "https://www.ercot.com/api/1/services/read/dashboards/energy-storage-resources.json"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()

    result = []

    for section in ["previousDay", "currentDay"]:
        if section in data and "data" in data[section]:
            for item in data[section]["data"]:
                result.append({
                    "timestamp": item.get("timestamp"),
                    "totalCharging": item.get("totalCharging")
                })

    # Save to JSON file
    with open("ercot_total_charging.json", "w") as f:
        json.dump(result, f, indent=2)

    print("✅ Data saved to ercot_total_charging.json")

else:
    print(f"❌ Failed to fetch data: {response.status_code}")
