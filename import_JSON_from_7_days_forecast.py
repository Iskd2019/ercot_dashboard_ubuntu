import requests
import zipfile
import io
import pandas as pd
import json

# Step 1: Fetch metadata to build ZIP URL
metadata_url = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=12311"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(metadata_url, headers=headers)
data = response.json()

# Step 2: Extract the latest document info
latest_doc = data["ListDocsByRptTypeRes"]["DocumentList"][0]["Document"]
doc_id = latest_doc["DocID"]
zip_url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}"

# Step 3: Download and extract ZIP file
zip_response = requests.get(zip_url, headers=headers)
zip_file = zipfile.ZipFile(io.BytesIO(zip_response.content))

# Step 4: Read CSV, filter columns, convert to JSON
for file_name in zip_file.namelist():
    if file_name.endswith(".csv"):
        with zip_file.open(file_name) as f:
            df = pd.read_csv(f)

            # Filter to only the needed columns
            needed_columns = ["DeliveryDate" ,"HourEnding","SystemTotal"]
            df_filtered = df[needed_columns].copy()

            # Rename columns to remove spaces
            df_filtered.columns = ['DeliveryDate','HourEnding', 'SystemTotal']

            # Convert to list of dicts (JSON structure)
            filtered_json = df_filtered.to_dict(orient='records')

# Step 5: Save JSON to file
json_path = "7_Days_ercot_forecast_filtered.json"
with open(json_path, "w") as json_file:
    json.dump(filtered_json, json_file, indent=2)

print(f"✅ JSON saved as {json_path}")