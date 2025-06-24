import requests
import zipfile
import io
import pandas as pd
import json

# Step 1: Fetch metadata to build ZIP URL
url = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=16553"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)
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

            # Filter columns and rename
            df_filtered = df[["IntervalEnding", "SystemTotal", "Model"]].copy()
            df_filtered.columns = ["IntervalEnding", "SystemTotal", "Model"]

            # Convert to JSON
            result = df_filtered.to_dict(orient='records')

            with open("Intra_Hour_ercot_filtered_forecast.json", "w") as out:
                json.dump(result, out, indent=2)

print("✅ JSON saved as Intra_Hour_ercot_filtered_forecast.json")