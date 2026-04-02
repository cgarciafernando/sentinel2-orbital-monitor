import os
import requests
import json

# Authenticate via Keycloak to retrieve the OAuth2 Bearer token required for OData API access.
username = os.getenv('CDSE_USERNAME')
password = os.getenv('CDSE_PASSWORD')

auth_payload = {
    'client_id': 'cdse-public',
    'grant_type': 'password',
    'username': username,
    'password': password
}

token_url = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
auth_response = requests.post(token_url, data=auth_payload).json()
token = auth_response.get('access_token')

# Ensure the raw data (Bronze) layer directory exists before ingestion.
output_dir = 'data/bronze'
os.makedirs(output_dir, exist_ok=True)

catalog_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
headers = {'Authorization': f'Bearer {token}'}

# OData filter applied to exclude orbital passes with >15% cloud cover.
# Sentinel-2 uses optical sensors that cannot penetrate clouds, which would corrupt the NDVI output.
cloud_filter = "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le 15.0)"

# Define the spatial and temporal query.
# The polygon defines a strict Bounding Box (WGS84) targeting the El Arenosillo area.
params = {
    "$filter": f"Collection/Name eq 'SENTINEL-2' and ContentDate/Start gt 2026-03-01T00:00:00.000Z and {cloud_filter} and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON((-6.75 37.08, -6.70 37.08, -6.70 37.12, -6.75 37.12, -6.75 37.08))')",
    "$top": 5,
    "$orderby": "ContentDate/Start desc"
}

# Execute the query against the Copernicus catalogue to retrieve product metadata.
response = requests.get(catalog_url, headers=headers, params=params).json()
images = response.get('value', [])

print(f"Found {len(images)} valid images:")
for image in images:
    print(f"- {image.get('Name')}")

# Store the metadata locally. 
# The S3 paths within this JSON will be parsed by the AWS client to download the actual .jp2 matrices.
json_path = os.path.join(output_dir, 'bronze_arenosillo.json')
with open(json_path, 'w') as f:
    json.dump(images, f, indent=4)

print(f"Metadata successfully saved to: {json_path}")