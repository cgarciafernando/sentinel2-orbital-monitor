import boto3
import os
import json

# Initialize S3 client targeting the Copernicus Data Space Ecosystem custom endpoint
s3 = boto3.client(
    's3',
    endpoint_url='https://eodata.dataspace.copernicus.eu',
    aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
    region_name='default'
)

output_dir = 'data/bronze'
os.makedirs(output_dir, exist_ok=True)

# Load product metadata previously fetched from the Copernicus OData API
with open('data/bronze/bronze_arenosillo.json', 'r') as f:
    images_metadata = json.load(f)

# Extract the S3 prefix path for the selected product, formatting it for boto3 compatibility
product_key = images_metadata[0]['S3Path'].replace('/eodata/', '').strip('/')

print(f"Exploring contents of {product_key}...")
response = s3.list_objects_v2(Bucket='eodata', Prefix=product_key)

for item in response.get('Contents', []):
    file_key = item['Key']

    # Filter strictly for B04 (Red) and B08 (Near-Infrared) bands.
    # These are the specific electromagnetic frequencies required for NDVI calculation.
    if ('.jp2' in file_key) and ('B04' in file_key or 'B08' in file_key):
        
        # Ensure we target the actual high-resolution spectral matrices
        # and ignore low-res previews or metadata files located outside IMG_DATA.
        if 'IMG_DATA' in file_key:
            local_filename = file_key.split('/')[-1]
            local_filepath = os.path.join(output_dir, local_filename)
            
            print(f"Downloading {local_filename}...")
            try:
                s3.download_file('eodata', file_key, local_filepath)
                print(f"Successfully saved to: {local_filepath}")
            except Exception as e:
                print(f"Failed to download {local_filename}: {e}")

print("Download process completed.")