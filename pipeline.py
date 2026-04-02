import os
import requests
import json
import boto3
import rasterio
import numpy as np
from prefect import task, flow, get_run_logger
from rasterio.mask import mask
from shapely.geometry import box
from rasterio.warp import transform_geom
import matplotlib.cm as cm
from rasterio.transform import from_origin

# --- CONFIGURATION & UTILITIES ---
OUTPUT_BRONZE = 'data/bronze'
OUTPUT_SILVER = 'data/silver'
OUTPUT_GOLD = 'data/gold'

# Bounding Box (WGS84) for El Arenosillo, Huelva
ARENOSILLO_COORDS = (-6.75, 37.08, -6.70, 37.12)

# --- ETL TASKS ---

@task(retries=3, retry_delay_seconds=30)
def fetch_and_download_bronze():
    logger = get_run_logger()
    os.makedirs(OUTPUT_BRONZE, exist_ok=True)
    
    # 1. Authentication via Keycloak
    auth_data = {
        'client_id': 'cdse-public',
        'grant_type': 'password',
        'username': os.getenv('CDSE_USERNAME'),
        'password': os.getenv('CDSE_PASSWORD')
    }
    token = requests.post('https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token', data=auth_data).json().get('access_token')
    
    # 2. OData Query Configuration
    # OData filter applied to exclude orbital passes with >15% cloud cover.
    # Sentinel-2 uses optical sensors that cannot penetrate clouds, which would corrupt the NDVI output.
    cloud_filter = "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le 15.0)"
    
    params = {
        "$filter": f"Collection/Name eq 'SENTINEL-2' and ContentDate/Start gt 2025-08-01T00:00:00.000Z and {cloud_filter} and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON(({ARENOSILLO_COORDS[0]} {ARENOSILLO_COORDS[1]}, {ARENOSILLO_COORDS[2]} {ARENOSILLO_COORDS[1]}, {ARENOSILLO_COORDS[2]} {ARENOSILLO_COORDS[3]}, {ARENOSILLO_COORDS[0]} {ARENOSILLO_COORDS[3]}, {ARENOSILLO_COORDS[0]} {ARENOSILLO_COORDS[1]}))')",
        "$top": 4, 
        "$orderby": "ContentDate/Start desc" 
    }
    
    response = requests.get("https://catalogue.dataspace.copernicus.eu/odata/v1/Products", headers={'Authorization': f'Bearer {token}'}, params=params).json()
    images = response.get('value', [])
    
    if not images:
        logger.warning("No cloud-free images found in the specified date range.")
        return False
        
    with open(os.path.join(OUTPUT_BRONZE, 'metadata.json'), 'w') as f:
        json.dump(images, f, indent=4)
        
    # 3. S3 Ingestion
    s3 = boto3.client('s3', endpoint_url='https://eodata.dataspace.copernicus.eu', aws_access_key_id=os.getenv('S3_ACCESS_KEY'), aws_secret_access_key=os.getenv('S3_SECRET_KEY'), region_name='default')
    
    for image in images:
        product_key = image['S3Path'].replace('/eodata/', '').strip('/')
        s3_response = s3.list_objects_v2(Bucket='eodata', Prefix=product_key)
        
        for item in s3_response.get('Contents', []):
            file_path = item['Key']
            # Target specifically the high-res Red and NIR matrices inside the IMG_DATA folder
            if ('.jp2' in file_path) and ('B04' in file_path or 'B08' in file_path) and 'IMG_DATA' in file_path:
                filename = file_path.split('/')[-1]
                
                # Incremental load: prevent re-downloading existing historical data to save bandwidth
                if not os.path.exists(os.path.join(OUTPUT_BRONZE, filename)):
                    logger.info(f"Downloading {filename} (Clear sky condition)...")
                    s3.download_file('eodata', file_path, os.path.join(OUTPUT_BRONZE, filename))
                else:
                    logger.info(f"Skipping {filename}, already exists in Bronze layer.")
    return True

@task
def process_silver(success):
    if not success: return False
    logger = get_run_logger()
    os.makedirs(OUTPUT_SILVER, exist_ok=True)
    
    # Define the exact geographical Bounding Box to mask the global tile.
    # This drastically reduces memory consumption during subsequent matrix operations.
    bbox = box(*ARENOSILLO_COORDS)
    geo_json = json.loads(json.dumps(bbox.__geo_interface__))
    
    for f in os.listdir(OUTPUT_BRONZE):
        if f.endswith('.jp2'):
            out_path = os.path.join(OUTPUT_SILVER, f.replace('.jp2', '.tif'))
            if os.path.exists(out_path): continue 
            
            with rasterio.open(os.path.join(OUTPUT_BRONZE, f)) as src:
                # Project WGS84 bounding box to the satellite's native UTM Coordinate Reference System
                transformed_shape = [transform_geom('EPSG:4326', src.crs, geo_json)]
                out_image, out_transform = mask(src, transformed_shape, crop=True)
                
                out_meta = src.meta.copy()
                out_meta.update({"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2], "transform": out_transform})
                
                with rasterio.open(out_path, "w", **out_meta) as dest:
                    dest.write(out_image)
    return True

@task(log_prints=True)
def process_gold(success):
    if not success:
        print("Silver phase failed or skipped. Aborting Gold phase.")
        return False
    
    logger = get_run_logger()
    os.makedirs(OUTPUT_GOLD, exist_ok=True)
    
    tifs = [f for f in os.listdir(OUTPUT_SILVER) if f.endswith('.tif')]
    
    # Group corresponding B04 and B08 files by extracting their unique acquisition dates
    unique_dates = set()
    for f in tifs:
        parts = f.split('_')
        for part in parts:
            if len(part) >= 8 and part[:8].isdigit():
                unique_dates.add(part[:8])
                break

    if not unique_dates:
        logger.error("No valid dates found in Silver layer.")
        return False

    # Process NDVI dynamically for every date detected in the Silver layer
    for date_str in unique_dates:
        logger.info(f"Processing mission dataset for date: {date_str}")
        
        b04_files = [os.path.join(OUTPUT_SILVER, f) for f in tifs if 'B04' in f and date_str in f]
        b08_files = [os.path.join(OUTPUT_SILVER, f) for f in tifs if 'B08' in f and date_str in f]
        
        if not b04_files or not b08_files:
            logger.warning(f"Missing spectral bands for date {date_str}. Skipping.")
            continue
            
        path_b04 = b04_files[0]
        path_b08 = b08_files[0]
        
        tif_out = os.path.join(OUTPUT_GOLD, f"NDVI_{date_str}.tif")
        png_out = os.path.join(OUTPUT_GOLD, f"VISUAL_{date_str}.png")
        
        if os.path.exists(tif_out) and os.path.exists(png_out):
            logger.info(f"NDVI products for {date_str} already exist. Skipping.")
            continue

        with rasterio.open(path_b04) as r_src, rasterio.open(path_b08) as n_src:
            red = r_src.read(1, masked=True).astype('float32')
            nir = n_src.read(1, masked=True).astype('float32')
            
            # Calculate NDVI. Ignore division errors over water bodies.
            np.seterr(divide='ignore', invalid='ignore')
            ndvi = (nir - red) / (nir + red)
            
            valid_data = ndvi.compressed()
            valid_data = valid_data[valid_data > -1.0] 

            if valid_data.size > 0:
                # Dynamic contrast stretching for web visualization
                p2, p98 = np.percentile(valid_data, [2, 98])
                ndvi_viz = np.clip((ndvi - p2) / (p98 - p2), 0, 1)
            else:
                ndvi_viz = ndvi.filled(0)

            colormap = cm.get_cmap('RdYlGn')
            ndvi_rgba = (colormap(ndvi_viz) * 255).astype('uint8')
            
            # Apply transparency mask to deep water and nodata regions
            alpha = np.where(ndvi.mask | (ndvi < -0.5), 0, 255).astype('uint8')
            
            # 1. Save 32-bit analytical TIF for the dashboard telemetry queries
            meta_tif = r_src.meta.copy()
            meta_tif.update(dtype='float32', count=1, driver='GTiff', nodata=np.nan)
            with rasterio.open(tif_out, 'w', **meta_tif) as dst:
                dst.write(ndvi.filled(np.nan), 1)

            # 2. Save 8-bit RGBA PNG for lightweight frontend map rendering
            meta_png = r_src.meta.copy()
            meta_png.update(dtype='uint8', count=4, driver='PNG', nodata=None)
            with rasterio.open(png_out, 'w', **meta_png) as dst:
                dst.write(ndvi_rgba[:, :, 0], 1)
                dst.write(ndvi_rgba[:, :, 1], 2)
                dst.write(ndvi_rgba[:, :, 2], 3)
                dst.write(alpha, 4)

    logger.info("Gold layer processing completed for all available dates.")
    return True

# --- ORCHESTRATION FLOW ---
@flow(name="Sentinel-2 El Arenosillo Pipeline")
def spatial_pipeline():
    status_bronze = fetch_and_download_bronze()
    status_silver = process_silver(status_bronze)
    process_gold(status_silver)

if __name__ == "__main__":
    print("🚀 Initializing Pipeline execution...")
    spatial_pipeline() 
    
    print("📡 Pipeline entering Polling mode (Cron schedule active)...")
    # Deploys a persistent background process that triggers the ETL every Monday at 09:00 AM
    spatial_pipeline.serve(
        name="daily-sentinel-check",
        cron="0 9 * * 1",
        description="Weekly automated monitor for El Arenosillo"
    )