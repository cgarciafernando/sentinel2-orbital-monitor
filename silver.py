import os
import rasterio
from rasterio.mask import mask
from shapely.geometry import box
import json
from rasterio.warp import transform_geom

output_dir = 'data/silver'
os.makedirs(output_dir, exist_ok=True)

# Define the Area of Interest (AOI) Bounding Box using WGS84 coordinates (Lat/Lon)
min_lon, min_lat, max_lon, max_lat = -6.75, 37.08, -6.70, 37.12
bbox = box(min_lon, min_lat, max_lon, max_lat)
geo_json = [json.loads(json.dumps(bbox.__geo_interface__))]

bronze_dir = 'data/bronze'
files = [f for f in os.listdir(bronze_dir) if f.endswith('.jp2')]

for file in files:
    input_path = os.path.join(bronze_dir, file)
    # Convert format from JPEG 2000 (.jp2) to GeoTIFF (.tif)
    # GeoTIFF is the GIS industry standard, optimizing I/O read speeds for the matrix operations in the Gold layer.
    output_path = os.path.join(output_dir, file.replace('.jp2', '.tif'))
    
    print(f"Processing Silver layer for: {file}...")
    
    with rasterio.open(input_path) as src:
        # Reproject the WGS84 bounding box to the satellite tile's native UTM Coordinate Reference System (CRS).
        # Sentinel-2 data is projected in UTM; cropping will fail if the spatial reference systems do not match.
        geo_json_transformed = [transform_geom('EPSG:4326', src.crs, geo_json[0])]
        
        # Apply the spatial mask to crop the global tile down to the exact AOI.
        # This drastically reduces the memory footprint and compute time for the downstream NDVI calculations.
        out_image, out_transform = mask(src, geo_json_transformed, crop=True)
        out_meta = src.meta.copy()
        
        out_meta.update({
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })
        
        with rasterio.open(output_path, "w", **out_meta) as dest:
            dest.write(out_image)
            print(f"Saved to Silver: {output_path}")

print("Silver layer processing completed.")