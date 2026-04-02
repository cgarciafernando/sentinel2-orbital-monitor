import os
import rasterio
import numpy as np

input_dir = 'data/silver'
output_dir = 'data/gold'
os.makedirs(output_dir, exist_ok=True)

def apply_contrast_stretch(data, p_low=2, p_high=98):
    # Apply a dynamic contrast stretch using percentiles.
    # This removes extreme outliers (like lingering cloud artifacts or deep water shadows)
    # and scales the remaining data to an 8-bit (0-255) format suitable for web map rendering.
    clean_data = np.nan_to_num(data, nan=0.0)
    vmin, vmax = np.percentile(clean_data, [p_low, p_high])
    
    if vmax == vmin:
        return (clean_data * 0).astype('uint8')
        
    stretched = np.clip((clean_data - vmin) / (vmax - vmin) * 255, 0, 255)
    return stretched.astype('uint8')

files = os.listdir(input_dir)

# Isolate the required spectral bands. 
# B04 (Red) and B08 (Near-Infrared) are the specific frequencies needed for biomass calculation.
b04_path = [os.path.join(input_dir, f) for f in files if 'B04' in f][0]
b08_path = [os.path.join(input_dir, f) for f in files if 'B08' in f][0]

with rasterio.open(b04_path) as red_src, rasterio.open(b08_path) as nir_src:
    red = red_src.read(1).astype('float32')
    nir = nir_src.read(1).astype('float32')
    meta = red_src.meta.copy()

    # Calculate Normalized Difference Vegetation Index (NDVI)
    # Formula: (NIR - RED) / (NIR + RED)
    # Suppress warnings for division by zero, which naturally occurs over water bodies or nodata regions.
    np.seterr(divide='ignore', invalid='ignore')
    ndvi = (nir - red) / (nir + red)

    # --- 1. Scientific Output (Gold TIF) ---
    # Save the pure mathematical index as a 32-bit float GeoTIFF.
    # This preserves the raw index values (-1.0 to 1.0) and spatial metadata required for exact telemetry queries.
    meta.update(dtype='float32', count=1, driver='GTiff')
    with rasterio.open(os.path.join(output_dir, 'ndvi_arenosillo.tif'), 'w', **meta) as dest:
        dest.write(ndvi, 1)

    # --- 2. Visual Output (Web PNGs) ---
    # Downsample to 8-bit and switch the driver to PNG. 
    # Web browsers (and Leaflet) cannot natively render heavy GeoTIFFs, so these act as lightweight visual overlays.
    meta.update(dtype='uint8', driver='PNG')
    
    with rasterio.open(os.path.join(output_dir, 'ndvi_visual.png'), 'w', **meta) as dest:
        dest.write(apply_contrast_stretch(ndvi), 1)

    with rasterio.open(os.path.join(output_dir, 'red_visual.png'), 'w', **meta) as dest:
        dest.write(apply_contrast_stretch(red), 1)

print("Gold layer processing completed.")