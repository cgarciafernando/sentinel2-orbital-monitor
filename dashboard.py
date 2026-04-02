from nicegui import ui
import os
import rasterio
from rasterio.warp import transform_bounds
from pyproj import Transformer
import numpy as np
from PIL import Image
import base64
from io import BytesIO

GOLD_DIR = 'data/gold'

# --- 1. FILE I/O ---
def get_gold_files():
    if not os.path.exists(GOLD_DIR): return []
    return sorted([f for f in os.listdir(GOLD_DIR) if f.endswith('.tif')], reverse=True)

def image_to_base64(path):
    # Convert PNG to base64 string. 
    # This avoids local file routing issues and CORS policies in the browser,
    # ensuring the Leaflet overlay renders reliably without needing a static file server.
    with Image.open(path) as img:
        img_rgba = img.convert("RGBA")
        buffered = BytesIO()
        img_rgba.save(buffered, format="PNG")
        return f"data:image/png;base64,{base64.b64encode(buffered.getvalue()).decode()}"

# --- 2. UI & LOGIC ---
@ui.page('/')
def main_page():
    # Inject custom CSS to force a brutalist, terminal-like dark mode 
    # and adjust the Leaflet cursor for precision targeting.
    ui.add_head_html('''
        <style>
            .leaflet-container { background: #000 !important; cursor: crosshair !important; }
            .leaflet-image-layer { pointer-events: none !important; }
            .nicegui-content { padding: 0 !important; }
            body { background-color: #000; overflow: hidden; }
        </style>
    ''')

    page_state = {
        'selected_date': None,
        'overlay': None
    }

    # HEADER
    with ui.header().classes('bg-black border-b border-zinc-900 items-center justify-between px-8 py-4'):
        ui.label('SENTINEL-2 // ORBITAL_MONITOR').classes('text-xs tracking-[0.6em] font-bold text-white')
        ui.label('ONLINE').classes('text-[9px] border border-zinc-800 px-3 py-1 text-emerald-500')

    # MAIN LAYOUT
    with ui.row().classes('w-full h-screen gap-0 no-wrap'):
        
        # SIDE PANEL (MISSION CTRL)
        with ui.column().classes('w-1/5 h-full p-8 border-r border-zinc-900 bg-black'):
            ui.label('MISSION_CTRL').classes('text-[10px] text-zinc-600 tracking-[0.3em] mb-8')
            ui.button('REFRESH', on_click=lambda: ui.navigate.to('/')).props('flat').classes('text-[10px] border border-zinc-800 text-white w-full hover:bg-zinc-900 mb-6')
            
            select_mission = ui.select([], label='DATA_SET').props('dark standout dense square text-xs').classes('w-full')
            
            ui.separator().classes('bg-zinc-900 my-8')
            ui.label('GEOPOS').classes('text-[10px] text-zinc-600 tracking-widest mb-2')
            lbl_coords = ui.label('---.--- N / ---.--- W').classes('text-xs text-zinc-400 font-mono')

        # MAP CANVAS 
        with ui.column().classes('w-[55%] h-full relative border-r border-zinc-900 bg-black'):
            m = ui.leaflet(center=(37.10, -6.725), zoom=13).classes('w-full h-full')
            # Using CartoDB dark matter basemap to enhance the contrast of the NDVI layer
            m.tile_layer(url_template='https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png')

        # TELEMETRY PANEL 
        with ui.column().classes('w-1/4 h-full p-12 gap-12 bg-black'):
            ui.label('ANALYSIS').classes('text-[10px] text-zinc-600 tracking-[0.4em]')
            
            with ui.column().classes('w-full gap-2'):
                ui.label('NDVI_INDEX').classes('text-[10px] text-zinc-600')
                lbl_ndvi = ui.label('0.0000').classes('text-7xl text-white font-light tracking-tighter')
            
            with ui.column().classes('w-full gap-2'):
                ui.label('BIO_STATUS').classes('text-[10px] text-zinc-600')
                lbl_status = ui.label('STANDBY').classes('text-xs text-white border-l border-zinc-800 pl-4 uppercase')

    # --- 3. EVENT HANDLERS ---

    def load_overlay(filename):
        if not filename: return
        page_state['selected_date'] = filename
        path_tif = os.path.join(GOLD_DIR, filename)
        path_png = path_tif.replace("NDVI_", "VISUAL_").replace(".tif", ".png")
        
        if os.path.exists(path_tif) and os.path.exists(path_png):
            with rasterio.open(path_tif) as src:
                # Transform the native CRS bounds to WGS84 (EPSG:4326) so Leaflet knows where to place the image bounds
                b = transform_bounds(src.crs, 'EPSG:4326', *src.bounds)
                
                if page_state['overlay']:
                    try: m.remove_layer(page_state['overlay'])
                    except: pass
                
                # interactive: False is critical here. It ensures the overlay image doesn't intercept 
                # mouse click events, allowing them to pass through to the base map handler.
                page_state['overlay'] = m.image_overlay(url=image_to_base64(path_png), bounds=[[b[1], b[0]], [b[3], b[2]]], options={'interactive': False})
                m.set_center(((b[1]+b[3])/2, (b[0]+b[2])/2))

    def handle_map_click(e):
        try:
            lat = e.args['latlng']['lat']
            lng = e.args['latlng']['lng']
            
            lbl_coords.set_text(f"{lat:.5f} N / {lng:.5f} W")
            
            if not page_state['selected_date']:
                ui.notify("Waiting for data...", position='bottom-right')
                return
                
            path_tif = os.path.join(GOLD_DIR, page_state['selected_date'])
            with rasterio.open(path_tif) as src:
                # Convert web map coordinates (WGS84) back to the satellite's native UTM projection
                # to accurately pierce the 32-bit TIFF matrix and extract the mathematical NDVI value.
                transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
                x, y = transformer.transform(lng, lat)
                row, col = src.index(x, y)
                
                if 0 <= row < src.height and 0 <= col < src.width:
                    val = src.read(1)[row, col]
                    if not np.isnan(val):
                        lbl_ndvi.set_text(f"{val:.4f}")
                        
                        # Biological classification based on standard NDVI thresholds
                        if val >= 0.6:
                            lbl_status.set_text("DENSE_FOREST")
                        elif val >= 0.4:
                            lbl_status.set_text("MODERATE_VEG")
                        elif val >= 0.2:
                            lbl_status.set_text("SHRUBS / GRASS")
                        elif val >= 0.05:
                            lbl_status.set_text("SPARSE_VEG")
                        elif val >= 0.0:
                            lbl_status.set_text("BARREN_SOIL")
                        else:
                            lbl_status.set_text("WATER_BODY")
                        return 
                        
            lbl_ndvi.set_text("0.0000")
            lbl_status.set_text("OUT_OF_BOUNDS")

        except Exception as ex:
            ui.notify(f"System Error: {ex}", position='bottom-right', type='negative')

    # --- 4. INITIALIZATION ---
    m.on('map-click', handle_map_click)
    
    select_mission.on_value_change(lambda e: load_overlay(e.value))
    
    files = get_gold_files()
    if files:
        # Separate the backend value (filename) from the UI representation (formatted date).
        # This improves UX by showing human-readable dates without breaking the underlying file routing logic.
        date_options = {}
        for f in files:
            raw_date = f.replace("NDVI_", "").replace(".tif", "")
            formatted_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            date_options[f] = formatted_date
            
        select_mission.options = date_options
        select_mission.value = files[0]
        ui.timer(0.5, lambda: load_overlay(files[0]), once=True)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='Sentinel Command', dark=True, port=8501, show=False)