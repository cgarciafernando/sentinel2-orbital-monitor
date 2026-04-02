# Sentinel-2 E2E Pipeline & Dashboard

This is a personal project built with the goal of developing a functional End-to-End (E2E) data pipeline from scratch. The system automates the download of satellite imagery, calculates vegetation indices (NDVI), and serves the data through an interactive dashboard.

<p align="center">
  <img src="assets/demo.gif" width="100%" alt="Dashboard Interactive Telemetry Demo">
</p>

The default target area is **El Arenosillo (Huelva, Spain)**, but it can be easily adapted to any geographical location by updating the *Bounding Box* coordinates.

## What does this project do?

The architecture is divided into two main components, both fully containerized using [Docker](https://www.docker.com/):

### 1. The ETL Pipeline & Format Transformations
An orchestrated workflow using [Prefect](https://www.prefect.io/) that follows a standard Bronze/Silver/Gold medallion architecture. In satellite imagery processing, **file formats are critical**, so the pipeline performs the following transformations:

* **Bronze (`.jp2`):** Connects to the [Copernicus API](https://dataspace.copernicus.eu/), filters out satellite passes with high cloud coverage (>15%), and downloads the B04 (Red) and B08 (NIR) bands. Sentinel-2 provides raw data in **JPEG 2000 (`.jp2`)**. This format is great for bandwidth compression but highly inefficient for mathematical array operations.
* **Silver (`.tif`):** Uses [Rasterio](https://rasterio.readthedocs.io/) and [Shapely](https://shapely.readthedocs.io/) to crop the global `.jp2` tile to the exact bounding box of the Area of Interest (AOI). The cropped image is saved as a **GeoTIFF (`.tif`)**, the GIS industry standard, allowing for faster matrix reads and preserving spatial metadata.
* **Gold (`.tif` + `.png`):** Applies map algebra to calculate the NDVI. The flow splits here because web browsers and scientific analysis speak different languages:
  * Generates a **32-bit float `.tif`** containing the pure mathematical index values (from -1.0 to 1.0).
  * Generates an **8-bit RGBA `.png`** applying a color palette. This is necessary because web mapping libraries (like Leaflet) cannot natively render raw TIFF files.

### 2. The Dashboard
A minimalist web interface built with [NiceGUI](https://nicegui.io/) and [Leaflet](https://leafletjs.com/).
* Automatically scans and loads processed datasets from the Gold directory.
* **Interactive Telemetry:** When clicking on the map, the frontend displays the colored `.png`, but the backend captures the latitude/longitude, transforms it to the satellite's native Coordinate Reference System (CRS) using [PyProj](https://pyproj4.github.io/), and instantly extracts the exact NDVI value by piercing through the hidden `.tif` file.

## Tech Stack
* **Python 3.12**
* **Data Engineering:** [Prefect](https://www.prefect.io/), Requests, Boto3 (AWS S3).
* **Geospatial Processing:** [Rasterio](https://rasterio.readthedocs.io/), [PyProj](https://pyproj4.github.io/), [Shapely](https://shapely.readthedocs.io/), [NumPy](https://numpy.org/).
* **Frontend:** [NiceGUI](https://nicegui.io/), [Leaflet.js](https://leafletjs.com/).
* **Infrastructure:** [Docker Compose](https://docs.docker.com/compose/).

## How to run it

You only need Docker and Docker Compose installed on your machine.

1. Clone this repository.
2. Set up your Copernicus Data Space credentials in your local environment or directly in the `docker-compose.yml` (`CDSE_USERNAME`, `CDSE_PASSWORD`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`).
3. Run the following command in the project root:

```bash
docker compose up --build
```

The pipeline will start downloading and processing data from recent weeks. You can access the real-time dashboard at `http://localhost:8501`.