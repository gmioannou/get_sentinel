
import os.path
import json
import logging

from cdsetool.query import query_features
from cdsetool.download import download_features
from cdsetool.monitor import StatusMonitor
from shapely.geometry import shape
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="get_sentinel.log",
    filemode="a"
)

def query_products(from_date, to_date, orbit_direction, aoi):
    """
    Query Sentinel-1 SLC products for a given orbit direction and AOI.

    Args:
        from_date (str): Start date for the query (YYYY-MM-DD).
        to_date (str): End date for the query (YYYY-MM-DD).
        orbit_direction (str): 'ASCENDING' or 'DESCENDING'.
        aoi (dict or str): GeoJSON geometry or WKT string for the area of interest.

    Returns:
        list: List of product features sorted by start date.

    Scope:
        Use in workflows needing filtered Sentinel-1 SLC product lists for further processing or downloading. 
        Only products matching the AOI and orbit direction within the date range are returned.
    """
    
    products = query_features(
        "Sentinel1",
        {
            "platform": "S1A",
            "productType": "SLC",
            "sensorMode": "IW",
            "processingLevel": "LEVEL1",
            "orbitDirection": orbit_direction,
            "startDate": from_date,
            "completionDate": to_date,
            "geometry": aoi,
        },
    )

    products_list = list(products)
    products_list.sort(key=lambda f: f['properties'].get('startDate', ''))
            
    return products_list

def download_products(products, orbit_direction):

    """
    Download Sentinel-1 products to a local folder based on orbit direction.

    Args:
        products (list): List of product features to download.
        orbit_direction (str): 'ASCENDING' or 'DESCENDING'.

    Returns:
        None

    Scope:
        Saves products to './products/ASCENDING' or './products/DESCENDING' depending on orbit direction. 
        Intended for use after querying products for batch downloading. 
        Handles folder creation and concurrent downloads.
    """
    
    out_folder = os.path.join(os.path.curdir, "./products/" + orbit_direction)    
    os.makedirs(out_folder, exist_ok=True)
    

    downloads = download_features(
        products,
        out_folder,
        {
            "concurrency": 4,
            "monitor": StatusMonitor(),
        },
    )
    
    logging.info(f"Download {orbit_direction} products ({len(products)})")
    for id in downloads:
        logging.info(f"{id}")
    
def query_orbit_files(from_date, to_date, orbit_type):
    """
    Query Sentinel-1 AUX orbit files of type AUX_POEORB or AUX_RESORB.

    Args:
        from_date (str): Start date for the query (YYYY-MM-DD).
        to_date (str): End date for the query (YYYY-MM-DD).
        orbit_type (str): Orbit file type ('POEORB' or 'RESORB').

    Returns:
        list: List of orbit file features matching the criteria.

    Scope:
        Use in workflows needing filtered Sentinel-1 AUX orbit files for further processing or downloading. 
        Only files matching the type and date range are returned.
    """
    
    orbit_files = query_features(
        "Sentinel1",
        {
            "platform": "S1A",
            "productType": f"AUX_{orbit_type}",
            "startDate": from_date,
            "completionDate": to_date,
        },
    )

    return orbit_files

def download_orbit_files(orbit_files, orbit_type):
    """
    Download Sentinel-1 AUX orbit files to a local folder based on orbit type.

    Args:
        orbit_files (list): List of orbit file features to download.
        orbit_type (str): Orbit file type ('POEORB' or 'RESORB').

    Returns:
        None

    Scope:
        Saves orbit files to './orbits/POEORB' or './orbits/RESORB' depending on orbit type. 
        Intended for use after querying orbit files for batch downloading. 
        Handles folder creation and concurrent downloads.
    """

    out_folder = os.path.join(os.path.curdir, "./orbits/" + orbit_type)
    os.makedirs(out_folder, exist_ok=True)

    downloads = download_features(
        orbit_files,
        out_folder,
        {
            "concurrency": 4,
            "monitor": StatusMonitor(),
        },
    )
    
    logging.info(f"Download {orbit_type} orbit files ({len(orbit_files)})")
    for id in downloads:
        logging.info(f"{id}")
    
def main():
    
    from_date = "2026-01-01"
    # to_date = "2026-01-07"
    to_date = date.today().strftime("%Y-%m-%d")
    
    # Load area of interest (AOI) from GeoJSON file
    with open("./aoi.json") as f:
        aoi_data = json.load(f)
    aoi = shape(aoi_data).wkt
        
    # Query and download Sentinel-1 orbit files (POEORB / RESORB)
    orbit_files = query_orbit_files(from_date, to_date, "POEORB")    
    download_orbit_files(orbit_files, "POEORB")
    
    # Query and download Sentinel-1 products
    for orbit_direction in ["ASCENDING", "DESCENDING"]:
        products = query_products(from_date, to_date, orbit_direction, aoi)
        download_products(products, orbit_direction)
    
if __name__ == "__main__":
    main()
    