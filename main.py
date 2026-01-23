
import os.path
import json
from cdsetool.query import query_features
from cdsetool.download import download_features
from cdsetool.monitor import StatusMonitor
from shapely.geometry import shape
from datetime import date

def query_products(from_date, to_date, orbit_direction, aoi):
    
    # Query Sentinel-1 SLC products for given orbit direction and AOI
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
    
    # Download products to local folder "./products/ASCENDING" or "./products/DESCENDING"
    out_folder = os.path.join(os.path.curdir, "./products/" + orbit_direction)    
    os.makedirs(out_folder, exist_ok=True)
    
    list(
        download_features(
            products,
            out_folder,
            {
                "concurrency": 4,
                "monitor": StatusMonitor(),
            },
        )
    )
    
def query_orbit_files(from_date, to_date, orbit_type):

    # Query orbit AUX as Sentinel1 products of type AUX_POEORB / AUX_RESORB
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
    
    # Download orbit files to local folder "./orbits/POEORB" or "./orbits/RESORB"
    out_folder = os.path.join(os.path.curdir, "./orbits/" + orbit_type)
    os.makedirs(out_folder, exist_ok=True)

    list(
        download_features(
            orbit_files,
            out_folder,
            {
                "concurrency": 4,
                "monitor": StatusMonitor(),
            },
        )
    )
    
def main():
    
    from_date = "2026-01-01"
    to_date = "2026-01-11" 
    # to_date = date.today()

    ## Load area of interest (AOI) from GeoJSON file
    with open("./aoi.json") as f:
        aoi_data = json.load(f)
        
    aoi = shape(aoi_data).wkt
        
    ## Query and download Sentinel-1 orbit files
    # POEORB / RESORB
    
    orbit_files = query_orbit_files(from_date, to_date, "POEORB")    
    download_orbit_files(orbit_files, "POEORB")
    
    ## Query and download Sentinel-1 products
    
    for orbit_direction in ["ASCENDING", "DESCENDING"]:
        products = query_products(from_date, to_date, orbit_direction, aoi)
        download_products(products, orbit_direction)

if __name__ == "__main__":
    main()
    