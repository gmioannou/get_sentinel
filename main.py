
import os.path
from cdsetool.query import query_features, shape_to_wkt
from cdsetool.credentials import Credentials
from cdsetool.download import download_features
from cdsetool.monitor import StatusMonitor
from datetime import date

def out_folder(orbit_number):
    current_dir = os.path.curdir
    if orbit_number == 160:
        out_dir = os.path.join(current_dir, "Ascending")
    elif orbit_number == 167:
        out_dir = os.path.join(current_dir, "Descending")

    return(out_dir)
    


def query_products(from_date, to_date, orbit_number, aoi):
    features = query_features(
        "Sentinel1",
        {
            "platform": "S1A",
            "productType": "SLC",
            "sensorMode": "IW",
            "processingLevel": "LEVEL1",
            "relativeOrbitNumber": orbit_number,
            "startDate": from_date,
            "completionDate": to_date,
            "geometry": aoi,
        },
    )

    features_list = list(features)
    features_list.sort(key=lambda f: f['properties'].get('startDate', ''))
    
    for f in features_list:
        print(f['properties']['title'])

    return features_list

def download_products(features, orbit_number):
    if orbit_number == 160:
        out_folder = os.path.join(os.path.curdir, "./products/ASC")
    elif orbit_number == 167:
        out_folder = os.path.join(os.path.curdir, "./products/DESC")
        
    os.makedirs(out_folder, exist_ok=True)
    
    list(
        download_features(
            features,
            out_folder,
            {
                "concurrency": 3,
                "monitor": StatusMonitor(),
                "credentials": Credentials(),
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

def download_orbit_files(features, orbit_type):
    out_folder = os.path.join(os.path.curdir, "./orbits/" + orbit_type)
    os.makedirs(out_folder, exist_ok=True)

    list(
        download_features(
            features,
            out_folder,
            {
                "concurrency": 3,
                "monitor": StatusMonitor(),
                "credentials": Credentials(),
            },
        )
    )
    
def main():

    orbit_number = 160  # 160 Ascending, 167 Descending
    aoi = shape_to_wkt("./aoi/aoi.shp")
    
    from_date = "2025-12-21"
    to_date = "2026-01-21" 
    # to_date = date.today()

    
    ## Query and download Sentinel-1 orbit files
    #
    
    # orbit_files = query_orbit_files(from_date, to_date, "RESORB")
    # download_orbit_files(orbit_files, "RESORB")
    
    orbit_files = query_orbit_files(from_date, to_date, "POEORB")    
    download_orbit_files(orbit_files, "POEORB")
    
    print("Orbit files downloaded...")
    
    ## Query and download Sentinel-1 products
    #
    
    products = query_products(from_date, to_date, orbit_number, aoi)
    download_products(products, orbit_number)
    
    print("Product files downloaded...")
            

if __name__ == "__main__":
    main()
    