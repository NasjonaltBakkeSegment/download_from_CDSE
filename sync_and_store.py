#!/usr/bin/env python3
"""
Script that expands on the query and download functionality.
Checks if queried products are in directory.
If not, these are download and stored according to the following structutre:
    platform/year/month/day/product_type

TO DO:
[x] func to unzip and store in correct file structure
[x] func to remove zip
[] func to check difference between storage and queried products
[x] add line to remove JSON wheen not needed anymore

"""
import argparse
from lib.metadata_products import Metadata_products
from lib.utils import load_config, init_logging, get_dict_satellites_and_product_types
from lib.download_products import download_product, get_access_token, unzip_and_store
import sys
import os

(
    username,
    password,
    output_dir,
    polygon_wkt,
    valid_satellites,
    polygon
) = load_config()

# Log to console
logger = init_logging()

def main(args):
           
    start_date = args.start_date
    end_date = args.end_date
    
    if args.sat not in valid_satellites:
        logger.info(f"------Invalid 'sat' value. Valid values are: {', '.join(valid_satellites)}------")
        sys.exit(1)
    
    satellites_and_product_types = get_dict_satellites_and_product_types(args.sat)          

    # try:
    #     access_token = get_access_token()
    #     # Do something with the access token here
    # except Exception as e:
    #     # Print the error message and exit
    #     logger.error(e) 
    #     exit(1)  # Exit with a non-zero status code to indicate an error


    
    for satellite, productTypes in satellites_and_product_types.items():
        for productType in productTypes:
            metadata_products = Metadata_products(satellite, productType, start_date, end_date)
            metadata_products.harvest_all_products_to_json()
            metadata_products.get_product_ids_and_titles()
            metadata_products.create_storage_paths()
            # Filter out products that are already stored
            filtered_products, filtered_storage_paths = metadata_products.filter_out_synced_products()
            for (product_id, product_title), (product_id, storage_path) in zip(filtered_products.items(), filtered_storage_paths.items()):
                # get or refresh access token if neccessary 
                access_token = get_access_token()
                download_product(product_id, product_title, access_token)
                unzip_and_store(product_title, storage_path)

    # Remove JSON once all products are downloaded and stored
    logger.info(f"------Removing: {metadata_products.filepath}------")
    os.remove(metadata_products.filepath)

    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to downloaded products from CDSE between two given dates")

    parser.add_argument("--start_date", type=str, required=True, help="First date you want to download products for (yyyymmdd)")
    parser.add_argument("--end_date", type=str, required=True, help="First date you want to download products for (yyyymmdd)")
    parser.add_argument("--sat", type=str, required=True, help="For which satellite do you want to harvest products?", choices=valid_satellites)
    
    args = parser.parse_args()
    main(args)

