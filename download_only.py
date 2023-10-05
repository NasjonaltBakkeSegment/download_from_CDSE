#!/usr/bin/env python3
from lib.metadata_products import Metadata_products
from lib.utils import load_config, init_logging
from lib.download_products import download_product, get_access_token
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

def main():
           
    if len(sys.argv) != 2:
        logger.info(f"------------Please provide the absolute filepath to a JSON of products. This should include in the filename either Sentinel-1, Sentinel-2, Sentinel-3, or Sentinel-5-------------")
        sys.exit(1)
    source = sys.argv[1]
    if not os.path.exists(source):
        logger.info(f"------------File {source} does not exist-------------")
        sys.exit(1)
    if not source.endswith('.json'):
        logger.info(f"------------File {source} is not a JSON file-------------")
        sys.exit(1)
    
    metadata_products = Metadata_products(json_filepath = source)
    metadata_products.load_json()
    products = metadata_products.get_product_ids_and_titles()
    
    for product_id,product_title in products.items():
        try:
            access_token = get_access_token()
            # Do something with the access token here
        except Exception as e:
            # Print the error message and exit
            logger.error(e) 
            exit(1)  # Exit with a non-zero status code to indicate an error
        
        download_product(product_id, product_title, access_token)

if __name__ == "__main__":
    main()






