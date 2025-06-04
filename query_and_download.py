#!/usr/bin/env python3
import argparse
from lib.metadata_products import Metadata_products
from lib.utils import load_values_from_config, init_logging, get_dict_satellites_and_product_types
from lib.download_products import download_product, get_access_token
import sys

(
    username,
    password,
    output_dir,
    polygon_wkt,
    valid_satellites,
    polygon,
    product_types_csv
) = load_values_from_config()

# Log to console
logger = init_logging()

def main(args):

    start_date = args.start_date
    end_date = args.end_date

    if args.sat not in valid_satellites:
        logger.info(f"------Invalid 'sat' value. Valid values are: {', '.join(valid_satellites)}------")
        sys.exit(1)

    satellites_and_product_types = get_dict_satellites_and_product_types(args.sat)

    try:
        access_token = get_access_token()
        # Do something with the access token here
    except Exception as e:
        # Print the error message and exit
        logger.error(e)
        exit(1)  # Exit with a non-zero status code to indicate an error

    for satellite, productTypes in satellites_and_product_types.items():
        for productType in productTypes:
            metadata_products = Metadata_products(satellite, productType, start_date, end_date)
            metadata_products.harvest_all_products_to_json()
            products = metadata_products.get_product_ids_and_titles()
            for product_id,product_title in products.items():
                download_product(product_id, product_title, access_token)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to downloaded products from CDSE between two given dates")

    parser.add_argument("--start_date", type=str, required=True, help="First date you want to download products for (yyyymmdd)")
    parser.add_argument("--end_date", type=str, required=True, help="First date you want to download products for (yyyymmdd)")
    parser.add_argument("--sat", type=str, required=True, help="For which satellite do you want to harvest products?", choices=valid_satellites)

    args = parser.parse_args()
    main(args)






