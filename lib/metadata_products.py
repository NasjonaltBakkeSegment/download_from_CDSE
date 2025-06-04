import requests
import json
import os
from lib.utils import date_from_string, load_values_from_config, init_logging
import sys

(
    username,
    password,
    output_dir,
    polygon_wkt,
    valid_satellites,
    polygon,
    product_types_csv
) = load_values_from_config(config_file='./config.yaml')

logger = init_logging()

class Metadata_products:

    def __init__(self, satellite=None, productType=None, start_date=None, end_date=None, json_filepath=None):
        if satellite:
            self.satellite = satellite
        if productType:
            self.productType = productType
        if start_date:
            self.start_date = date_from_string(start_date)
        if end_date:
            self.end_date = date_from_string(end_date)

        if json_filepath:
            self.filepath = json_filepath
        elif self.satellite and self.productType and self.start_date and self.end_date:
            self.filename = f'CDSE_{satellite}_{productType}_{start_date}-{end_date}_all_products.json'
            self.filepath = os.path.join(output_dir, self.filename)
        else:
            logger.info(f"------Class requires you to provide either a JSON file to download products or all 4 arguements (satellite, productType, start_date, end_date) ")
            sys.exit(1)

    def harvest_all_products_to_json(self):
        logger.info(f"------Creating JSON file of {self.satellite} {self.productType} products that are present on CDSE between {self.start_date} and {self.end_date} -------")
        base_url = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/"
        maxRecords = 1000 # Number of products to query in one go

        # Initialize an empty list to store the records
        self.all_records = []
        page = 1

        while True:

            # Create the URL with the current offset and limit
            if self.productType == 'all':
                url = f"{base_url}{self.satellite}/search.json?startDate={self.start_date}T00:00:00Z&completionDate={self.end_date}T00:00:00Z&sortParam=startDate&geometry={polygon}&maxRecords={maxRecords}&page={page}"
            else:
                url = f"{base_url}{self.satellite}/search.json?productType={self.productType}&startDate={self.start_date}T00:00:00Z&completionDate={self.end_date}T00:00:00Z&sortParam=startDate&geometry={polygon}&maxRecords={maxRecords}&page={page}"
            # Make the request and get the JSON response
            response = requests.get(url).json()

            # Append the records from the current response to the list
            self.all_records.extend(response.get("features", []))

            # Check if there are more records to fetch
            if len(response.get("features", [])) < maxRecords:
                break  # No more records to fetch

            # Increment page for the next request
            page = page + 1

        with open(self.filepath, 'w', encoding='utf-8') as f:
            json.dump(self.all_records, f, ensure_ascii=False, indent=4)
            logger.info(f"------File created: {self.filepath}-------")

    def load_json(self):
        logger.info(f"------Loading JSON file of {self.filepath}-------")
        with open(self.filepath, 'r', encoding='utf-8') as f:
            self.all_records = json.load(f)

    def get_product_ids_and_titles(self):
        '''
        Get the ID (a UUID) for each product to be downloaded
        Note that the UUID is Copernicus Data Space Ecosystem does not match the UUID in Colhub-Archive.
        '''
        logger.info("------Creating dictionary of IDs and file names for products that will be downloaded-------")
        products = {}

        for item in self.all_records:
            products[item['id']] = item['properties']['title'].split('.')[0]

        self.product_ids_and_titles = products
        return products

    def create_storage_paths(self):
        '''
        Extract metadata from JSON file to create storage path:
            platform/year/month/day/product_type
        '''
        logger.info("------Creating product storage paths-------")
        paths = {}

        for item in self.all_records:
            # platform = item['properties']['platform'] # sometimes only given as "SENTINEL-3"
            platform = item['properties']['title'].split('_')[0]
            date = item['properties']['startDate'].split('T')[0].split('-')
            year = date[0]
            month = date[1]
            day = date[2]
            product_type = item['properties']['productType']

            path = os.path.join(output_dir, platform, year, month, day, product_type)

            paths[item['id']] = path

        self.storage_paths = paths
        return paths

    def create_metadata_storage_paths(self):
        '''
        Extract metadata from JSON file to create storage path:
            /metadata/platform/year/month/day/product_type
        '''
        logger.info("------Creating metadata storage paths-------")
        paths = {}

        for item in self.all_records:
            # platform = item['properties']['platform'] # sometimes only given as "SENTINEL-3"
            platform = item['properties']['title'].split('_')[0]
            date = item['properties']['startDate'].split('T')[0].split('-')
            year = date[0]
            month = date[1]
            day = date[2]
            product_type = item['properties']['productType']

            path = os.path.join(output_dir, 'metadata', platform, year, month, day, product_type)

            paths[item['id']] = path

        self.metadata_storage_paths = paths
        return paths

    def filter_out_synced_products(self):
        filtered_products = {}
        filtered_storage_paths = {}
        for product_id, storage_path in self.storage_paths.items():
            file_path_SEN3 = os.path.join(storage_path, self.product_ids_and_titles[product_id] + ".SEN3")
            file_path_SAFE = os.path.join(storage_path, self.product_ids_and_titles[product_id] + ".SAFE")

            # Check if the file path does not exist
            if not (os.path.exists(file_path_SEN3) or os.path.exists(file_path_SAFE)):
                filtered_products[product_id] = self.product_ids_and_titles[product_id]
                filtered_storage_paths[product_id] = storage_path
            else:
                logger.info(f"------Skipping {self.product_ids_and_titles[product_id]}, already synced to storage------")

        self.filtered_metadata_products = filtered_products
        return filtered_products, filtered_storage_paths

    def create_products_metadata_dict(self):
        logger.info("------Creating dictionary of individual product metadata to be stored-------")

        product_metadata_dict = {product["id"]: product for product in self.all_records}
        self.product_metadata_dict = product_metadata_dict

    def store_individual_product_metadata(self, product_id):
        product = self.product_metadata_dict[product_id]
        product_json = product.get('properties', {}).get('title').split(".")[0] + ".json"

        metadata_dirpath = self.metadata_storage_paths[product_id]
        metadata_filepath = os.path.join(metadata_dirpath, product_json)

        os.makedirs(metadata_dirpath, exist_ok=True)

        with open(metadata_filepath, 'w', encoding='utf-8') as f:
            json.dump(product, f, ensure_ascii=False, indent=4)
            logger.info(f"------Created metadata file: {metadata_filepath}-------")

    # def store_all_individual_product_metadata(self):
    #     '''
    #     Extract all individual metadata for each product and store to its own file.
    #     Stored in metadata directory.
    #     '''
    #     logger.info("------Storing metadata-------")

    #     for product in self.all_records:
    #         product_id = product.get("id")
    #         if not product_id:
    #             logger.info("------Skipping product with no ID------")
    #             continue
    #         if hasattr(self, "filtered_metadata_products"): # This should be made more efficient. To much branching...
    #             if product_id not in self.filtered_metadata_products:
    #                 logger.info(f"------Skipping {product.get('properties', {}).get('title').split('.')[0]}, already synced product metadata------")
    #                 continue

    #         product_json = product.get('properties', {}).get('title').split(".")[0] + ".json"
    #         metadata_dirpath = self.metadata_storage_paths[product_id]
    #         metadata_filepath = os.path.join(metadata_dirpath, product_json)

    #         os.makedirs(metadata_dirpath, exist_ok=True)

    #         with open(metadata_filepath, 'w', encoding='utf-8') as f:
    #             json.dump(product, f, ensure_ascii=False, indent=4)
    #             logger.info(f"------File created: {metadata_filepath}-------")





