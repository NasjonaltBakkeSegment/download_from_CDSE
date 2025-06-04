import concurrent.futures
import sys
import requests
import os
import time
from lib.utils import init_logging, predict_base_path
from lib.integrity_check import check_extracted_integrity
import shutil
import time
import threading
import glob

logger = init_logging()

class Product:
    def __init__(self, product_id, title, tmp_storage_area, output_dir, product_types_csv):
        self.id = product_id
        self.title = title
        self.tmp_storage_area = tmp_storage_area
        self.output_dir = output_dir
        self.download_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({self.id})/$value"
        self.product_types_csv = product_types_csv

    def download(self, access_token):
        logger.info(f"------Downloading product: {self.title}-------")
        with requests.Session() as session:
            session.headers.update({'Authorization': f'Bearer {access_token}'})
            response = session.get(self.download_url, allow_redirects=False)

            while response.status_code in (301, 302, 303, 307):
                response = session.get(response.headers['Location'], allow_redirects=False)

            final_response = session.get(response.url, verify=False, allow_redirects=True)

        ext = ".nc" if self.title.startswith('S5') else ".zip"
        self.tmp_absolute_filepath = os.path.join(self.tmp_storage_area, f"{self.title}{ext}")
        with open(self.tmp_absolute_filepath, 'wb') as f:
            f.write(final_response.content)

    def was_downloaded(self):
        return bool(glob.glob(self.tmp_absolute_filepath))

    def move_to_output(self):

        src = self.tmp_absolute_filepath

        product_name = os.path.basename(src)
        base_path = predict_base_path(product_name, self.output_dir, self.product_types_csv)
        os.makedirs(base_path, exist_ok=True)

        dst = os.path.join(base_path, os.path.basename(src))
        try:
            shutil.move(src, dst)
            logger.info(f"Moved {self.title} to {self.output_dir}")
        except Exception as e:
            logger.error(f"Error moving {self.title}: {e}")
            return False
        return True

def get_access_token(username, password):

    # Define the URL and payload data
    url = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
    payload = {
        'grant_type': 'password',
        'username': username,
        'password': password,
        'client_id': 'cdse-public'
    }

    # Define the headers
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    # Make the POST request
    response = requests.post(url, data=payload, headers=headers)
    response.raise_for_status()

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Print the response content (which should contain the access token)
        return response.json()['access_token']
    else:
        # Raise an exception with the error message if the request fails
        raise Exception(f"Error: {response.status_code} - {response.text}")

def download_product_with_retries(product, max_retries, token_lock, access_token, username, password):
    retries = 0
    while retries < max_retries:
        try:
            with token_lock:
                current_token = access_token[0]
            return product.download(current_token)
        except Exception as e:
            if 'token expired' in str(e).lower():
                with token_lock:
                    access_token[0] = get_access_token(username, password)
                logger.warning(f"Token refreshed for product {product.id} ({product.title})")
            else:
                retries += 1
                logger.warning(f"Retry {retries}/{max_retries} for product {product.id} ({product.title}): {e}")
                time.sleep(1)
    raise Exception(f"Failed to download product {product.id} ({product.title}) after {max_retries} retries.")

def download_list_of_products(list_of_products, config):

    max_parallel_downloads = config['max_parallel_downloads']
    max_retries = config['max_retries_per_iteration']
    tmp_storage_area = config['tmp_storage_area']
    output_dir = config['output_dir']
    username = config['username']
    password = config['password']
    product_types_csv = config['product_types_csv']

    try:
        access_token = [get_access_token(username, password)]
    except Exception as e:
        sys.exit(e)

    token_lock = threading.Lock()

    products = [Product(pid, title, tmp_storage_area, output_dir, product_types_csv) for pid, title in list_of_products]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_downloads) as executor:
        futures = {
            executor.submit(download_product_with_retries, product, max_retries, token_lock, access_token, username, password): product
            for product in products
        }

        for future in concurrent.futures.as_completed(futures):
            product = futures[future]
            try:
                future.result()
                logger.info(f"Downloaded product {product.id} ({product.title})")
            except Exception as exc:
                logger.error(f"Product {product.id} ({product.title}) failed with exception: {exc}")

    successes = []
    failures = []
    for product in products:
        if product.was_downloaded():
            successes.append((product.id, product.title))
            product.move_to_output()
        else:
            failures.append((product.id, product.title))

    return successes, failures