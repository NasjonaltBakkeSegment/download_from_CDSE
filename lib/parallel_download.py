import concurrent.futures
import functools
import sys
import requests
import os
import time
from lib.utils import init_logging
from lib.integrity_check import check_extracted_integrity
import pandas as pd
import re
import time
import threading
import glob

logger = init_logging()

# TODO: Run on bigmem
# TODO: 6 parallel downloads 128 Gb memory limit

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

def download_product(product_id, product_title, access_token, tmp_storage_area):
    '''
    Download product from Copernicus Data Space Ecosystem
    '''
    logger.info(f"------Downloading product: {product_title}-------")
    #session = requests.Session()
    with requests.Session() as session:
        session.headers.update({'Authorization': f'Bearer {access_token}'})
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
        #url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
        print(product_title, url)
        response = session.get(url, allow_redirects=False)

    while response.status_code in (301, 302, 303, 307):
        #print(product_title, response.status_code)
        url = response.headers['Location']
        response = session.get(url, allow_redirects=False)

    file = session.get(url, verify=False, allow_redirects=True)
    output_filepath = os.path.join(tmp_storage_area, product_title)

    if product_title.startswith('S5'):
        with open(f"{output_filepath}.nc", 'wb') as p:
            p.write(file.content)
    else:
        with open(f"{output_filepath}.zip", 'wb') as p:
            p.write(file.content)

def download_product_with_retries(product_id, title, output_directory, max_retries, token_lock, access_token):
    retries = 0
    while retries < max_retries:
        try:
            with token_lock:
                current_token = access_token[0]
            return download_product(product_id, title, current_token, output_directory)
        except Exception as e:
            print(str(e))
            if 'token expired' in str(e).lower():
                with token_lock:
                    access_token[0] = get_access_token('dhr@met.no', '6tj&Rh=n9~GA)4>')
                print(f"Token refreshed for product {product_id} ({title})")
            else:
                retries += 1
                print(f"Retry {retries}/{max_retries} for product {product_id} ({title}) due to error: {e}")
                if retries < max_retries:
                    time.sleep(1)  # Optional: wait a bit before retrying
    raise Exception(f"Failed to download product {product_id} ({title}) after {max_retries} retries.")

def check_download_status(list_of_products, tmp_storage_area):

    successes = []
    failures = []

    for id, product_name in list_of_products:
        pattern = os.path.join(tmp_storage_area, f"{product_name}.*")
        matching_files = glob.glob(pattern)

        if matching_files:
            successes.append((id, product_name))
        else:
            failures.append((id, product_name))

    return successes, failures


def download_list_of_products(list_of_products, config):

    max_parallel_downloads = config['max_parallel_downloads']
    max_retries = config['max_retries_per_iteration']
    tmp_storage_area = config['tmp_storage_area']
    output_dir = config['output_dir']
    username = config['username']
    password = config['password']

    try:
        access_token = get_access_token(username, password)
        # Do something with the access token here
    except Exception as e:
        sys.exit(e)

    # Process downloads
    token_lock = threading.Lock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_downloads) as executor:
        partial_download_product = functools.partial(download_product_with_retries, output_directory=tmp_storage_area, max_retries=max_retries, token_lock=token_lock, access_token=[access_token])
        # Submit all the download tasks to the executor
        future_to_product = {executor.submit(partial_download_product, pid, title): (pid, title) for pid, title in list_of_products}

        # Process the results as they complete
        for future in concurrent.futures.as_completed(future_to_product):
            product_id, title = future_to_product[future]
            try:
                data = future.result()
                print(f"Downloaded product {product_id} ({title}): {data}")
            except Exception as exc:
                print(f"Product {product_id} ({title}) generated an exception: {exc}")

    # Failures needs to scan downloaded products
    successes, failures = check_download_status(list_of_products, tmp_storage_area)

    # Move products to their storage location
    #TODO: Write this function
    #* Where should the products be stored if also syncing from GSS?
    #move_products(successes, tmp_storage_area, output_dir)

    return successes, failures