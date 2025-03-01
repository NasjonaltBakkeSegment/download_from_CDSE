from lib.utils import load_config, init_logging
from lib.integrity_check import check_extracted_integrity
import requests
import os
import zipfile
import time

(
    username,
    password,
    output_dir,
    polygon_wkt,
    valid_satellites,
    polygon
) = load_config(config_file='/lustre/storeB/users/alessioc/download_from_CDSE/config.yaml')

logger = init_logging()

# Init access token
access_token = None
token_expiry = 0
refresh_token = None

def get_access_token():
    global access_token, token_expiry, refresh_token

    # If token exists and is still valid, return it
    # REMOVED since it seems to be better to refresh before each download.
    # if access_token and time.time() < token_expiry - 60:  # Refresh 1 minute before expiry
    #     return access_token

    # Define the URL and payload data
    url = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
    
    if refresh_token:
        payload = {
            "client_id": "cdse-public",
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
    else: 
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
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        access_token = response.json()['access_token']
        token_expiry = time.time() + response.json()['expires_in']
        refresh_token = response.json()['refresh_token']
        return access_token
    else:
        # Raise an exception with the error message if the request fails
        raise Exception(f"Error: {response.status_code} - {response.text}")

def download_product(product_id, product_title, access_token):
    '''
    Download product from Copernicus Data Space Ecosystem
    '''
    logger.info(f"------Downloading product: {product_title}-------") 
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {access_token}'})
    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    response = session.get(url, allow_redirects=False)

    while response.status_code in (301, 302, 303, 307):
        url = response.headers['Location']
        response = session.get(url, allow_redirects=False)

    file = session.get(url, verify=False, allow_redirects=True)
    output_filepath = os.path.join(output_dir, product_title)

    with open(f"{output_filepath}.zip", 'wb') as p:
        p.write(file.content)

def unzip_and_store(product_title, storage_path):
    '''
    Unzip downloaded product and store to a specified directory.
    The zip file is subsequently deleted.
    '''
    zip_filepath = os.path.join(output_dir, f"{product_title}.zip")

    logger.info(f"------Extracting all files from: {product_title}.zip-------")

    if not os.path.exists(zip_filepath):
        logger.warning(f"------Zip file {zip_filepath} not found, skipping extraction------")
        return

    try:
        with zipfile.ZipFile(zip_filepath, "r") as zip_file:
            zip_file.extractall(storage_path)
        # Integrity check of the extracted files
        check_extracted_integrity(zip_filepath, storage_path)
        os.remove(zip_filepath)
        logger.info(f"------Extracted and deleted zip file: {zip_filepath}------")
    
    except zipfile.BadZipFile:
        os.remove(zip_filepath)
        logger.error(f"------Failed to extract: {zip_filepath}. Corrupt (Non-valid) zip file has been removed------") # Most likely due to download taking more than 10 mins (token expires)

    except Exception as e:
        os.remove(zip_filepath)
        logger.error(f"------Unexpected error while extracting {zip_filepath}: {e}------")
