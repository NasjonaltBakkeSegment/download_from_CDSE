from lib.utils import load_config, init_logging
import requests
import os

(
    username,
    password,
    output_dir,
    polygon_wkt,
    valid_satellites,
    polygon
) = load_config()

logger = init_logging()

def get_access_token():
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
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Print the response content (which should contain the access token)
        return response.json()['access_token']
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