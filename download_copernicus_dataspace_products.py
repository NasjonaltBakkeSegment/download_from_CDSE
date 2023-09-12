#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 12:55:35 2023

@author: lukem
"""

import requests
import json
import argparse
from datetime import date
import os
from shapely.wkt import loads

# AOI
wkt_string = (
    "POLYGON ("
    "(-20.263238824222373 84.8852877777822, "
    "-36.25445787748578 67.02581594412311, "
    "11.148084316116405 52.31593720759386, "
    "45.98609725358305 63.94940066151824, "
    "89.96194965005743 84.8341192704811, "
    "-20.263238824222373 84.8852877777822)"
    ")")

polygon = loads(wkt_string)

with open("config.json", 'r') as file:
    config_data = json.load(file)

# Access the username and password
username = config_data['username']
password = config_data['password']
base_filepath = config_data['output_dir']

valid_sats = ['S1', 'S2L1C', 'S2L2A', 'S3', 'all']

def date_from_string(date_string):
    '''
    Converts date from 6 digits e.g. 20220712 to datetime timestamp
    '''
    year = int(date_string[:4])
    month = int(date_string[4:6])
    day = int(date_string[6:])
    
    return date(year,month,day)

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
        
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {access_token}'})
    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    response = session.get(url, allow_redirects=False)

    while response.status_code in (301, 302, 303, 307):
        url = response.headers['Location']
        response = session.get(url, allow_redirects=False)

    file = session.get(url, verify=False, allow_redirects=True)
    output_filepath = os.path.join(base_filepath, product_title)

    with open(f"{output_filepath}.zip", 'wb') as p:
        p.write(file.content)

class All_products_JSON:
    
    def __init__(self, satellite, productType, start_date, end_date):
        self.satellite = satellite
        self.productType = productType
        self.start_date = date_from_string(start_date)
        self.end_date = date_from_string(end_date)
        self.filepath = f'{satellite}_{start_date}-{end_date}_all_products.csv'
    
    def metadata_all_products_to_json(self):
        base_url = "http://catalogue.dataspace.copernicus.eu/resto/api/collections/"
        maxRecords = 1000 # Number of products to query in one go
        
        # Initialize an empty list to store the records
        self.all_records = []
        print('starting to harvest')
        page = 1
        
        while True:
            
            print(f'starting page {page}')
            # Create the URL with the current offset and limit
            if self.productType == 'all':
                url = f"{base_url}{self.satellite}/search.json?startDate={self.start_date}T00:00:00Z&completionDate={self.end_date}T02:09:59Z&sortParam=startDate&geometry={polygon}&maxRecords={maxRecords}&page={page}"
            else:
                url = f"{base_url}{self.satellite}/search.json?productType={self.productType}&startDate={self.start_date}T00:00:00Z&completionDate={self.end_date}T02:09:59Z&sortParam=startDate&geometry={polygon}&maxRecords={maxRecords}&page={page}"
            # Make the request and get the JSON response
            response = requests.get(url).json()
            
            # Append the records from the current response to the list
            self.all_records.extend(response.get("features", []))
            
            # Check if there are more records to fetch
            if len(response.get("features", [])) < maxRecords:
                break  # No more records to fetch
            
            # Increment page for the next request
            page = page + 1
            
        if self.productType == 'all':
            filename = f'{self.satellite}_all_products_{self.start_date}_to_{self.end_date}.json'
        else:
            filename = f'{self.satellite}_{self.productType}_{self.start_date}_to_{self.end_date}.json'
            
        output_filepath = os.path.join(base_filepath, filename)
        
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(self.all_records, f, ensure_ascii=False, indent=4)
        
    def get_product_ids_and_titles(self):
        '''
        Get the ID (a UUID) for each product to be downloaded
        Note that the UUID is Copernicus Data Space Ecosystem does not match the UUID in Colhub-Archive.
        '''
        products = {}
        
        for item in self.all_records:
            #if self.satellite == 'Sentinel2'
            products[item['id']] = item['properties']['title'].split('.')[0]
        
        #product_ids = [item["id"] for item in self.all_records]
        
        return products
        
def main(args):
    
    start_date = args.start_date
    end_date = args.end_date
    
    if args.sat not in valid_sats:
        print(f"Invalid 'sat' value. Valid values are: {', '.join(valid_sats)}")
        return
    elif args.sat == 'S1':
        satellites = ['Sentinel1']
    elif args.sat in ['S2L1C','S2L2A']:
        satellites = ['Sentinel2']
    elif args.sat == 'S3':
        satellites = ['Sentinel3']
    elif args.sat == 'all':
        satellites = ['Sentinel1', 'Sentinel2', 'Sentinel3']
    
    for satellite in satellites:
        
        if satellite in ['Sentinel1', 'Sentinel3']:
            productTypes = ['all'] # Download all products
        elif satellite == 'Sentinel2':
            if args.sat == 'S2L1C':
                productTypes = ['L1C']
            elif args.sat == 'S2L2A':
                productTypes = ['L2A']
            elif args.sat == 'all':
                productTypes = ['L1C', 'L2A']           
    
        for productType in productTypes:
            myjson = All_products_JSON(satellite, productType, start_date, end_date)
            myjson.metadata_all_products_to_json()
            products = myjson.get_product_ids_and_titles()
            
            for product_id,product_title in products.items():
                try:
                    access_token = get_access_token()
                    # Do something with the access token here
                except Exception as e:
                    # Print the error message and exit
                    print(e)
                    exit(1)  # Exit with a non-zero status code to indicate an error
                
                download_product(product_id, product_title, access_token)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to delete all products except Fast-24hr between two given dates")

    parser.add_argument("--start_date", type=str, required=True, help="First date you want to delete products for (yyyymmdd)")
    parser.add_argument("--end_date", type=str, required=True, help="First date you want to delete products for (yyyymmdd)")
    parser.add_argument("--sat", type=str, required=True, help="For which satellite do you want to harvest products?", choices=valid_sats)
    
    args = parser.parse_args()
    main(args)
