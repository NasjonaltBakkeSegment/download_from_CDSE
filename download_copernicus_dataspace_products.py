#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 12:55:35 2023

@author: lukem
"""

import requests
import json

import argparse
from datetime import date, timedelta
import os
import subprocess

satellites = ['S1', 'S2L1C', 'S2L2A', 'S3']

def date_from_string(date_string):
    '''
    Converts date from 6 digits e.g. 20220712 to datetime timestamp
    '''
    year = int(date_string[:4])
    month = int(date_string[4:6])
    day = int(date_string[6:])
    
    return date(year,month,day)

class All_products_JSON:
    
    def __init__(self, satellite, start_date, end_date):
        self.satellite = satellite
        self.start_date = date_from_string(start_date)
        self.end_date = date_from_string(end_date)
        self.filepath = f'{satellite}_{start_date}-{end_date}_all_products.csv'
    
    def all_products_to_json(self):
        base_url = "http://catalogue.dataspace.copernicus.eu/resto/api/collections/"
        maxRecords = 1000 # Number of products to query in one go
        
        # Initialize an empty list to store the records
        all_records = []
        print('starting to harvest')
        page = 1
        
        while True:
            
            print(f'starting page {page}')
            # Create the URL with the current offset and limit
            url = f"{base_url}{self.satellite}/search.json?startDate={self.start_date}T00:00:00Z&completionDate={self.end_date}T00:59:59Z&sortParam=startDate&&maxRecords={maxRecords}&page={page}"
            
            # Make the request and get the JSON response
            response = requests.get(url).json()
            
            # Append the records from the current response to the list
            all_records.extend(response.get("features", []))
            
            # Check if there are more records to fetch
            if len(response.get("features", [])) < maxRecords:
                break  # No more records to fetch
            
            # Increment page for the next request
            page = page + 1
        
        with open('all_records.json', 'w', encoding='utf-8') as f:
            json.dump(all_records, f, ensure_ascii=False, indent=4)
            
        
        

def main(args):
    # valid_sats = ['Sentinel-1']

    # if args.sat not in valid_sats:
    #     print(f"Invalid 'sat' value. Valid values are: {', '.join(valid_sats)}")
    #     return
    
    satellite = 'Sentinel1'
    start_date = '20210701'
    end_date = '20210701'
    
    myjson = All_products_JSON(satellite, start_date, end_date)
    myjson.all_products_to_json()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to delete all products except Fast-24hr between two given dates")

    parser.add_argument("--sat", type=str, required=False, help="For which satellite do you want to harvest products?", choices=satellites+['all'])
    
    args = parser.parse_args()
    main(args)

# VALIDATE ID BY DOWNLOADING PRODUCT FROM OTHER SCRIPT FROM OTHER API AND CHECKING METADATA
