#!/bin/bash

conda activate base

# Define Python script
python_script="./download_only.py"

# Absolute filepath of a JSON file containing products to download
filepath="/path/to/products/CDSE_Sentinel1_all_20210703-20210703_all_products.json"

# Execute the Python script with arguments
$python_script "$filepath"
