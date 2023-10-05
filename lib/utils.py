import yaml
from datetime import date
from shapely.wkt import loads
import logging
import sys

def date_from_string(date_string):
    '''
    Converts date from 6 digits e.g. 20220712 to datetime timestamp
    '''
    year = int(date_string[:4])
    month = int(date_string[4:6])
    day = int(date_string[6:])
    
    return date(year,month,day)

def load_config():
    with open('config.yaml', 'r') as yaml_file:
        config_data = yaml.safe_load(yaml_file)
    
    username = config_data.get('username', '')
    password = config_data.get('password', '')
    output_dir = config_data.get('output_dir', '')
    polygon_wkt = config_data.get('polygon_wkt', '')
    valid_satellites = config_data.get('valid_satellites', [])
    polygon = loads(polygon_wkt)
    
    return username, password, output_dir, polygon_wkt, valid_satellites, polygon

def init_logging():
    # Log to console
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    log_info = logging.StreamHandler(sys.stdout)
    log_info.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(log_info)
    return logger

def get_dict_satellites_and_product_types(sat):
    satellites_and_product_types = {}
    if sat == 'S1':
        satellites_and_product_types['Sentinel1'] = ['all']
    elif sat == 'S2L1C':
        satellites_and_product_types['Sentinel2'] = ['L1C']
    elif sat == 'S2L2A':
        satellites_and_product_types['Sentinel2'] = ['L2A']
    elif sat == 'S3':
        satellites_and_product_types['Sentinel3'] = ['all']
    elif sat == 'all':
        satellites_and_product_types['Sentinel1'] = ['all']
        satellites_and_product_types['Sentinel2'] = ['L1C', 'L2A']
        satellites_and_product_types['Sentinel3'] = ['all']
    
    return satellites_and_product_types
          