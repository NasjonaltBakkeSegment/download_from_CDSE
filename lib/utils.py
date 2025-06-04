import yaml
from datetime import date
from shapely.wkt import loads
from shapely.geometry import shape
import logging
import sys
import os
import pandas as pd

def date_from_string(date_string):
    '''
    Converts date from 6 digits e.g. 20220712 to datetime timestamp
    '''
    year = int(date_string[:4])
    month = int(date_string[4:6])
    day = int(date_string[6:])

    return date(year,month,day)

def load_values_from_config(config_file = 'config.yaml'):
    with open(config_file, 'r') as yaml_file:
        config_data = yaml.safe_load(yaml_file)

    username = config_data.get('username', '')
    password = config_data.get('password', '')
    output_dir = config_data.get('output_dir', '')
    valid_satellites = config_data.get('valid_satellites', [])
    product_types_csv = config_data.get('product_types_csv', '')

    try:
        polygon_wkt = config_data.get('polygon_wkt', '')
        polygon = loads(polygon_wkt)
    except:
        polygon_wkt = polygon = None

    return username, password, output_dir, polygon_wkt, valid_satellites, polygon, product_types_csv

def load_config(config_filename):
    """Load a YAML configuration file into a Python dictionary, resolving path relative to the script."""
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get script directory
    adjusted_config_path = os.path.join(script_dir, "..", config_filename)

    try:
        with open(adjusted_config_path, 'r') as file:
            config = yaml.safe_load(file)

        try:
            config['polygon'] = loads(config['polygon_wkt'])
        except:
            pass
        return config
    except FileNotFoundError:
        print(f"Error: The file at {adjusted_config_path} was not found.")
        return None
    except yaml.YAMLError as exc:
        print(f"Error loading YAML file: {exc}")
        return None

def load_and_combine_configs(mission_config_filepath, general_config_filepath):
    mission_config = load_config(mission_config_filepath)
    general_config = load_config(general_config_filepath)

    combined_config = {**general_config, **mission_config}
    return combined_config

def update_time_in_config(filepath, start_dt, next_start_dt):
    """
    Replace all occurrences of start_dt with next_start_dt in the given file.

    Args:
        filepath (str): Path to the file to be updated.
        start_dt (str): The datetime string to be replaced.
        next_start_dt (str): The new datetime string to insert.
    """
    with open(filepath, 'r', encoding='utf-8') as file:
        contents = file.read()

    updated_contents = contents.replace(start_dt, next_start_dt)

    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(updated_contents)

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
    elif sat == "S5P":
        satellites_and_product_types['Sentinel5P'] = ['all']
    elif sat == "S6":
        satellites_and_product_types['Sentinel6'] = ['all']
    elif sat == 'all':
        satellites_and_product_types['Sentinel1'] = ['all']
        satellites_and_product_types['Sentinel2'] = ['L1C', 'L2A']
        satellites_and_product_types['Sentinel3'] = ['all']
        satellites_and_product_types['Sentinel5P'] = ['all']
        satellites_and_product_types['Sentinel6'] = ['all']

    return satellites_and_product_types

def filter_based_on_polygon(features_list, polygon):
    '''
    Filters a list of geographic features to only include those that intersect with a given polygon.
    Can be used if CDSE is reconverting any polygon to a rectangular bounding box.

    Args:
        features_list (list): A list of geographic features, where each feature is a dictionary containing a 'geometry' key
                              with a geometry that can be converted to a Shapely shape.
        polygon (Polygon): A Shapely Polygon object representing the polygon to filter the features by.

    Returns:
        list: A list of features that are inside or intersect with the given polygon.
    '''
    print('Features in CDSE rectangular bounding box: ', len(features_list))
    print('Filtering...')

    filtered_features = []
    for feature in features_list:
        geom = shape(feature['geometry'])
        if geom.intersects(polygon):
            filtered_features.append(feature)

    print('Features intersecting polygon: ', len(filtered_features))

    return filtered_features

def get_product_metadata(product_metadata_df, esa_product_type):
    """
    Extracts all non-empty values from a single row where 'Alias (ESA product type)'
    matches the given esa_product_type.

    Parameters:
        product_metadata_df (pd.DataFrame): The input DataFrame.
        esa_product_type (str): The value to match in 'Alias (ESA product type)'.

    Returns:
        dict: A dictionary of column names and their non-empty values.
    """
    # Filter the DataFrame for the matching row
    row = product_metadata_df[product_metadata_df['Alias (ESA product type)'] == esa_product_type]

    # Extract non-empty values
    if not row.empty:
        return {col: row.iloc[0][col] for col in row.columns if pd.notna(row.iloc[0][col]) and row.iloc[0][col] != ''}
    else:
        return {}

def predict_base_path(filename, root_path, product_metadata_csv):

    if filename.startswith('S1'):
        filename_product_type = filename.split('_')[1] + '_' + filename.split('_')[2]
        if filename_product_type.startswith('S'):
            filename_product_type = filename[4:14]
    elif filename.startswith('S2'):
        filename_product_type = filename.split('_')[1]
    elif filename.startswith('S3'):
        filename_product_type = filename[4:15]
    elif filename.startswith('S5'):
        filename_product_type = filename[9:19]

    platform = filename.split('_')[0]
    mission = filename[0:2]
    product_metadata_df = pd.read_csv(product_metadata_csv)
    product_metadata = get_product_metadata(product_metadata_df,filename_product_type)

    if mission == 'S1':
        date = filename[17:25]
        mode = filename[4:6]
    elif mission == 'S2':
        date = filename[11:19]
    elif mission == 'S3':
        date = filename[16:24]
    elif mission == 'S5':
        date = filename[20:28]

    year = date[:4]
    month = date[4:6]
    day = date[6:]

    if mission in ['S3', 'S5']:
        url = f'{root_path}{platform}/{year}/{month}/{day}/{product_metadata["product_type"]}/'
    elif mission == 'S1':
        url = f'{root_path}{platform}/{year}/{month}/{day}/{mode}/'
    elif mission == 'S2':
        url = f'{root_path}{platform}/{year}/{month}/{day}/'

    return url