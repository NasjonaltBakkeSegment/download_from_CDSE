import requests
from datetime import datetime, timedelta, timezone
import sys
import os
import time
import sqlite3
import argparse
import pandas as pd
import glob
import re
import gc
from lib.utils import load_and_combine_configs, init_logging, update_time_in_config, predict_base_path

# TODO: Create start, stop, restart scripts that execute both the query and download jobs as subprocesses.
# They need their own separate bash scripts (on qsub) to run. Stopping them could be challenging as this will require the job ID.

# TODO: Don't synchronise COG or ETA for S3. Check if there are other products that shouldn't be synchronised.
def no_matches(path_pattern):
    return not glob.glob(path_pattern + '*')

def extract_short_name_by_mission(product_name):
    mission = product_name[:2]

    if mission in ['S1', 'S3', 'S5', 'S6']:
        # These missions usually have two timestamps, use the second one as sensing date
        match = re.match(r'^(.*?\d{8}T\d{6}_\d{8}T\d{6})', product_name)
    elif mission == 'S2':
        # S2 products typically only have one timestamp (sensing time)
        match = re.match(r'^(.*?\d{8}T\d{6})', product_name)
    else:
        # Fallback: return full string if mission not recognised
        return product_name

    return match.group(1) if match else product_name

def query_time_window(url, config, logger):
    logger.info(f"Querying: {url}")
    all_results = []

    response = None

    while url:
        for attempt in range(1, config['max_query_attempts'] + 1):
            try:
                r = requests.get(url)
                if r.ok:
                    response = r.json()
                    all_results.extend(response.get('value', []))
                    url = response.get('@odata.nextLink')
                    break
                else:
                    logger.error(f"Attempt {attempt} failed: HTTP {r.status_code}")
            except Exception as e:
                logger.error(f"Query attempt {attempt} raised an error: {e}")

            if attempt < config['max_query_attempts']:
                logger.error(f"Waiting {config['wait_time_between_failed_queries']} seconds before retrying...")
                time.sleep(config['wait_time_between_failed_queries'])
            else:
                logger.error("All attempts failed, moving on to next time window.")

    if not all_results:
        logger.info("No products found for the given filters.")
    else:
        df = pd.DataFrame.from_dict(all_results)

        # Remove suffix(es) from filename
        df['Product_Name'] = df['Name'].str.replace(r'(\.\w+){1,2}$', '', regex=True)

        # This needs to only include sensing date(s) not ingestion date since products are updated later and we don't want to redownload.
        df['Short_Name'] = df['Product_Name'].apply(extract_short_name_by_mission)

        # Create a new column with full paths without extensions
        df['SearchPath'] = df['Short_Name'].apply(lambda name: os.path.join(predict_base_path(name, config['output_dir'], config['product_types_csv']), name))

        # Keep only rows where the file does *not* exist
        pd.set_option('display.max_colwidth', None)

        df['matches'] = df['SearchPath'].apply(glob.glob)

        logger.info(f'Number of products found: {len(df)}')

        df = df[df['SearchPath'].apply(no_matches)].reset_index(drop=True)

        logger.info(f'Number of products not already on disk: {len(df)}')

        with sqlite3.connect(config['product_download_queue_db']) as conn:
            cur = conn.cursor()

            # Create table with 'attempts' column if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    name TEXT PRIMARY KEY,
                    id TEXT,
                    attempts INTEGER DEFAULT 0
                )
            """)

            for _, row in df.iterrows():
                try:
                    cur.execute(
                        "INSERT INTO products (name, id, attempts) VALUES (?, ?, ?)",
                        (row['Product_Name'], row['Id'], 0)
                    )
                except sqlite3.IntegrityError:
                    # Already exists, skip
                    pass

        # Deleting df and collecting garbage to avoid memory creep.
        del df
        gc.collect()

def create_query_url(config, logger, end_timestamp):
    if config['polygon']:
        spatial_filter = f" and OData.CSC.Intersects(area=geography'SRID=4326;{config['polygon']}')"
    else:
        spatial_filter = ''

    if config['date_to_filter_by'] == 'ContentDate':
        temporal_filter = (
            f"ContentDate/Start gt {config['start_timestamp']} and "
            f"ContentDate/End lt {end_timestamp} and "
        )

    elif config['date_to_filter_by'] == 'PublicationDate':
        temporal_filter = (
            f"PublicationDate gt {config['start_timestamp']} and "
            f"PublicationDate lt {end_timestamp} and "
        )
    else:
        logger.error(f'Invalid date_to_filter_by option in mission config file: {config["date_to_filter_by"]}')
        sys.exit()

    url = (
        f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter="
        f"{temporal_filter}"
        f"Collection/Name eq '{config['collection']}'"
        f"{spatial_filter}"
        f"&$top={config['products_per_page']}"
    )

    return url

def run_query(
        mission_config_path
    ):

    logger = init_logging()

    while True:
        # If time after cutoff, terminate the job.
        now = datetime.now(timezone.utc)
        cutoff = now.replace(hour=23, minute=50, second=0, microsecond=0)
        if now > cutoff:
            sys.exit(f"Current time is after {cutoff.time()}. Terminating before querying starts again in new job.")

        # Load config fresh each time (to pick up updated start_timestamp from mission config)
        config = load_and_combine_configs(mission_config_path, 'config/config.yaml')

        start_dt = datetime.strptime(config['start_timestamp'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)

        end_dt = start_dt + timedelta(minutes=int(config['time_window']))
        end_timestamp = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        if end_dt >= now:
            sleep_time = int(config['time_window']) * 60 / 2
            logger.info(f"End time is in the future. Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)
            continue

        url = create_query_url(config, logger, end_timestamp)

        query_time_window(url, config, logger)

        # Update config for next iteration
        next_start_dt = start_dt + timedelta(minutes=int(config['time_step']))
        start_dt = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        next_start_dt = next_start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        update_time_in_config(mission_config_path, start_dt, next_start_dt)

# Run the loop
if __name__ == "__main__":

    # Argument parser setup
    parser = argparse.ArgumentParser(description="Process Sentinel product files.")

    parser.add_argument('--mission_config_path', '-c', default='config/config_production.yaml',
                        help="Path to the YAML configuration file for that mission")
    args = parser.parse_args()

    run_query(
        args.mission_config_path
    )