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
import shutil
from lib.utils import load_and_combine_configs, init_logging, update_time_in_config, predict_base_path
from lib.parallel_download import download_list_of_products

# TODO: Create start, stop, restart scripts that execute jobs
#? Could start script take time window as an optional parameter (default takes from config). If provided backup the download queue.
# They need their own separate bash scripts (on qsub) to run. Stopping them could be challenging as this will require the job ID.

# TODO: Don't synchronise COG or ETA for S3. Check if there are other products that shouldn't be synchronised.
# TODO: Don't add to queue to download from CDSE if already in queue to download from GSS
# TODO: Query and download from GSS too
# TODO: GSS query same mechanics - only add to download list if not already downloaded and not already in CDSE list.

# TODO: Create a clean repo for CDSE synchroniser, reset this repo to previous commit.
# TODO: Separate classes into separate files
# TODO: Need a way to clear out the queue when the time in the config is adjusted. Part of shell script?
# TODO: Check I am happy with what happens if querying unsucessful (e.g. can't connect)
logger = init_logging()

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

class Query():

    def __init__(self, config, mission, database):
        self.config = config
        self.mission = mission
        self.database = database
        self.start_time = datetime.strptime(config['start_timestamp'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        self.start_time_string = self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.end_time = self.start_time + timedelta(minutes=int(self.config['time_window']))
        self.end_time_string = self.end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    def check_if_time_after_end_time_to_query(self):
        '''
        Checking end of time window in future or after delay time
        '''
        now = datetime.now(timezone.utc)

        time_delay = int(self.config['time_delay'])
        delay_threshold = now - timedelta(hours=time_delay)

        if self.end_time >= delay_threshold:
            sleep_time = int(self.config['time_window']) * 60 / 2
            if time_delay == 0:
                logger.info(f"End time {self.end_time_string} is in the future. Sleeping for {sleep_time} seconds...")
            else:
                logger.info(
                    f"End time {self.end_time_string} is within the delay window (after {time_delay} hours ago). "
                    f"Sleeping for {sleep_time} seconds..."
                )
            return True
        else:
            return False

    def create_request_url(self):
        if self.config['polygon']:
            spatial_filter = f" and OData.CSC.Intersects(area=geography'SRID=4326;{self.config['polygon']}')"
        else:
            spatial_filter = ''

        if self.config['date_to_filter_by'] == 'ContentDate':
            temporal_filter = (
                f"ContentDate/Start gt {self.config['start_timestamp']} and "
                f"ContentDate/End lt {self.end_time_string} and "
            )

        elif self.config['date_to_filter_by'] == 'PublicationDate':
            temporal_filter = (
                f"PublicationDate gt {self.config['start_timestamp']} and "
                f"PublicationDate lt {self.end_time_string} and "
            )
        else:
            logger.error(f'Invalid date_to_filter_by option in mission config file: {self.config["date_to_filter_by"]}')
            sys.exit()

        self.query_url = (
            f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter="
            f"{temporal_filter}"
            f"Collection/Name eq '{self.config['collection']}'"
            f"{spatial_filter}"
            f"&$top={self.config['products_per_page']}"
        )

    def execute(self):
        '''
        Query CDSE using the URL computed
        '''

        logger.info(f"Querying: {self.query_url}")
        all_results = []

        response = None

        url = self.query_url

        while url:
            for attempt in range(1, self.config['max_query_attempts'] + 1):
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

                if attempt < self.config['max_query_attempts']:
                    logger.error(f"Waiting {self.config['wait_time_between_failed_queries']} seconds before retrying...")
                    time.sleep(self.config['wait_time_between_failed_queries'])
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
            df['SearchPath'] = df['Short_Name'].apply(lambda name: os.path.join(predict_base_path(name, self.config['output_dir'], self.config['product_types_csv']), name))

            # Keep only rows where the file does *not* exist
            pd.set_option('display.max_colwidth', None)

            df['matches'] = df['SearchPath'].apply(glob.glob)

            logger.info(f'Number of products found: {len(df)}')

            df = df[df['SearchPath'].apply(no_matches)].reset_index(drop=True)

            logger.info(f'Number of products not already on disk: {len(df)}')

            self.database.add_products(df)

            # Deleting df and collecting garbage to avoid memory creep.
            del df
            gc.collect()

class Database():

    def __init__(self, filepath, limit_download_attempts=None):
        self.filepath = filepath
        self.limit_download_attempts = limit_download_attempts

    def check_integrity(self):
        '''
        Check if the database has become corrupt and contains the expected schema.
        Return True if okay.
        '''
        try:
            with sqlite3.connect(self.filepath) as conn:
                cursor = conn.cursor()

                # 1. Check for file corruption
                cursor.execute("PRAGMA integrity_check;")
                result = cursor.fetchone()
                if result[0] != "ok":
                    logger.error(f"Database integrity check failed: {result[0]}")
                    return False

                # 2. Check table and column structure
                cursor.execute("PRAGMA table_info(products);")
                columns = cursor.fetchall()

                expected_columns = [
                    ('name', 'TEXT', 'PRIMARY KEY', None),
                    ('id', 'TEXT', None, None),
                    ('attempts', 'INTEGER', None, '0'),
                    ('progress', 'TEXT', None, 'Pending'),
                    ('created', 'TIMESTAMP', None, "datetime('now')"),
                    ('modified', 'TIMESTAMP', None, "datetime('now')")
                ]

                if len(columns) != len(expected_columns):
                    logger.error(f"Unexpected number of columns in database table: {len(columns)}")
                    return False

                for col_def, expected in zip(columns, expected_columns):
                    _, name, coltype, notnull, dflt_value, pk = col_def
                    exp_name, exp_type, exp_pk, exp_default = expected

                    if name != exp_name or coltype.upper() != exp_type:
                        logger.error(f"Column mismatch: expected {expected}, got {col_def}")
                        return False

                    # Check primary key
                    if (exp_pk == 'PRIMARY KEY' and pk != 1) or (exp_pk is None and pk != 0):
                        logger.error(f"Primary key mismatch on column '{name}': expected {exp_pk}, got pk={pk}")
                        return False

                    # Normalise default values
                    actual_default = dflt_value.strip("'") if dflt_value else None
                    expected_default = exp_default.strip("'") if exp_default else None

                    if actual_default != expected_default:
                        logger.error(f"Default mismatch in column '{name}': expected {expected_default}, got {actual_default}")
                        return False

                logger.info('Database okay')
                return True

        except sqlite3.DatabaseError as e:
            logger.error(f"Database error: {e}")
            return False

    def backup(self):
        base, ext = os.path.splitext(self.filepath)
        dirpath = os.path.dirname(self.filepath) or '.'
        pattern = re.compile(rf"{re.escape(os.path.basename(base))}_backup_(\d+){re.escape(ext)}")

        # Scan for existing backups
        existing = [
            int(match.group(1))
            for fname in os.listdir(dirpath)
            if (match := pattern.fullmatch(fname))
        ]

        next_number = max(existing, default=0) + 1
        backup_filename = f"{base}_backup_{next_number:03}{ext}"
        shutil.move(self.filepath, backup_filename)
        logger.debug(f"Backup created: {backup_filename}")
        return backup_filename

    def create(self):
        with sqlite3.connect(self.filepath) as conn:
            cur = conn.cursor()

            # Create table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    name TEXT PRIMARY KEY,
                    id TEXT,
                    attempts INTEGER DEFAULT 0,
                    progress TEXT DEFAULT 'Pending',
                    created TIMESTAMP DEFAULT (datetime('now')),
                    modified TIMESTAMP DEFAULT (datetime('now'))
                )
            """)

    def add_products(self, df):
        with sqlite3.connect(self.filepath) as conn:
            cur = conn.cursor()

            for _, row in df.iterrows():
                try:
                    cur.execute("""
                        INSERT INTO products (name, id, attempts, progress, created, modified)
                        VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
                    """, (row['Product_Name'], row['Id'], 0, 'Pending'))
                except sqlite3.IntegrityError:
                    # Already exists, skip
                    pass

    def count_pending_products(self):
        '''
        Count how many rows in the products table have progress = 'Pending'.
        '''
        try:
            with sqlite3.connect(self.filepath) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM products WHERE progress = 'Pending';
                """)
                result = cursor.fetchone()
                return result[0]
        except sqlite3.DatabaseError as e:
            logger.error(f"Database error during count_pending: {e}")
            return None

    def get_products_to_download(self, limit):
        """
        Retrieves the first 'limit' product ids and names from the queue,
        and updates their progress to 'In Progress'.

        Parameters:
            limit (int): Number of products to retrieve.

        Returns:
            list of tuples: Each tuple contains (id, name) of a product to download.
        """
        # TODO: Test this
        conn = sqlite3.connect(self.filepath)
        cur = conn.cursor()

        try:
            # Step 1: Get products to download
            cur.execute("""
                SELECT id, name FROM products
                WHERE progress = 'Pending'
                ORDER BY ROWID ASC
                LIMIT ?
            """, (limit,))
            rows = cur.fetchall()

            if rows:
                names = [row[1] for row in rows]  # Extract product names

                # Step 2: Update progress and modified timestamp
                cur.executemany("""
                    UPDATE products
                    SET progress = 'In Progress',
                        modified = datetime('now')
                    WHERE name = ?
                """, [(name,) for name in names])

                conn.commit()

            return rows  # List of (id, name) tuples

        finally:
            conn.close()

    def remove_products(self, product_list):
        if not product_list:
            return # Nothing to do'

        conn = sqlite3.connect(self.filepath)
        cur = conn.cursor()

        try:
            for id, name in product_list:
                cur.execute("""
                    DELETE FROM products
                    WHERE id = ?
                """, (id,))
            conn.commit()
        finally:
            conn.close()

    def update_download_attempts(self, product_list):
        """
        Increments the 'attempts' value by 1 for each product.
        """
        if not product_list:
            return  # Nothing to do

        conn = sqlite3.connect(self.filepath)
        cur = conn.cursor()

        try:
            for id, name in product_list:
                cur.execute("""
                    UPDATE products
                    SET attempts = attempts + 1,
                        progress = 'Pending',
                        modified = datetime('now')
                    WHERE id = ?
                """, (id,))
            conn.commit()
        finally:
            conn.close()

    def remove_repeated_failures(self, failures_database):
        '''
        Move entries from the main queue to the failures database if attempts exceed the limit.
        '''
        # TODO: Test this
        try:
            with sqlite3.connect(self.filepath) as queue_conn, \
                sqlite3.connect(failures_database.filepath) as failures_conn:

                queue_cur = queue_conn.cursor()
                failures_cur = failures_conn.cursor()

                # Find rows that exceed or meet the retry limit
                queue_cur.execute("""
                    SELECT name, id, attempts, progress, created, modified
                    FROM products
                    WHERE attempts >= ?
                """, (self.limit_download_attempts,))
                failed_rows = queue_cur.fetchall()

                if failed_rows:
                    logger.info(f"Moving {len(failed_rows)} failed entries to failure database.")

                # Insert into failures database (ignore duplicates)
                failures_cur.executemany("""
                    INSERT OR IGNORE INTO products (name, id, attempts, progress, created, modified)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, failed_rows)

                # Remove them from the queue
                queue_cur.executemany("""
                    DELETE FROM products WHERE name = ?
                """, [(row[0],) for row in failed_rows])

                # Commit both operations
                queue_conn.commit()
                failures_conn.commit()

        except sqlite3.DatabaseError as e:
            logger.error(f"Error during remove_repeated_failures: {e}")

    def clean_hanging_products(self, failures_database):
        # TODO: Test this
        try:
            with sqlite3.connect(self.filepath) as queue_conn, \
                sqlite3.connect(failures_database.filepath) as failures_conn:

                queue_cur = queue_conn.cursor()
                failures_cur = failures_conn.cursor()

                # Time threshold: 3 hours ago in UTC
                threshold_time = datetime.now(timezone.utc) - timedelta(hours=3)
                threshold_str = threshold_time.strftime('%Y-%m-%d %H:%M:%S')

                # Find hanging products (In Progress), not modified in the last 3 hours
                queue_cur.execute("""
                    SELECT name, id, attempts, progress, created, modified
                    FROM products
                    WHERE progress = 'In Progress' AND modified < ?
                """, (threshold_str,))
                hanging_rows = queue_cur.fetchall()

                restored_count = 0
                failed_count = 0

                for row in hanging_rows:
                    name, prod_id, attempts, progress, created, modified = row

                    if attempts >= self.limit_download_attempts:
                        # Move to failures database
                        failures_cur.execute("""
                            INSERT OR IGNORE INTO products (name, id, attempts, progress, created, modified)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (name, prod_id, attempts, progress, created, modified))

                        queue_cur.execute("DELETE FROM products WHERE name = ?", (name,))
                        failed_count += 1
                    else:
                        # Reset progress to Pending and update modified time
                        queue_cur.execute("""
                            UPDATE products
                            SET progress = 'Pending',
                                modified = datetime('now')
                            WHERE name = ?
                        """, (name,))
                        restored_count += 1

                queue_conn.commit()
                failures_conn.commit()

                logger.info(f"Cleaned hanging products: restored={restored_count}, moved_to_failures={failed_count}")

        except Exception as e:
            logger.error(f"Error cleaning hanging products: {e}")

class Product_List:

    def __init__(self, list_of_products, database, config):
        self.list_of_products = list_of_products
        self.database = database
        self.config = config

    def download(self):
        successes, failures = download_list_of_products(
            self.list_of_products,
            self.config
        )

        # Remove successfully downloaded products from the download queue database
        self.database.remove_products(successes)

        # Update attempts column adding 1
        self.database.update_download_attempts(failures)

def check_if_job_run_time_exceeded(start_time, max_runtime):
    now = datetime.now(timezone.utc)
    if now - start_time > max_runtime:
        return True
    else:
        return False

def synchronise(
        mission
    ):

    # TODO: Issue of duplicated logging

    # TODO: Test downloading when starting with long queue (create by querying only)
    # TODO: Test running multiple at once
    # TODO: Test running over long time period (one day?) in isolated area
    # TODO: Test running over backlog
    # TODO: Add subprocess to submit wrapper for downloaded products


    script_dir = os.path.dirname(os.path.abspath(__file__))
    mission_config_path = os.path.join(script_dir, 'config', f'{mission}_config.yaml')

    # Record start time
    start_time = datetime.now(timezone.utc)
    max_runtime = timedelta(minutes=59, seconds=30)

    config = load_and_combine_configs(mission_config_path, 'config/config.yaml')

    queue_database = Database(config['product_download_queue_db'], config['limit_download_attempts'])
    queue_database.create() # Only creates if doesn't already exist
    if queue_database.check_integrity() == False:
        queue_database.backup()
        queue_database.create()

    failures_database = Database(config['download_failures_db'])
    failures_database.create() # Only creates if doesn't already exist
    if failures_database.check_integrity() == False:
        failures_database.backup()
        failures_database.create()

    queue_database.remove_repeated_failures(failures_database)
    queue_database.clean_hanging_products(failures_database)

    while queue_database.count_pending_products() > 0:

        if check_if_job_run_time_exceeded(start_time, max_runtime) == True:
            logger.info('Max run time of job exceeded. Safely exiting.')
            sys.exit()

        # Make a list of the first N rows in the database
        # list of tuples [(id, product_name), (id, product_name)...]
        product_list = Product_List(
            queue_database.get_products_to_download(
                config['number_downloads_per_iteration']
            ),
            queue_database,
            config
        )

        # Download products in list
        product_list.download()

        # If attempted too many times, remove from queue.
        database.remove_repeated_failures(failures_database)

    else:
        while True:

            if check_if_job_run_time_exceeded(start_time, max_runtime) == True:
                logger.info('Max run time of job exceeded. Safely exiting.')
                sys.exit()

            else:
                # Reload config fresh each time (to pick up updated start_timestamp from mission config)
                config = load_and_combine_configs(mission_config_path, 'config/config.yaml')

                database = Database(config['product_download_queue_db'])
                database.create() # Only create if doesn't already exist

                query = Query(config, mission, database)

                if query.check_if_time_after_end_time_to_query() == True:
                    continue
                else:
                    query.create_request_url()
                    query.execute()

                # Make a list of the first N rows in the database
                # list of tuples [(id, product_name), (id, product_name)...]
                product_list = Product_List(
                    queue_database.get_products_to_download(
                        config['number_downloads_per_iteration']
                    ),
                    queue_database,
                    config
                )

                # Download products in list
                product_list.download()

                # If attempted too many times, remove from queue.
                database.remove_repeated_failures(failures_database)

                # Update config for next iteration
                next_start_dt = query.start_time + timedelta(minutes=int(config['time_step']))
                next_start_string = next_start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                update_time_in_config(mission_config_path, query.start_time_string, next_start_string)

# Run the loop
if __name__ == "__main__":

    # Argument parser setup
    parser = argparse.ArgumentParser(description="Synchronise Sentinel products from CDSE.")

    VALID_MISSIONS = ['s1', 's2', 's3', 's5', 's6']

    parser = argparse.ArgumentParser(description="Process mission options.")
    parser.add_argument(
        '-m', '--mission',
        choices=VALID_MISSIONS,
        required=True,
        help=f"One or more missions to include. Valid options: {', '.join(VALID_MISSIONS)}"
    )

    args = parser.parse_args()

    synchronise(
        args.mission
    )