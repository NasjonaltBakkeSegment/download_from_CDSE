'''
PLAN:
1. Pass N (100?) at a time to the job, extracting them from top of database.
2. Download products in parallel.
3. Return list of products successfully downloaded, remove the rows from database. Errors try again a couple of times?
4. If less than N products, pass all.
5. If no products in database, sleep for 10 minutes and check again.
6. End job after 23.5 hours, leaving 0.5 hours before next job submitted on crontab.
'''

import os
import time
import sqlite3
import argparse
from datetime import datetime, timezone
import sys

from lib.utils import init_logging, load_and_combine_configs
from lib.parallel_download import download_list_of_products

def update_number_of_attempts(failures, db_path):
    """
    Increments the 'attempts' value by 1 for each product name in the failures list.

    Parameters:
        failures (list of str): List of product names to update.
        db_path (str): Path to the SQLite database.
    """
    if not failures:
        return  # Nothing to do

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        for failed_id, failed_name in failures:
            cur.execute("""
                UPDATE products
                SET attempts = attempts + 1
                WHERE id = ?
            """, (failed_id,))
        conn.commit()
    finally:
        conn.close()

def remove_repeated_failures_from_queue(failures, db_path, limit_download_attempts):
    """
    Removes products from the queue if they have failed 3 or more times.

    Parameters:
        failures (list of str): List of product ids to check.
        db_path (str): Path to the SQLite database.
    """
    if not failures:
        return  # Nothing to do

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        for failed_id, failed_name in failures:
            cur.execute("""
                DELETE FROM products
                WHERE id = ? AND attempts >= ?
            """, (failed_id,limit_download_attempts))
        conn.commit()
    finally:
        conn.close()

def get_products_to_download(db_path, limit):
    """
    Retrieves the first `limit` product ids and names from the queue.

    Parameters:
        db_path (str): Path to the SQLite database.
        limit (int): Number of products to retrieve.

    Returns:
        list of tuples: Each tuple contains (id, name) of a product to download.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT id, name FROM products
            ORDER BY ROWID ASC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()  # This will be a list of (id, name) tuples
    finally:
        conn.close()

def remove_products_from_queue(products, db_path):
    """
    Removes products from the queue based on their ids.

    Parameters:
        ids (list of str): Product ids to remove.
        db_path (str): Path to the SQLite database.
    """
    if not products:
        return  # Nothing to do

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        for id, name in products:
            cur.execute("""
                DELETE FROM products
                WHERE id = ?
            """, (id,))
        conn.commit()
    finally:
        conn.close()

def run_download(
        mission_config_path
    ):

    logger = init_logging()
    config = load_and_combine_configs(mission_config_path, 'config/config.yaml')

    while True:

        # If time after cutoff, terminate the job.
        now = datetime.now(timezone.utc)
        cutoff = now.replace(hour=23, minute=30, second=0, microsecond=0)
        if now > cutoff:
            sys.exit(f"Current time is after {cutoff.time()}. Terminating before querying starts again in new job.")

        # Make a list of the first N rows in the database
        # list of tuples [(id, product_name), (id, product_name)...]
        products_to_download = get_products_to_download(
            config['product_download_queue_db'],
            config['number_downloads_per_iteration']
        )

        if len(products_to_download) > 0:
            # Download products in list
            successes, failures = download_list_of_products(products_to_download, config)

            # midpoint = len(products_to_download) // 2
            # successes = products_to_download[:midpoint]
            # failures = products_to_download[midpoint:]

            # Remove successfully downloaded products from the download queue database
            remove_products_from_queue(successes, config['product_download_queue_db'])

            # Update attempts column adding 1
            update_number_of_attempts(failures, config['product_download_queue_db'])

            # If attempted too many times, remove from queue.
            remove_repeated_failures_from_queue(failures, config['product_download_queue_db'], config['limit_download_attempts'])

            # Consider logging failures to check up
            #time.sleep(3)
        else:
            logger.info('No products in queue. Sleeping...')
            time.sleep(600)

# Run the loop
if __name__ == "__main__":

    # Argument parser setup
    parser = argparse.ArgumentParser(description="Process Sentinel product files.")

    parser.add_argument('--mission_config_path', '-c', default='config/config_production.yaml',
                        help="Path to the YAML configuration file for that mission")
    args = parser.parse_args()

    run_download(
        args.mission_config_path
    )