"""
TO DO:
[] Timestamp verification
"""

from lib.utils import init_logging
import hashlib
import zipfile
import os
import datetime
import time
import shutil
from netCDF4 import Dataset

logger = init_logging()

def get_file_checksum(filepath):
    """Returns the MD5 checksum of a file."""
    md5_check = hashlib.md5()
    try:
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_check.update(chunk)
        return md5_check.hexdigest()
    except FileNotFoundError:
        return 'File not found'
    except Exception as e:
        return str(e)

def zip_timestamp_to_unix(zip_date_time):
    """Convert ZIP (YYYY, MM, DD, HH, MM, SS) tuple to Unix timestamp."""
    dt = datetime.datetime(*zip_date_time)  # Convert to datetime object
    return int(time.mktime(dt.timetuple()))

def get_zip_file_integrity_metrics(zip_filepath):
    """Returns a dictionary of file checksums inside a ZIP archive."""
    metadata = {
        "checksums": {},
        "filesizes": {},
        "timestamps": {}
        }

    with zipfile.ZipFile(zip_filepath, "r") as zip_file:
        for file_name in zip_file.namelist():
            with zip_file.open(file_name) as f:
                file_data = f.read()
                file_checksum = hashlib.md5(file_data).hexdigest()
                file_size = zip_file.getinfo(file_name).file_size
                # file_timestamp = zip_timestamp_to_unix(zip_file.getinfo(file_name).date_time)

                metadata["checksums"][file_name]= file_checksum
                metadata["filesizes"][file_name]= file_size
                # metadata["timestamps"][file_name]= file_timestamp
    return metadata

def extract_zip(zip_filepath, extract_to=None):
    """Extracts the ZIP file to the specified directory, or derives it from the ZIP path."""
    if extract_to is None:
        extract_to = os.path.splitext(zip_filepath)[0]

    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

    return extract_to

def check_zip_integrity(zip_filepath):
    """Checks the integrity of a ZIP file"""

    extracted_dir = extract_zip(zip_filepath)

    try:

        zip_metadata = get_zip_file_integrity_metrics(zip_filepath)
        zip_checksums = zip_metadata["checksums"]
        zip_filesizes = zip_metadata["filesizes"]
        # zip_timestamps = zip_metadata["timestamps"]

        failed_checks = set()

        for file_name in zip_checksums:
            extracted_file_path = os.path.join(extracted_dir, file_name)

            if not os.path.exists(extracted_file_path):
                logger.info(f"------Missing extracted file: {extracted_file_path}------")
                failed_checks.add(file_name)
                continue

            extracted_checksum = get_file_checksum(extracted_file_path)
            extracted_size = os.path.getsize(extracted_file_path)
            # extracted_timestamp = os.stat(extracted_file_path).st_mtime
            # TODO: need to figure out what times to compare... currently extracted_timestamp changes to the time of extraction.
            #       Does not stay same as time of last file change


            if zip_checksums[file_name] != extracted_checksum:
                logger.info(f"------Checksum mismatch: {file_name}------")
                failed_checks.add(file_name)

            if zip_filesizes[file_name] != extracted_size:
                logger.info(f"------File size mismatch: {file_name}------")
                failed_checks.add(file_name)

            # if abs (zip_timestamps[file_name] - extracted_timestamp) > 2: # not tested yet
            #     logger.info(f"------Timestamp mismatch: {file_name}------")
            #     failed_checks.add(file_name)

        shutil.rmtree(extracted_dir)

        if failed_checks:
            logger.error("------Integrity check failed for files:", failed_checks, "------")
            return False
        logger.info("------Passed all integrity checks------")
        return True

    except:
        logger.error("------Integrity check failed------")
        shutil.rmtree(extracted_dir)
        return False

def check_netcdf_integrity(filepath):
    try:
        with Dataset(filepath, 'r') as ds:
            return True
    except Exception as e:
        return False