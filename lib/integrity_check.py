from lib.utils import init_logging
import hashlib
import zipfile
import os

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

def get_zip_file_checksums(zip_filepath):
    """Returns a dictionary of file checksums inside a ZIP archive."""
    checksums = {}
    with zipfile.ZipFile(zip_filepath, "r") as zip_file:
        for file_name in zip_file.namelist():
            with zip_file.open(file_name) as f:
                file_data = f.read()
                file_checksum = hashlib.md5(file_data).hexdigest()
                checksums[file_name] = file_checksum
    return checksums

def check_extracted_integrity(zip_filepath, extracted_dir):
    """Compares checksums of original ZIP files and extracted files."""
    zip_checksums = get_zip_file_checksums(zip_filepath)
    failed_checks = []

    for file_name, original_checksum in zip_checksums.items():
        extracted_file_path = os.path.join(extracted_dir, file_name)
        
        if not os.path.exists(extracted_file_path):
            logger.info(f"------Missing extracted file: {extracted_file_path}------")
            failed_checks.append(file_name)
            continue
        
        extracted_checksum = get_file_checksum(extracted_file_path)
        if original_checksum != extracted_checksum:
            logger.info(f"------Checksum mismatch: {file_name}------")
            failed_checks.append(file_name)

    if failed_checks:
        logger.error("------Integrity check failed for files:", failed_checks, "------")
        return False
    logger.info("------Passed all integrity checks------")
    return True
