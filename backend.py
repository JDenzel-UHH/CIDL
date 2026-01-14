#######################################################################################
#
# backend.py
#
# Handles:
# - S3 connection (with read-only vs. write mode)
# - Listing objects
# - Uploading files/directories
# - Deleting files
# - Automatic lazy connection if user forgets connect_s3()
#
# This layer is purely organisational. 
# Data loading logic will live in loaders.py
#
#######################################################################################


import os
from pathlib import Path
from itertools import islice
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import boto3
from botocore.config import Config


# --------------------------------------------------------------------
# INTERNAL GLOBAL STATE
# --------------------------------------------------------------------
_S3 = None
_BUCKET = None
_ACTIVE_ENDPOINT = None
_READ_ONLY = True


# --------------------------------------------------------------------
# INTERNAL HELPERS
# --------------------------------------------------------------------
def _ensure_connected():
    """Internal safeguard: auto-connect if not connected."""
    global _S3, _BUCKET, _ACTIVE_ENDPOINT
    if _S3 is None or _BUCKET is None:
        connect_s3()  # falls User vergessen hat, wird automatisch verbunden

def _check_write_permission():
    """Internal safeguard: block write ops in read-only mode."""
    if _READ_ONLY:
        raise PermissionError(
            "Write operation blocked. Backend is in read-only mode. "
            "Call connect_s3(read_only=False) with valid credentials."
        )


# --------------------------------------------------------------------
# S3 CONNECTION
# --------------------------------------------------------------------

def connect_s3(
    bucket_name: str = "cidl-test",
    endpoint_key: str = "primary",
    read_only: bool = True
):
    """
    Establish a connection to the S3 bucket with granular error reporting.

    This function connects to the specified bucket on a chosen endpoint using
    credentials from environment variables (UHH_S3_ACCESS and UHH_S3_SECRET). 

    Args:
        bucket_name (str, optional): Name of the S3 bucket. Defaults to "cidl-test".
        endpoint_key (str, optional): Key to select the S3 endpoint (primary, site-1, site-2, site-3).
        read_only (bool, optional): If True, disables write operations. Defaults to True.

    Returns:
        tuple: (_S3 resource, _BUCKET, _ACTIVE_ENDPOINT)

    Raises:
        RuntimeError: With detailed message if connection fails or credentials are missing/invalid.
    """
    global _S3, _BUCKET, _ACTIVE_ENDPOINT, _READ_ONLY
    _READ_ONLY = read_only

    # --- 1. Credentials prüfen ---
    ACCESS_KEY = os.environ.get("UHH_S3_ACCESS")
    SECRET_KEY = os.environ.get("UHH_S3_SECRET")

    if not ACCESS_KEY and not SECRET_KEY:
        raise RuntimeError("S3 connection failed: ACCESS_KEY and SECRET_KEY are missing in environment variables.")
    if not ACCESS_KEY:
        raise RuntimeError("S3 connection failed: ACCESS_KEY is missing in environment variables.")
    if not SECRET_KEY:
        raise RuntimeError("S3 connection failed: SECRET_KEY is missing in environment variables.")

    # --- 2. Endpoint auswählen ---
    ENDPOINTS = {
        "primary": "https://s3-uhh.lzs.uni-hamburg.de:443",
        "site-1": "https://s3-uhh-s1.lzs.uni-hamburg.de:443",
        "site-2": "https://s3-uhh-s2.lzs.uni-hamburg.de:443",
        "site-3": "https://s3-uhh-s3.lzs.uni-hamburg.de:443",
    }

    if endpoint_key not in ENDPOINTS:
        raise ValueError(
            f"Invalid endpoint '{endpoint_key}'. Choose from {list(ENDPOINTS.keys())}."
        )
    _ACTIVE_ENDPOINT = ENDPOINTS[endpoint_key]

    # --- 3. S3 Resource initialisieren ---
    try:
        session = boto3.session.Session()
        _S3 = session.resource(
            "s3",
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            endpoint_url=_ACTIVE_ENDPOINT,
            config=Config(signature_version="s3v4"),
        )
        _BUCKET = _S3.Bucket(bucket_name)
    except Exception as e:
        raise RuntimeError(f"S3 resource creation failed: {e}")

    # --- 4. Bucketzugriff testen ---
    try:
        _BUCKET.load()
    except boto3.exceptions.ClientError as e:
        code = e.response['Error']['Code']
        if code == 'InvalidAccessKeyId':
            raise RuntimeError("S3 connection failed: Access Key is invalid.")
        elif code == 'SignatureDoesNotMatch':
            raise RuntimeError("S3 connection failed: Secret Key is invalid.")
        elif code == 'AccessDenied':
            raise RuntimeError("S3 connection failed: Access denied to bucket. Check credentials and bucket permissions.")
        elif code == 'NoSuchBucket':
            raise RuntimeError(f"S3 connection failed: Bucket '{bucket_name}' does not exist.")
        else:
            raise RuntimeError(f"S3 connection failed: {e}")
    except Exception as e:
        raise RuntimeError(f"S3 connection failed: Unknown error ({e})")

    # --- 5. Erfolgreiche Verbindung ausgeben ---
    mode = "READ-ONLY" if read_only else "WRITE"
    print(f"Connected to bucket '{_BUCKET.name}' via {_ACTIVE_ENDPOINT} [{mode} mode]")

    return _S3, _BUCKET, _ACTIVE_ENDPOINT

# --------------------------------------------------------------------
# LISTING AND INFO FUNCTIONS
# --------------------------------------------------------------------
def list_objects(prefix: str = "", limit: int = 50):
    """List objects under a prefix."""
    _ensure_connected()
    return [obj.key for obj in islice(_BUCKET.objects.filter(Prefix=prefix), limit)]


def bucket_summary():
    """
    Return a concise status summary of the connected S3 bucket.

    Provides essential information for debugging and quick operational checks,
    including endpoint, access mode, estimated object count, and total data size.
    """
    _ensure_connected()

    total_objects = 0
    total_size = 0
    for obj in _BUCKET.objects.all():
        total_objects += 1
        total_size += obj.size

    return {
        "bucket": _BUCKET.name,
        "endpoint": _ACTIVE_ENDPOINT,
        "read_only": _READ_ONLY,
        "object_count": total_objects,
        "total_size_gb": round(total_size / (1024**3), 4),
    }



# --------------------------------------------------------------------
# UPLOAD FUNCTIONS
# --------------------------------------------------------------------
def upload_file(file_path: Path, key_prefix: str):
    """
    Upload a single file to the S3 bucket.

    This function uploads the given file to the specified key prefix in the currently
    connected S3 bucket. It ensures that a connection exists and that the backend
    is not in read-only mode.

    Args:
        file_path (Path): Full path to the local file to upload 
                          (e.g., 'C:\\...\\file.csv'.
        key_prefix (str): S3 key prefix/folder where the file should be uploaded 
                          (e.g., 'acic22').

    Returns:
        tuple: (file name, True, s3_path) if upload succeeds, or 
               (file name, Exception, None) if it fails.
               - s3_path: full path of the uploaded file in the bucket
                          (e.g., 's3://cidl-test/acic22/sim_0001.parquet')
    """
    _ensure_connected()
    _check_write_permission()

    try:
        key = f"{key_prefix}/{file_path.name}"

        # Fortschrittsanzeige vorbereiten
        file_size = file_path.stat().st_size
        progress = tqdm(total=file_size, unit="B", unit_scale=True, desc=f"Uploading {file_path.name}")

        def progress_callback(bytes_transferred):
            progress.update(bytes_transferred)

        # Datei hochladen mit Callback
        _BUCKET.Object(key).upload_file(
            str(file_path),
            Callback=progress_callback
        )
        progress.close()

        s3_path = f"s3://{_BUCKET.name}/{key}"
        return file_path.name, True, s3_path
    except Exception as e:
        return file_path.name, e, None




def upload_directory(path: Path, key_prefix: str, max_workers: int = 8, extensions=None):
    """
    Upload all files in a local directory that match the given file extensions.

    This function scans the provided local directory for files with the specified
    extensions (default: parquet, csv, json), compares them with the objects that
    already exist under the given S3 prefix, and uploads only the missing files.
    Uploads are executed in parallel using a thread pool.

    Args:
        path (Path): Local directory to search for files.
        key_prefix (str): Target S3 prefix (folder) where the files will be uploaded.
        max_workers (int, optional): Number of parallel upload threads. 
                                     Defaults to 8.
        extensions (list[str], optional): List of glob patterns to match files.
                                          Defaults to ["*.parquet", "*.csv", "*.json"].

    Returns:
        list[tuple]: A list of (filename, error) tuples for all failed uploads.
                     If all uploads succeed, the list is empty.
    """

    _ensure_connected()
    _check_write_permission()

    if extensions is None:
        extensions = ["*.parquet", "*.csv", "*.json"]

    files = []
    for ext in extensions:
        files.extend(path.glob(ext))
    files = sorted(files)

    print(f"Found {len(files)} files in '{path}'.")

    existing = {obj.key for obj in _BUCKET.objects.filter(Prefix=key_prefix)}
    to_upload = [f for f in files if f"{key_prefix}/{f.name}" not in existing]

    print(f"Uploading {len(to_upload)} files...")

    failed = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        with tqdm(total=len(to_upload), desc="Total files") as overall_bar:
            for fname, result, s3_path in ex.map(lambda f: upload_file(f, key_prefix), to_upload):
                if result is not True:
                    failed.append((fname, result))
                overall_bar.update(1)

    print(f"Upload complete. {len(failed)} failed.")
    return failed


# --------------------------------------------------------------------
# DELETE FUNCTION
# --------------------------------------------------------------------
def delete_object(key: str):
    """
    Delete a single object from the connected S3 bucket.

    This function removes the specified object, identified by its full S3 key
    (including prefix and filename). A valid backend connection and write
    permissions are required.

    Args:
        key (str): Full S3 object key, consisting of prefix and filename 
                   (e.g., 'acic22/sim_0001.parquet').

    Returns:
        bool | Exception:
            - True if deletion succeeds
            - The raised Exception if deletion fails
    """

    _ensure_connected()
    _check_write_permission()

    try:
        _BUCKET.Object(key).delete()
        print(f"Deleted: {key}")
        return True
    except Exception as e:
        print(f"Failed to delete {key}: {e}")
        return e


def delete_prefix(key_prefix: str):
    """
    Delete all objects under the given prefix (effectively deleting a folder).

    This removes every object whose key starts with the specified prefix.
    Requires an active backend connection and write permissions.

    Args:
        key_prefix (str): Prefix of the folder (e.g., 'acic22').

    Returns:
        bool | Exception:
            - True if deletion succeeds
            - The raised Exception if deletion fails
    """
    _ensure_connected()
    _check_write_permission()

    try:
        objects = list(_BUCKET.objects.filter(Prefix=key_prefix))

        if not objects:
            print(f"No objects found under prefix '{key_prefix}'.")
            return True

        for obj in objects:
            obj.delete()

        print(f"Deleted {len(objects)} objects under prefix '{key_prefix}'.")
        return True

    except Exception as e:
        print(f"Failed to delete prefix '{key_prefix}': {e}")
        return e
