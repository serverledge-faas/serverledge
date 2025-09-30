import os
from minio import Minio

# Read config from environment
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "serverledge")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Initialize client
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

def ensure_bucket(bucket_name=MINIO_BUCKET):
    """Create bucket if it does not exist"""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket {bucket_name} already exists")

def exists(object_name, bucket_name=MINIO_BUCKET):
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except:
        return False
     
def upload_file(local_path, object_name, bucket_name=MINIO_BUCKET, override=False):
    """Upload local file to MinIO"""
    ensure_bucket(bucket_name)
    obj_exists = exists(object_name, bucket_name)
    if not override and obj_exists:
        print(f"! Upload canceled. Object {bucket_name}/{object_name} already exists.")
    client.fput_object(bucket_name, object_name, local_path)
    # print(f"Uploaded {local_path} → {bucket_name}/{object_name}")

def download_file(object_name, local_path, bucket_name=MINIO_BUCKET):
    """Download file from MinIO"""
    try:
        client.fget_object(bucket_name, object_name, local_path)
        # print(f"Downloaded {bucket_name}/{object_name} → {local_path}")
    except Exception as e:
        print(f"Error while downloading file from MinIO: {str(e)}")
        return False
    return True

# Save a dataset
# upload_file("test_sample.csv", "raw/test_sample.csv")

# Retrieve it later
# ret = download_file("raw/test_sample.csv", "restored_test_sample.csv")
# print(f"Downloaded? {ret}")