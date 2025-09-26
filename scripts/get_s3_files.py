import boto3
import os


# AWS S3 Config
bucket_name = "your-bucket-name"       # Replace with your S3 bucket name
s3_folder = "your/folder/path/"        # Folder in S3 where files are stored
local_download_path = "data/"          # Local folder to save files

# Make sure local folder exists
os.makedirs(local_download_path, exist_ok=True)

# Create S3 client
s3 = boto3.client('s3',
                  aws_access_key_id="AWS_ACCESS_KEY",
                  aws_secret_access_key="AWS_SECRET_KEY",
                  region_name="REGION")   # e.g., "ap-south-1"


# Download all files from folder
def download_from_s3(bucket, s3_folder, local_path):
    """Download all files from an S3 folder into local folder."""
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=s3_folder):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith("/"):  # skip folder entries
                    file_name = os.path.basename(key)
                    local_file_path = os.path.join(local_path, file_name)
                    print(f"Downloading {key} --> {local_file_path}")
                    s3.download_file(bucket, key, local_file_path)


download_from_s3(bucket_name, s3_folder, local_download_path)
print("Download complete!")
