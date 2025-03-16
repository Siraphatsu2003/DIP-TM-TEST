import os
from dotenv import load_dotenv
import boto3  # Assuming you are using `boto3` for Huawei Cloud SDK

# Specify the path to your .env file
env_path = '/path/to/your/.env'
load_dotenv(dotenv_path=env_path)

# Load Huawei Cloud configuration from environment variables
huawei_cloud_config = {
    "access_key": os.getenv("HUAWEI_ACCESS_KEY"),
    "secret_key": os.getenv("HUAWEI_SECRET_KEY"),
    "endpoint": os.getenv("HUAWEI_ENDPOINT"),
    "bucket_name": os.getenv("HUAWEI_BUCKET_NAME")
}

# Create Huawei Cloud S3 client (or other services based on your configuration)
try:
    client = boto3.client(
        's3', 
        aws_access_key_id=huawei_cloud_config['access_key'],
        aws_secret_access_key=huawei_cloud_config['secret_key'],
        endpoint_url=huawei_cloud_config['endpoint']
    )
    # Test by listing the buckets
    response = client.list_buckets()
    print("Huawei Cloud connection successful! Buckets:", response['Buckets'])
except Exception as e:
    print("Huawei Cloud connection failed:", e)
