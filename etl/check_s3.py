import os
import boto3
from dotenv import load_dotenv
from pathlib import Path

# --------------------------
# 1. Load environment variables
# --------------------------
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

# --------------------------
# 2. Initialize S3 Client
# --------------------------
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

bucket_name = os.getenv("S3_BUCKET_NAME")


def verify_s3_ingestion():
    """
    List all objects in the S3 bucket's 'raw/' directory
    to verify that data ingestion was successful.
    """
    print(f"Action: Auditing S3 bucket '{bucket_name}'...")

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='raw/')

        if 'Contents' in response:
            print(f"Total files detected in 'raw/' prefix: {len(response['Contents'])}")
            print("-" * 50)
            for obj in response['Contents']:
                file_size_kb = round(obj['Size'] / 1024, 2)  # Convert bytes to KB
                print(f"Object: {obj['Key']} | Size: {file_size_kb} KB")
            print("-" * 50)
        else:
            print("Status: No objects found. Please check the ingestion pipeline.")

    except Exception as e:
        print(f"Critical Error: Access denied or bucket does not exist. {e}")


if __name__ == "__main__":
    verify_s3_ingestion()
