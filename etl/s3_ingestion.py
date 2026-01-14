import os
import boto3
from pathlib import Path
from dotenv import load_dotenv

# --------------------------
# 1. Path Configuration
# --------------------------
current_dir = Path(__file__).parent
root_dir = current_dir.parent

# Load .env from project root
load_dotenv(dotenv_path=root_dir / '.env')

# --------------------------
# 2. Environment Variables
# --------------------------
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_REGION", "us-east-1")
bucket_name = os.getenv("S3_BUCKET_NAME")

# --------------------------
# 3. S3 Client Initialization
# --------------------------
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region
)

# --------------------------
# 4. Data Source Path
# --------------------------
base_path = root_dir / "data" / "archive"


def upload_all_files():
    """
    Upload all CSV files from the local archive folder to S3 'raw/' path.
    """
    # List all CSV files in source directory
    csv_files = list(base_path.glob('*.csv'))
    
    if not csv_files:
        print(f"Warning: No CSV files found at path: {base_path}")
        return

    print(f"Starting upload: {len(csv_files)} file(s) detected (Source: {base_path})")

    for file_path in csv_files:
        file_name = file_path.name
        s3_path = f"raw/{file_name}"
        
        try:
            # Upload file to S3
            s3_client.upload_file(str(file_path), bucket_name, s3_path)
            print(f"Success: {file_name} uploaded to s3://{bucket_name}/{s3_path}")
        except Exception as e:
            print(f"Error: Failed to upload {file_name}. Reason: {e}")


if __name__ == "__main__":
    upload_all_files()
