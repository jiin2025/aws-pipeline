import os
import sys

# --------------------------
# 1. Add project root to Python path for module imports
# --------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# --------------------------
# 2. Import pipeline modules
# --------------------------
import db_test
import s3_ingestion
import s3_to_rds
import create_data_mart
import mart_to_csv


def run_pipeline():
    """
    Orchestrates the end-to-end ETL pipeline:
    1. Validate RDS database connectivity
    2. Upload raw CSV files to AWS S3
    3. Load data from S3 into RDS tables
    4. Create daily sales data mart (Gold Layer)
    5. Export final mart to local CSV
    """

    print("=== Pipeline Started ===")

    # Step 1: Check database connection
    print("Step 1: Validating database connectivity...")
    db_test.test_connection()

    # Step 2: Upload source files to S3
    print("Step 2: Uploading raw CSV files to S3...")
    s3_ingestion.upload_all_files()

    # Step 3: Load data from S3 into RDS
    print("Step 3: Loading data from S3 to RDS...")
    s3_to_rds.run_load()

    # Step 4: Create daily sales data mart
    print("Step 4: Creating daily sales data mart...")
    create_data_mart.create_daily_sales_mart()

    # Step 5: Export data mart to CSV
    print("Step 5: Exporting mart to CSV...")
    mart_to_csv.export_mart_to_csv()

    print("=== Pipeline Completed Successfully ===")


if __name__ == "__main__":
    run_pipeline()
