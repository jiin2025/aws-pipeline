\# Olist AWS S3 → RDS ETL Pipeline

End-to-end ETL pipeline for Olist e-commerce data using AWS S3 and PostgreSQL. It ingests raw CSV data, loads it into RDS, creates a daily sales data mart, and exports the result to CSV for analysis.

( CSV-olist → S3 → RDS → Data Mart → CSV export )



\## Project Structure

aws-pipeline/

 ├─ data/               # Source CSV files

&nbsp;├─ etl/		# ETL Logic \& Scripts

&nbsp;│   ├─ db\_test.py              # Validate PostgreSQL connection

 │   ├─ s3\_ingestion.py         # Upload CSV files to AWS S3

 │   ├─ s3\_to\_rds.py            # Load S3 data into AWS RDS

 │   ├─ create\_data\_mart.py     # Build daily sales data mart

 │   ├─ mart\_to\_csv.py          # Export mart to CSV

 │   ├─ run\_pipeline.py         # Orchestrates all ETL steps

 │   └─ main.py                 # Orchestrates the full ETL pipeline

 ├─ .env		# Environment variables (AWS Credentials, DB info)

 └─ README.md		# Project documentation



\## Technologies Used

\- Languages/Libs: Python, Pandas, SQLAlchemy, Psycopg2, Boto3

\- Database: PostgreSQL

\- Cloud: AWS S3 (Storage), AWS RDS (Database)

\- Environment: Docker, Apache Airflow (Orchestration)



\## Setup

\- Python 3.10+

\- Install required packages:

  pip install pandas sqlalchemy psycopg2-binary boto3 python-dotenv



\## Configuration: Set up your .env file with your AWS and RDS credentials.



\## How to Run

1\. Navigate to the project root folder 

2\. Execute the full pipeline:

&nbsp;    python etl/main.py

&nbsp;

\## Output

\- AWS RDS: Fully populated tables and Daily Sales Data Mart.

\- Local Export: data/daily\_sales\_mart.csv

\- Logs: Execution status messages in the console.







