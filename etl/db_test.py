import os
import psycopg2
from dotenv import load_dotenv

# --------------------------
# 1. Resolve absolute path for the .env file
# --------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(current_dir, "..", ".env")

# Load environment variables explicitly
load_dotenv(dotenv_path)


def test_connection():
    """
    Validate connection to the PostgreSQL RDS instance before executing the pipeline.
    Provides troubleshooting tips if connection fails.
    """
    print(f"Action: Connecting to host {os.getenv('DB_HOST')}...")

    try:
        # Establish database connection
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        print("Success: Database connection established.")
        conn.close()

    except Exception as e:
        print(f"Error: Database connection failed. Reason: {e}")
        print("\n[Troubleshooting Checklist]")
        print("1. Verify DB_HOST (endpoint) address.")
        print("2. Ensure AWS Security Group 'Inbound Rules' allow your current IP.")
        print("3. Check that DB_NAME matches the RDS instance configuration.")


if __name__ == "__main__":
    test_connection()
