import os
import pyodbc
from dotenv import load_dotenv

# Specify the path to your .env file
env_path = '/path/to/your/.env'
load_dotenv(dotenv_path=env_path)

# Load SQL Server configuration from environment variables
sql_server_config = {
    "DRIVER": os.getenv("SQL_SERVER_DRIVER"),
    "SERVER": os.getenv("SQL_SERVER_SERVER"),
    "UID": os.getenv("SQL_SERVER_UID"),
    "PWD": os.getenv("SQL_SERVER_PWD"),
    "TrustServerCertificate": os.getenv("SQL_SERVER_TRUST")
}

# Build the connection string for SQL Server
connection_string = (
    f"DRIVER={{{sql_server_config['DRIVER']}}};"
    f"SERVER={sql_server_config['SERVER']};"
    f"UID={sql_server_config['UID']};"
    f"PWD={sql_server_config['PWD']};"
    f"TrustServerCertificate={sql_server_config['TrustServerCertificate']};"
)

# Attempt to establish a connection to SQL Server
try:
    conn = pyodbc.connect(connection_string, timeout=5)
    print("SQL Server connection successful!")
    conn.close()
except Exception as e:
    print("SQL Server connection failed:", e)