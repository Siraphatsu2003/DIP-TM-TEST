import os
import pyodbc
from dotenv import load_dotenv

# Specify the path to your .env file
env_path = r'C:\work\IMC\DIP\DIP-TM-TEST\.env'

# Load the environment variables from the .env file
load_dotenv(dotenv_path=env_path)

# Now that environment variables are loaded, access them
sql_server_config = {
    "DRIVER": os.environ.get("SQL_SERVER_DRIVER"),
    "SERVER": os.environ.get("SQL_SERVER_SERVER"),
    "UID": os.environ.get("SQL_SERVER_UID"),
    "PWD": os.environ.get("SQL_SERVER_PWD"),
    "TrustServerCertificate": os.environ.get("SQL_SERVER_TRUST")
}

# Add debug printing to verify the values were loaded correctly
print("SQL Server Configuration:")
for key, value in sql_server_config.items():
    print(f"{key}: {value}")

# Build the connection string for SQL Server
connection_string = (
    f"DRIVER={{{sql_server_config['DRIVER']}}};"
    f"SERVER={sql_server_config['SERVER']};"
    f"UID={sql_server_config['UID']};"
    f"PWD={sql_server_config['PWD']};"
    f"TrustServerCertificate={sql_server_config['TrustServerCertificate']};"
)

print("\nConnection string (credentials masked):")
masked_connection_string = connection_string.replace(sql_server_config['PWD'], "********")
print(masked_connection_string)

# Attempt to establish a connection to SQL Server
try:
    conn = pyodbc.connect(connection_string, timeout=5)
    print("\nSQL Server connection successful!")
    conn.close()
except Exception as e:
    print("\nSQL Server connection failed:", e)