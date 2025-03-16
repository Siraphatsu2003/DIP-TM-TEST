from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
import pyodbc
import json
import base64
from obs import ObsClient
import logging
import os
from airflow.models import Variable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 11),
    'retries': 1,
}

# Define DAG
dag = DAG(
    'TM_direct_to_Huawei_cloud_top_5',
    default_args=default_args,
    description='ETL from SQL Server top 5 rows and upload directly to Huawei Cloud ',
    schedule_interval=None,  # Run daily at 20:00 UTC+7
)

# SQL Server Connection Details
sql_server_config = {
    "DRIVER": Variable.get("SQL_SERVER_DRIVER"),
    "SERVER": Variable.get("SQL_SERVER_SERVER"),
    "UID": Variable.get("SQL_SERVER_UID"),
    "PWD": Variable.get("SQL_SERVER_PWD"),
    "TrustServerCertificate": Variable.get("SQL_SERVER_TRUST")
}

huawei_cloud_config = {
    "access_key": Variable.get("HUAWEI_ACCESS_KEY"),
    "secret_key": Variable.get("HUAWEI_SECRET_KEY"),
    "endpoint": Variable.get("HUAWEI_ENDPOINT"),
    "bucket_name": "dip-eexchange-data"
}

def etl_and_upload(**kwargs):
    try:
        # Connect to SQL Server
        logger.info("Attempting to connect to SQL Server...")
        connection = pyodbc.connect(";".join([f"{key}={value}" for key, value in sql_server_config.items()]))
        logger.info("Connected to SQL Server successfully!")
        cursor = connection.cursor()

        # Columns
        columns = ["FileName", "BiblioID", "Application_Number", "Expiration_Date", "Filing_Date",
                   "IRN_Number", "No_Permit", "Publication_Date", "Registration_Date",
                   "Registration_Number", "Status", "Trademark_Type", "Owner_Name",
                   "Nationallity_Code", "Class", "Goods", "Agent_Name", "physical_path"]

        # Initialize Huawei Cloud OBS Client
        logger.info("Initializing Huawei Cloud OBS Client...")
        client = ObsClient(
            access_key_id=huawei_cloud_config['access_key'],
            secret_access_key=huawei_cloud_config['secret_key'],
            server=huawei_cloud_config['endpoint']
        )
        logger.info("OBS Client initialized successfully.")

        # Batch processing with OFFSET and FETCH NEXT
        # batch_size = 1000
        # offset = 0

        # while True:
        #     logger.info(f"Executing SQL Query with OFFSET: {offset}, BATCH_SIZE: {batch_size}")
        query = f"""
        SELECT TOP 5
            CONCAT('TM_', TM_STG_BIBLIO.id) AS FileName,
            TM_STG_BIBLIO.id AS BiblioID,
            TM_STG_BIBLIO.Application_Number,
            CAST(TM_STG_BIBLIO.Expiration_Date AS DATE) AS Expiration_Date,
            CAST(TM_STG_BIBLIO.Filing_Date AS DATE) AS Filing_Date,
            TM_STG_BIBLIO.IRN_Number,
            TM_STG_BIBLIO.No_Permit,
            CAST(TM_STG_BIBLIO.Publication_Date AS DATE) AS Publication_Date,
            CAST(TM_STG_BIBLIO.Registration_Date AS DATE) AS Registration_Date,
            TM_STG_BIBLIO.Registration_Number,
            TM_STG_BIBLIO.Status,
            TM_STG_BIBLIO.Trademark_Type,
            TM_STG_OWNER.Owner_Name,
            TM_STG_OWNER.Nationallity_Code,
            TM_STG_GOODS.Class,
            TM_STG_GOODS.Goods,
            TM_STG_AGENT.Agent_Name,
            CONCAT('/image/', TM_STG_File.physical_path) AS physical_path
        FROM TM_STG_BIBLIO
        LEFT JOIN TM_STG_OWNER ON TM_STG_BIBLIO.id = TM_STG_OWNER.save_id
        LEFT JOIN TM_STG_GOODS ON TM_STG_BIBLIO.id = TM_STG_GOODS.save_id
        LEFT JOIN TM_STG_AGENT ON TM_STG_BIBLIO.id = TM_STG_AGENT.save_id
        LEFT JOIN TM_STG_File ON TM_STG_BIBLIO.id = TM_STG_File.save_id
        ORDER BY TM_STG_BIBLIO.id
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        logger.info(f"Fetched {len(rows)} rows from database.")

        # if not rows:
        #     logger.info("No more rows to process. Exiting loop.")
        #     break

        # Process rows and upload directly
        for row in rows:
            row_dict = dict(zip(columns, row))

            # Convert date/datetime fields to strings
            for key, value in row_dict.items():
                if isinstance(value, (datetime, date)):
                    row_dict[key] = value.strftime('%Y-%m-%d')

            # Handle physical_path (validate and encode image directly as Base64)
            physical_path = row_dict.get("physical_path")
            if physical_path:
                logger.info(f"Processing physical_path: {physical_path}")
                try:
                    if os.path.isfile(physical_path):
                        with open(physical_path, "rb") as image_file:
                            base64_encoded = base64.b64encode(image_file.read()).decode('utf-8')
                            row_dict["TM_image"] = base64_encoded
                    else:
                        logger.warning(f"Invalid or missing file path: {physical_path}")
                        row_dict["TM_image"] = None
                except Exception as e:
                    logger.error(f"Error processing file at {physical_path}: {e}")
                    row_dict["TM_image"] = None
            else:
                row_dict["TM_image"] = None

            # Convert to JSON string
            file_name = row_dict.pop("FileName") + ".json"
            json_data = json.dumps(row_dict, ensure_ascii=False, indent=4)

            # Define the folder path within the bucket
            folder_name = "trademark_training/"  # Change this to your desired folder name (ensure it ends with '/')

            # Combine folder name and file name to form the full object key
            object_key = f"{folder_name}{file_name}"

            # Upload JSON data to OBS
            response = client.putContent(
                bucketName=huawei_cloud_config['bucket_name'],
                objectKey=object_key,
                content=json_data
            )
            if response.status < 300:
                logger.info(f"Successfully uploaded {file_name}!")
            else:
                logger.error(f"Failed to upload {file_name}: {response.errorMessage}")

        # # Increment offset for the next batch
        # offset += batch_size

    except pyodbc.Error as e:
        logger.error(f"Error while connecting to SQL Server: {e}")
        raise
    except Exception as e:
        logger.error(f"Exception occurred during ETL and upload: {e}")
        raise
    finally:
        if 'connection' in locals() and connection:
            connection.close()
            logger.info("SQL Server connection closed.")
        if 'client' in locals():
            client.close()
            logger.info("ObsClient connection closed.")


# Define Task
etl_and_upload_task = PythonOperator(
    task_id='etl_and_upload_task',
    python_callable=etl_and_upload,
    dag=dag,
)