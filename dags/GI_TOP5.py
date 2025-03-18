from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
import pyodbc
import json
from obs import ObsClient
import logging
from airflow.models import Variable
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
}

# Define DAG
dag = DAG(
    'GI_direct_to_Huawei_cloud_top_5',
    default_args=default_args,
    description='ETL from SQL Server and upload directly to Huawei Cloud',
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

# Function to clean the HTML and special characters
def clean_html(text):
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Replace special characters like &nbsp; and &#160;
    text = re.sub(r'&nbsp;|&#160;', ' ', text)
    # Remove carriage return and newline characters (\r\n, \r, \n)
    text = re.sub(r'[\r\n]+', ' ', text)
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text)
    # Remove escaped double quotes
    text = re.sub(r'\"', ' ', text)
    # Strip leading and trailing whitespace
    return text.strip()

# Task: ETL and Upload to Huawei Cloud
def etl_and_upload(**kwargs):
    try:
        # Connect to SQL Server
        connection = pyodbc.connect(";".join([f"{key}={value}" for key, value in sql_server_config.items()]))
        logger.info("Connected to SQL Server successfully!")
        cursor = connection.cursor()

        # SQL Query
        query = """
        SELECT TOP 5
            CONCAT('GI_', prd.PROD_NUMBER) AS FileName,
            prd.PROD_NUMBER,
            CAST(prd.SUBMIT_DATE AS DATE) AS SUBMIT_DATE,
            CAST(prd.PUBLIC_DATE AS DATE) AS PUBLIC_DATE,
            prd.REGIS_NUMBER,
            prd.GI_NAME,
            prd.GI_PRODUCT,
            prd.GI_NAME_EN,
            CAST(prd.REGIS_DATE AS DATE) AS REGIS_DATE,
            cat.GI_CATEGORY_NAME,
            typ.NAME AS TYPE_NAME,
            prv.NAME_TH AS PROVINCE_NAME,
            reg.NAME_TH AS REGION_NAME,
            pub.ITEM1_DEFINITION,
            pub.ITEM4_LOCATION,
            pub.ITEM3_TOPOGRAPHY,
            pub.ITEM3_HISTORY,
            pub.ITEM6_SECTION15,
            pub.ITEM5_ORIGIN,
            pub.APPLICANT,
            cn.NAME_TH AS COUNTRY_NAME
        FROM [gi_pre].[dbo].[GI_Product] AS prd
        LEFT JOIN [gi_pre].[dbo].[GI_ProductLocation] AS pl ON prd.id = pl.prod_id
        LEFT JOIN [gi_pre].[dbo].[GI_Category] AS cat ON cat.id = prd.cat_id
        LEFT JOIN [gi_pre].[dbo].[GI_Type] AS typ ON typ.id = prd.gi_type
        LEFT JOIN [gi_pre].[dbo].[GI_Publication] AS pub ON prd.id = pub.prod_id
        LEFT JOIN [gi_pre].[dbo].[MS_Province] AS prv ON pl.province_id = prv.id
        LEFT JOIN [gi_pre].[dbo].[MS_Region] AS reg ON prv.region_id = reg.id
        LEFT JOIN [gi_pre].[dbo].[MS_Country] AS cn ON pl.country_id = cn.id
        WHERE prd.REGIS_NUMBER IS NOT NULL
        ORDER BY prd.REGIS_NUMBER
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # Columns
        columns = ["FileName",
            "PROD_NUMBER", "SUBMIT_DATE", "PUBLIC_DATE", "REGIS_NUMBER", "GI_NAME", 
            "GI_PRODUCT", "GI_NAME_EN", "REGIS_DATE", "GI_CATEGORY_NAME",
              "TYPE_NAME", 
            "PROVINCE_NAME", "REGION_NAME", "ITEM1_DEFINITION", "ITEM4_LOCATION", 
            "ITEM3_TOPOGRAPHY", "ITEM3_HISTORY", "ITEM6_SECTION15", "ITEM5_ORIGIN", 
            "APPLICANT", "COUNTRY_NAME"
        ]

        # Initialize Huawei Cloud OBS Client
        client = ObsClient(
            access_key_id=huawei_cloud_config['access_key'],
            secret_access_key=huawei_cloud_config['secret_key'],
            server=huawei_cloud_config['endpoint']
        )
        logger.info("OBS Client initialized successfully.")

        # Process rows and upload directly to Huawei Cloud
        for row in rows:
            row_dict = dict(zip(columns, row))

            # Clean HTML and unwanted characters in relevant fields
            fields_to_clean = [
                "ITEM1_DEFINITION", "ITEM4_LOCATION", "ITEM3_TOPOGRAPHY",
                "ITEM3_HISTORY", "ITEM6_SECTION15", "ITEM5_ORIGIN", "APPLICANT"
            ]
            for field in fields_to_clean:
                if field in row_dict and isinstance(row_dict[field], str):
                    row_dict[field] = clean_html(row_dict[field])

            # Convert date/datetime fields to strings
            for key, value in row_dict.items():
                if isinstance(value, (datetime, date)):
                    row_dict[key] = value.strftime('%Y-%m-%d')

            # Convert to JSON string
            file_name = row_dict.pop("FileName") + ".json"
            json_data = json.dumps(row_dict, ensure_ascii=False, indent=4)

            # Define folder path and upload to OBS
            folder_name = "gi/"
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

# Define Task in Airflow DAG
etl_and_upload_task = PythonOperator(
    task_id='etl_and_upload_to_cloud',
    python_callable=etl_and_upload,
    dag=dag,
)
