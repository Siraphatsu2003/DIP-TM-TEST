from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
import pyodbc
import json
import logging
from airflow.models import Variable
import re
import os

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
    'GI_direct_to_local_top_5',
    default_args=default_args,
    description='ETL from SQL Server and save directly to local filesystem',
    schedule_interval=None,  # Run manually
)

# SQL Server Connection Details
sql_server_config = {
    "DRIVER": Variable.get("SQL_SERVER_DRIVER"),
    "SERVER": Variable.get("SQL_SERVER_SERVER"),
    "UID": Variable.get("SQL_SERVER_UID"),
    "PWD": Variable.get("SQL_SERVER_PWD"),
    "TrustServerCertificate": Variable.get("SQL_SERVER_TRUST")
}

# Local storage configuration
import os
import logging

# Set the output directory
local_storage_config = {
    "output_dir": Variable.get("LOCAL_OUTPUT_DIR", "/opt/airflow/output_airflow")
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

# Task: ETL and Save to Local Storage
def etl_and_save_local(**kwargs):
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

        # Ensure output directory exists with more detailed logging
        output_dir = local_storage_config['output_dir']
        gi_folder = os.path.join(output_dir, "gi")
        
        # Add more debugging
        logger.info(f"Trying to create/access directory: {gi_folder}")
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Base output directory created: {output_dir}")
        except Exception as e:
            logger.error(f"Error creating base directory {output_dir}: {str(e)}")
            
        try:
            os.makedirs(gi_folder, exist_ok=True)
            logger.info(f"GI folder created: {gi_folder}")
            # Check if directory exists and is writable
            if os.path.exists(gi_folder):
                logger.info(f"Confirmed {gi_folder} exists")
                test_file = os.path.join(gi_folder, "test_write.txt")
                with open(test_file, 'w') as f:
                    f.write("Test write access")
                logger.info(f"Successfully wrote test file: {test_file}")
        except Exception as e:
            logger.error(f"Error creating or writing to directory {gi_folder}: {str(e)}")

        # Process rows and save to local filesystem
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

            # Define file path and save locally with more robust error handling
            file_path = os.path.join(gi_folder, file_name)
            
            # Log the exact path we're trying to write to
            logger.info(f"Attempting to write file to: {file_path}")
            
            # Check if parent directory exists again
            if not os.path.exists(gi_folder):
                logger.warning(f"Directory {gi_folder} doesn't exist before file write, creating it again")
                try:
                    os.makedirs(gi_folder, exist_ok=True)
                except Exception as e:
                    logger.error(f"Failed to create directory {gi_folder}: {str(e)}")
            
            # Write JSON data to file with detailed error handling
            try:
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(json_data)
                
                # Verify file was actually created
                if os.path.exists(file_path):
                    file_size = os.path.getsize(file_path)
                    logger.info(f"Successfully saved {file_name} to {file_path}! File size: {file_size} bytes")
                else:
                    logger.warning(f"File operation completed but {file_path} doesn't exist!")
            except IOError as e:
                logger.error(f"IOError while saving {file_name}: {str(e)}")
            except PermissionError as e:
                logger.error(f"Permission denied when saving {file_name}: {str(e)}")
            except Exception as e:
                logger.error(f"Failed to save {file_name}: {str(e)} (type: {type(e).__name__})")

        logger.info(f"Successfully processed and saved {len(rows)} files.")

    except pyodbc.Error as e:
        logger.error(f"Error while connecting to SQL Server: {e}")
        raise
    except Exception as e:
        logger.error(f"Exception occurred during ETL and save: {e}")
        raise
    finally:
        if 'connection' in locals() and connection:
            connection.close()
            logger.info("SQL Server connection closed.")

# Define Task in Airflow DAG
etl_and_save_local_task = PythonOperator(
    task_id='etl_and_save_to_local',
    python_callable=etl_and_save_local,
    dag=dag,
)