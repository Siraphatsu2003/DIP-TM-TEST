from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
import pyodbc
import json
import base64
from obs import ObsClient
import logging
import os
from tqdm import tqdm
from airflow.models import Variable
# from memory_profiler import profile

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
    'TM_direct_to_Huawei_cloud',
    default_args=default_args,
    description='ETL from SQL Server and upload directly to Huawei Cloud ',
    # schedule_interval='15 13 * * *',  # Run daily at 20:00 UTC+7
    schedule=None
)

# SQL Server Connection Details
sql_server_config = {
    "DRIVER": Variable.get("SQL_SERVER_DRIVER"),
    "SERVER": Variable.get("SQL_SERVER_SERVER"),
    "UID": Variable.get("SQL_SERVER_UID"),
    "PWD": Variable.get("SQL_SERVER_PWD"),
    "TrustServerCertificate": Variable.get("SQL_SERVER_TRUST")
}

# Huawei Cloud OBS Configuration สำหรับ ETL upload
huawei_cloud_config = {
    "access_key": Variable.get("HUAWEI_ACCESS_KEY"),
    "secret_key": Variable.get("HUAWEI_SECRET_KEY"),
    "endpoint": Variable.get("HUAWEI_ENDPOINT"),
    "bucket_name": "dip-eexchange-data"
}

# @profile
def get_Image(physical_path : str) -> str :
    if physical_path:
        # logger.info(f"Processing physical_path: {physical_path}")
        # print((f"Processing physical_path: {physical_path}"))
        try:
            if os.path.isfile(physical_path):
                with open(physical_path, "rb") as image_file:
                    base64_encoded = base64.b64encode(image_file.read()).decode('utf-8')
                    # row_dict["TM_image"] = base64_encoded
                    return base64_encoded
            else:
                # logger.warning(f"Invalid or missing file path: {physical_path}")
                # print(f"Invalid or missing file path: {physical_path}")
                # row_dict["TM_image"] = None
                return None
        except Exception as e:

            # logger.error(f"Error processing file at {physical_path}: {e}")
            # print(f"Error processing file at {physical_path}: {e}")
            # row_dict["TM_image"] = None
            return None
    else:
        # row_dict["TM_image"] = None
        return None
# @profile
def etl_and_upload(**kwargs):
    try:
        # Connect to SQL Server
        logger.info("Attempting to connect to SQL Server...")
        connection = pyodbc.connect(";".join([f"{key}={value}" for key, value in sql_server_config.items()]))
        logger.info("Connected to SQL Server successfully!")
        cursor = connection.cursor()
        query = f"""
        SELECT DISTINCT FileName 
        FROM 
        (
                SELECT
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
        ) T_x
        """
        cursor.execute(query)
        unique_rows = cursor.fetchall()
        length_unique_rows = len(unique_rows)
        
        logger.info(f"Fetched {len(unique_rows)} unique rows from database.")
        cursor.close()
        del cursor
        # FileName_query
        unique_rows_sorted = sorted([unique_row[0] for unique_row in unique_rows])
        # Batch processing with OFFSET and FETCH NEXT
        batch_size = 10000
        # offset = 3000000

        # unique_TM = {}
        # n = 83
        n = 120
        init_step = 120 * batch_size
        inject_bar = tqdm(total=length_unique_rows, desc="Injecting data", position=0, dynamic_ncols=True, initial=init_step)
        while True:
            FileName_query = "'" + "','".join(unique_rows_sorted[batch_size * n: batch_size*(n+1)]) + "'"
            # Connect to SQL Server
            logger.info("Attempting to connect to SQL Server...")
            connection = pyodbc.connect(";".join([f"{key}={value}" for key, value in sql_server_config.items()]))
            logger.info("Connected to SQL Server successfully!")
            cursor = connection.cursor()

            # Columns
            columns = ["FileName", "BiblioID", "ApplicationNumber", "Expiration_Date", "Filing_Date",
                    "IRN_Number", "No_Permit", "Publication_Date", "Registration_Date",
                    "Registration_Number", "Status", "Trademark_Type", "OWNER_NAME",
                    "Nationallity_Code", "Class", "GOODS", "Agent_Name", "physical_path","Updated_date"]

            # Initialize Huawei Cloud OBS Client
            logger.info("Initializing Huawei Cloud OBS Client...")
            client = ObsClient(
                access_key_id=huawei_cloud_config['access_key'],
                secret_access_key=huawei_cloud_config['secret_key'],
                server=huawei_cloud_config['endpoint']
            )
            logger.info("OBS Client initialized successfully.")

            logger.info(f"Executing SQL Query with unique TM from {n*batch_size} - {(n+1)*batch_size} rows")
            query = f"""
            SELECT *
            FROM 
            (
            SELECT 
                CONCAT('TM_', TM_STG_BIBLIO.id) AS FileName,
                TM_STG_BIBLIO.id AS BiblioID,
                TM_STG_BIBLIO.Application_Number AS ApplicationNumber,
                CAST(TM_STG_BIBLIO.Expiration_Date AS DATE) AS Expiration_Date,
                CAST(TM_STG_BIBLIO.Filing_Date AS DATE) AS Filing_Date,
                TM_STG_BIBLIO.IRN_Number,
                TM_STG_BIBLIO.No_Permit,
                CAST(TM_STG_BIBLIO.Publication_Date AS DATE) AS Publication_Date,
                CAST(TM_STG_BIBLIO.Registration_Date AS DATE) AS Registration_Date,
                TM_STG_BIBLIO.Registration_Number,
                TM_STG_BIBLIO.Status,
                TM_STG_BIBLIO.Trademark_Type,
                TM_STG_OWNER.Owner_Name AS OWNER_NAME,
                TM_STG_OWNER.Nationallity_Code,
                TM_STG_GOODS.Class,
                TM_STG_GOODS.Goods AS GOODS,
                TM_STG_AGENT.Agent_Name,
                CONCAT('/image/', TM_STG_File.physical_path) AS physical_path
                ,TM_STG_AGENT.updated_date AS updated_date
            FROM TM_STG_BIBLIO
            LEFT JOIN TM_STG_OWNER ON TM_STG_BIBLIO.id = TM_STG_OWNER.save_id
            LEFT JOIN TM_STG_GOODS ON TM_STG_BIBLIO.id = TM_STG_GOODS.save_id
            LEFT JOIN TM_STG_AGENT ON TM_STG_BIBLIO.id = TM_STG_AGENT.save_id
            LEFT JOIN TM_STG_File ON TM_STG_BIBLIO.id = TM_STG_File.save_id
            ) T_x 
            WHERE FileName in ({FileName_query}) 
            ORDER BY FileName DESC
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            length = len(rows)
            logger.info(f"Fetched {length} rows from database.")

            if not rows:
                logger.info("No more rows to process. Exiting loop.")
                break

            # Process rows and upload directly
            # for row in rows:
            #     row_dict = dict(zip(columns, row))

                # Convert date/datetime fields to strings
                # for key, value in row_dict.items():
                #     if isinstance(value, (datetime, date)):
                #         row_dict[key] = value.strftime('%Y-%m-%d')
            rows_dict = [dict(zip(columns,row)) for row in rows]
            unique_TM = {}
            logger.info(f"Processing transform data.")
            # for row in tqdm(rows_dict, total = len(rows_dict)):
            for row in rows_dict :
                TM_file = row.get("FileName")
                
                # Extract necessary info once
                Class_ID = row.get("Class")
                GOODS = row.get("GOODS")
                OWNER_NAME = row.get("OWNER_NAME")
                Nationality = row.get("Nationallity_Code")
                Agent_Name = row.get("Agent_Name")
                Physical_Path = row.get("physical_path")
                ApplicationNumber = row.get("ApplicationNumber")
                Biblio_ID = row.get("BiblioID")
                Status = row.get("Status")
                Trademark_Type = row.get("Trademark_Type")
                Registration_Number = row.get("Registration_Number")
                No_Permit = row.get("No_Permit")
                IRN_Number = row.get("IRN_Number")

                Update_date = row.get("Updated_date")
                if isinstance(Update_date, (datetime, date)):
                    Update_date = Update_date.strftime('%Y-%m-%d')
                    
                Filing_Date = row.get("Filing_Date")
                if isinstance(Filing_Date, (datetime, date)):
                    Filing_Date = Filing_Date.strftime('%Y-%m-%d')

                Publication_Date = row.get("Publication_Date")
                if isinstance(Publication_Date, (datetime, date)):
                    Publication_Date = Publication_Date.strftime('%Y-%m-%d')

                Registration_Date = row.get("Registration_Date")
                if isinstance(Registration_Date, (datetime, date)):
                    Registration_Date = Registration_Date.strftime('%Y-%m-%d')

                Expiration_Date = row.get("Expiration_Date")
                if isinstance(Expiration_Date, (datetime, date)):
                    Expiration_Date = Expiration_Date.strftime('%Y-%m-%d')

                # Image = get_Image(physical_path = Physical_Path)
                
                if TM_file in unique_TM:
                    # Update ClassList logic
                    class_found = False
                    for i, item in enumerate(unique_TM[TM_file]["ClassList"]):
                        if Class_ID == item["Class"]:
                            if {"GOODS": GOODS} not in unique_TM[TM_file]["ClassList"][i]["GOODSList"]:
                                unique_TM[TM_file]["ClassList"][i]["GOODSList"].append({"GOODS": GOODS})
                            class_found = True
                            break
                    if not class_found:
                        unique_TM[TM_file]["ClassList"].append(
                            {
                                "Class": Class_ID,
                                "GOODSList": [{"GOODS": GOODS}]
                            }
                        )

                    # Update Owner list
                    Owner = {"OWNER_NAME": OWNER_NAME, "Nationallity": Nationality}
                    if Owner not in unique_TM[TM_file]["OWNER_NAMEList"]:
                        unique_TM[TM_file]["OWNER_NAMEList"].append(Owner)

                    # Update Agent list
                    Agent = {"AGENT_NAME": Agent_Name}
                    if Agent not in unique_TM[TM_file]["AGENT_NAMEList"]:
                        unique_TM[TM_file]["AGENT_NAMEList"].append(Agent)

                    # Update TM Image list
                    # if Physical_Path not in unique_TM[TM_file]["TM_image"]:
                    # unique_TM[TM_file]["Physical_Path"] = Physical_Path

                    # Update Application list
                    # Application = {"Application_Number": Application_Number}
                    # if Application not in unique_TM[TM_file]["Application"]:
                    unique_TM[TM_file]["ApplicationNumber"] = ApplicationNumber

                    # Update BiblioID list (FIXED)
                    # BiblioID = {"BiblioID": Biblio_ID}
                    if Biblio_ID not in unique_TM[TM_file]["Biblio_ID"]:
                        unique_TM[TM_file]["Biblio_ID"].append(Biblio_ID)

                    # Update Status list (FIXED)
                    if Status not in unique_TM[TM_file]["Status"]:
                        unique_TM[TM_file]["Status"].append(Status)

                    # Update No_Permit list (FIXED)
                    if No_Permit not in unique_TM[TM_file]["No_Permit"]:
                        unique_TM[TM_file]["No_Permit"].append(No_Permit)
                
                    # Update IRN_Number list (FIXED)
                    if IRN_Number not in unique_TM[TM_file]["IRN_Number"]:
                        unique_TM[TM_file]["IRN_Number"].append(IRN_Number)
                    
                    # Update Trademark_Type list (FIXED)
                    if Trademark_Type not in unique_TM[TM_file]["Trademark_Type"]:
                        unique_TM[TM_file]["Trademark_Type"].append(Trademark_Type)

                    # Update Registration_Number list (FIXED)
                    if Registration_Number not in unique_TM[TM_file]["Registration_Number"]:
                        unique_TM[TM_file]["Registration_Number"].append(Registration_Number)

                    # Update Updated_date list (FIXED)
                    if Update_date not in unique_TM[TM_file]["Update_date"]:
                        unique_TM[TM_file]["Update_date"].append(Update_date)
                    
                    # Update Filing_date list (FIXED)
                    if Filing_Date not in unique_TM[TM_file]["Filing_Date"]:
                        unique_TM[TM_file]["Filing_Date"].append(Filing_Date)

                    # Update Publication_date list (FIXED)
                    if Publication_Date not in unique_TM[TM_file]["Publication_Date"]:
                        unique_TM[TM_file]["Publication_Date"].append(Publication_Date)


                else:
                    unique_TM[TM_file] = {
                        "ClassList": [
                            {
                                "Class": Class_ID,
                                "GOODSList": [{"GOODS": GOODS}]
                            }
                        ],
                        "OWNER_NAMEList": [
                            {
                                "OWNER_NAME": OWNER_NAME,
                                "Nationallity": Nationality
                            }
                        ],
                        "AGENT_NAMEList": [
                            {
                                "AGENT_NAME": Agent_Name
                            }
                        ],
                        "Physical_Path": Physical_Path,
                        "ApplicationNumber": ApplicationNumber,
                        "Biblio_ID": [
                            Biblio_ID
                        ],
                        "Status": [
                            Status
                        ],
                        "No_Permit" : [
                            No_Permit
                        ],
                        "IRN_Number": [
                            IRN_Number
                        ],
                        "Trademark_Type": [
                            Trademark_Type
                        ],
                        "Registration_Number": [
                            Registration_Number
                        ],
                        "Update_date" : [
                            Update_date
                        ],
                        "Filing_Date" : [
                            Filing_Date
                        ],
                        "Publication_Date" : [
                            Publication_Date
                        ],
                        "Registration_Date" : [
                            Registration_Date
                        ],
                        "Expiration_Date" : [
                            Expiration_Date
                        ]
                    }

            logger.info(f"Inject data to Cloud.")
            # length_TM = len(unique_TM)
            
            for row_key, row_value in unique_TM.items() :
                TM_image = get_Image(row_value['Physical_Path'])
                # unique_TM[row_key] = get_Image(i["Image"])
                # Convert to JSON string
                # file_name = row_dict.pop("FileName") + ".json"
                file_name = row_key + ".json"
                json_data = json.dumps( row_value | {"physical_path" : TM_image} , ensure_ascii=False, indent=4)

                # Define the folder path within the bucket
                folder_name = "trademark/"  # Change this to your desired folder name (ensure it ends with '/')

                # Combine folder name and file name to form the full object key
                object_key = f"{folder_name}{file_name}"

                # Upload JSON data to OBS
                response = client.putContent(
                    bucketName=huawei_cloud_config['bucket_name'],
                    objectKey=object_key,
                    content=json_data
                )
                if response.status < 300:
                    # logger.info(f"Successfully uploaded {file_name}!")
                    inject_bar.write(f"Successfully uploaded {file_name}!")
                else:
                    # logger.error(f"Failed to upload {file_name}: {response.errorMessage}")
                    inject_bar.write(f"Failed to upload {file_name}: {response.errorMessage}")

                # message = f"Upload {row_key} to Cloud"
                # inject_bar.write(message)
                inject_bar.update(1)
                # tqdm.write(f"Processed up to {row_key}/{len(unique_TM)}
            
            # Increment offset for the next batch
            n += 1
            del rows_dict
            del unique_TM
            del cursor


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

if __name__ == "__main__" :
    etl_and_upload()

# Define Task
etl_and_upload_task = PythonOperator(
    task_id='etl_and_upload_task',
    python_callable=etl_and_upload,
    dag=dag,
)
