from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date, timedelta
import pyodbc
import json
import base64
from obs import ObsClient
import logging
import os
from tqdm import tqdm
from airflow.models import Variable

# -----------------------------------------------------
# 1. สร้าง Custom Logging Handler สำหรับอัปโหลด log ไปยัง OBS
# -----------------------------------------------------
class ObsLoggingHandler(logging.Handler):
    def __init__(self, obs_client, bucket_name, base_folder):
        """
        :param obs_client: Instance ของ ObsClient สำหรับอัปโหลด log
        :param bucket_name: ชื่อ bucket สำหรับเก็บ log (ตัวอย่าง "tm-log")
        :param base_folder: โฟลเดอร์สำหรับเก็บ logบน OBS (ตัวอย่าง "Airflow_log/TM")
        """
        super().__init__()
        self.obs_client = obs_client
        self.bucket_name = bucket_name
        self.base_folder = base_folder.rstrip('/')  # ลบ / ท้ายสุดออกหากมี
        self.buffer = []  # Buffer สำหรับสะสม log message

    def emit(self, record):
        try:
            log_entry = self.format(record)
            self.buffer.append(log_entry)
        except Exception:
            self.handleError(record)

    def flush(self):
        """อัปโหลด log ที่สะสมอยู่ใน buffer ไปยัง OBS โดยใช้ชื่อไฟล์เป็น TM_YYYYMMDD_HHMMSS.log"""
        if self.buffer:
            log_content = "\n".join(self.buffer)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            object_key = f"{self.base_folder}/TM_{timestamp}.log"
            response = self.obs_client.putContent(
                bucketName=self.bucket_name,
                objectKey=object_key,
                content=log_content
            )
            if response.status < 300:
                self.buffer = []
            else:
                # สามารถเพิ่มกลไก retry หรือ error handling ได้ที่นี่
                pass

    def close(self):
        self.flush()
        super().close()

# -----------------------------------------------------
# 2. Configure Logging (Console + OBS)
# -----------------------------------------------------
# Logging พื้นฐาน (console)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# กำหนดค่า OBS สำหรับ logging
log_bucket_name = "dip-prod-log"         # เปลี่ยนเป็น bucket สำหรับ log ที่คุณต้องการ
log_base_folder = "Airflow_log/TM"       # โฟลเดอร์สำหรับเก็บ logบน OBS

# สร้าง OBS client สำหรับ logging (ใช้ credential เดียวกับ ETL หากต้องการ)
obs_log_config = {
    "access_key_id": Variable.get("OBS_LOG_ACCESS_KEY"),
    "secret_access_key": Variable.get("OBS_LOG_SECRET_KEY"),
    "obs_server": Variable.get("OBS_LOG_SERVER")
}

obs_log_client = ObsClient(
    access_key_id=obs_log_config["access_key_id"],
    secret_access_key=obs_log_config["secret_access_key"],
    server=obs_log_config["obs_server"]
)

# สร้างและแนบ custom logging handler เพื่อส่ง log ไปยัง OBS
obs_log_handler = ObsLoggingHandler(obs_log_client, log_bucket_name, log_base_folder)
obs_log_handler.setLevel(logging.INFO)
obs_log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(obs_log_handler)

# -----------------------------------------------------
# 3. Default Arguments และ Configuration สำหรับ DAG
# -----------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 7),
    'retries': 1,
}

# Define DAG
dag = DAG(
    'TM_direct_to_Huawei_cloud_update', 
    default_args=default_args,
    description='ETL from SQL Server and upload directly to Huawei Cloud ',
    schedule='10 17 * * *'  # Run daily at 17:10 (UTC)
    #schedule=None
)

# SQL Server Connection Details
sql_server_config = {
    "DRIVER": Variable.get("SQL_SERVER_DRIVER"),
    "SERVER": Variable.get("SQL_SERVER_SERVER"),
    "UID": Variable.get("SQL_SERVER_UID"),
    "PWD": Variable.get("SQL_SERVER_PWD"),
    "TrustServerCertificate": Variable.get("SQL_SERVER_TRUST")
}

# Huawei Cloud OBS Configuration สำหรับ ETL upload (ข้อมูล)
huawei_cloud_config = {
    "access_key": Variable.get("HUAWEI_ACCESS_KEY"),
    "secret_key": Variable.get("HUAWEI_SECRET_KEY"),
    "endpoint": Variable.get("HUAWEI_ENDPOINT"),
    "bucket_name": "dip-eexchange-data"
}

# -----------------------------------------------------
# 4. ฟังก์ชัน get_Image สำหรับดึงรูปภาพและแปลงเป็น Base64
# -----------------------------------------------------
def get_Image(physical_path: str) -> str:
    if physical_path:
        try:
            if os.path.isfile(physical_path):
                with open(physical_path, "rb") as image_file:
                    base64_encoded = base64.b64encode(image_file.read()).decode('utf-8')
                    return base64_encoded
            else:
                return None
        except Exception as e:
            return None
    else:
        return None

# -----------------------------------------------------
# 5. ฟังก์ชัน ETL และ Upload ข้อมูล
# -----------------------------------------------------
def etl_and_upload(**kwargs):
    connection = None
    try:
        # ดึงข้อมูล unique FileName จาก SQL Server
        date_now = (datetime.now() - timedelta(2)).strftime('%Y-%m-%d')
        logger.info("Attempting to connect to SQL Server for unique rows...")
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
                CONCAT('/image/', TM_STG_File.physical_path) AS physical_path,
                TM_STG_File.updated_date
            FROM TM_STG_BIBLIO
            LEFT JOIN TM_STG_OWNER ON TM_STG_BIBLIO.id = TM_STG_OWNER.save_id
            LEFT JOIN TM_STG_GOODS ON TM_STG_BIBLIO.id = TM_STG_GOODS.save_id
            LEFT JOIN TM_STG_AGENT ON TM_STG_BIBLIO.id = TM_STG_AGENT.save_id
            LEFT JOIN TM_STG_File ON TM_STG_BIBLIO.id = TM_STG_File.save_id
            WHERE 
                TM_STG_BIBLIO.updated_date > '{date_now}' 
                OR TM_STG_OWNER.updated_date > '{date_now}'
                OR TM_STG_GOODS.updated_date > '{date_now}'
                OR TM_STG_AGENT.updated_date > '{date_now}'
                OR TM_STG_File.updated_date > '{date_now}'
                OR Filing_Date > '{date_now}'
                OR Publication_Date > '{date_now}'
                OR Registration_Date > '{date_now}'
        ) T_x
        """
        cursor.execute(query)
        unique_rows = cursor.fetchall()
        length_unique_rows = len(unique_rows)
        logger.info(f"Fetched {length_unique_rows} unique rows from database.")
        cursor.close()

        unique_rows_sorted = sorted([row[0] for row in unique_rows])
        batch_size = 1000
        n = 0
        
        while True:
            batch_list = unique_rows_sorted[batch_size * n: batch_size*(n+1)]
            if not batch_list:
                logger.info("No more rows to process. Exiting batch loop.")
                break
            FileName_query = "'" + "','".join(batch_list) + "'"
            logger.info("Attempting to connect to SQL Server for batch data...")
            connection = pyodbc.connect(";".join([f"{key}={value}" for key, value in sql_server_config.items()]))
            logger.info("Connected to SQL Server successfully!")
            cursor = connection.cursor()
            columns = ["FileName", "BiblioID", "ApplicationNumber", "Expiration_Date", "Filing_Date",
                       "IRN_Number", "No_Permit", "Publication_Date", "Registration_Date",
                       "Registration_Number", "Status", "Trademark_Type", "OWNER_NAME",
                       "Nationallity_Code", "Class", "GOODS", "Agent_Name", "physical_path", "updated_date"]
            logger.info("Initializing Huawei Cloud OBS Client for ETL upload...")
            client = ObsClient(
                access_key_id=huawei_cloud_config['access_key'],
                secret_access_key=huawei_cloud_config['secret_key'],
                server=huawei_cloud_config['endpoint']
            )
            logger.info("OBS Client initialized successfully.")
            logger.info(f"Executing SQL Query for batch {n+1} with unique TM")
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
                    CONCAT('/image/', TM_STG_File.physical_path) AS physical_path,
                    TM_STG_AGENT.updated_date
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
            logger.info(f"Fetched {length} rows from database for batch {n+1}.")

            if not rows:
                logger.info("No rows returned for this batch. Exiting loop.")
                break

            rows_dict = [dict(zip(columns, row)) for row in rows]
            unique_TM = {}
            logger.info("Processing and transforming data.")
            for row in rows_dict:
                TM_file = row.get("FileName")
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
                Update_date = row.get("updated_date")
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

                if TM_file in unique_TM:
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
                    Owner = {"OWNER_NAME": OWNER_NAME, "Nationallity": Nationality}
                    if Owner not in unique_TM[TM_file]["OWNER_NAMEList"]:
                        unique_TM[TM_file]["OWNER_NAMEList"].append(Owner)
                    Agent = {"AGENT_NAME": Agent_Name}
                    if Agent not in unique_TM[TM_file]["AGENT_NAMEList"]:
                        unique_TM[TM_file]["AGENT_NAMEList"].append(Agent)
                    unique_TM[TM_file]["ApplicationNumber"] = ApplicationNumber
                    if Biblio_ID not in unique_TM[TM_file]["Biblio_ID"]:
                        unique_TM[TM_file]["Biblio_ID"].append(Biblio_ID)
                    if Status not in unique_TM[TM_file]["Status"]:
                        unique_TM[TM_file]["Status"].append(Status)
                    if No_Permit not in unique_TM[TM_file]["No_Permit"]:
                        unique_TM[TM_file]["No_Permit"].append(No_Permit)
                    if IRN_Number not in unique_TM[TM_file]["IRN_Number"]:
                        unique_TM[TM_file]["IRN_Number"].append(IRN_Number)
                    if Trademark_Type not in unique_TM[TM_file]["Trademark_Type"]:
                        unique_TM[TM_file]["Trademark_Type"].append(Trademark_Type)
                    if Registration_Number not in unique_TM[TM_file]["Registration_Number"]:
                        unique_TM[TM_file]["Registration_Number"].append(Registration_Number)
                    if Update_date not in unique_TM[TM_file]["Update_date"]:
                        unique_TM[TM_file]["Update_date"].append(Update_date)
                    if Filing_Date not in unique_TM[TM_file]["Filing_Date"]:
                        unique_TM[TM_file]["Filing_Date"].append(Filing_Date)
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
                        "Biblio_ID": [Biblio_ID],
                        "Status": [Status],
                        "No_Permit": [No_Permit],
                        "IRN_Number": [IRN_Number],
                        "Trademark_Type": [Trademark_Type],
                        "Registration_Number": [Registration_Number],
                        "Update_date": [Update_date],
                        "Filing_Date": [Filing_Date],
                        "Publication_Date": [Publication_Date],
                        "Registration_Date": [Registration_Date],
                        "Expiration_Date": [Expiration_Date]
                    }

            logger.info("Injecting data to Cloud.")
            for row_key, row_value in unique_TM.items():
                TM_image = get_Image(row_value['Physical_Path'])
                file_name = row_key + ".json"
                json_data = json.dumps(row_value | {"physical_path": TM_image}, ensure_ascii=False, indent=4)
                folder_name = "trademark/"  # ปรับ folder ตามที่ต้องการ
                object_key = f"{folder_name}{file_name}"
                response = client.putContent(
                    bucketName=huawei_cloud_config['bucket_name'],
                    objectKey=object_key,
                    content=json_data
                )
                if response.status < 300:
                    logger.info(f"Successfully uploaded {file_name}!")
                else:
                    logger.error(f"Failed to upload {file_name}: {response.errorMessage}")
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

# -----------------------------------------------------
# 6. สร้าง Task สำหรับ ETL and Upload
# -----------------------------------------------------
etl_and_upload_task = PythonOperator(
    task_id='etl_and_upload_task',
    python_callable=etl_and_upload,
    dag=dag,
)
