from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date, timedelta
import pyodbc
import json
from obs import ObsClient
import logging
import os
import re
from airflow.models import Variable

# ---------------------------------------------------------
# 1. กำหนด Custom Logging Handler สำหรับอัปโหลด log ไปยัง OBS
# ---------------------------------------------------------
class ObsLoggingHandler(logging.Handler):
    def __init__(self, obs_client, bucket_name, base_folder):
        """
        :param obs_client: Instance ของ ObsClient สำหรับอัปโหลด log
        :param bucket_name: ชื่อ bucket สำหรับเก็บ log (เช่น "dip-prod-log")
        :param base_folder: โฟลเดอร์สำหรับเก็บ log (เช่น "Airflow_log/GI")
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
        if self.buffer:
            log_content = "\n".join(self.buffer)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            object_key = f"{self.base_folder}/GI_{timestamp}.log"
            response = self.obs_client.putContent(
                bucketName=self.bucket_name,
                objectKey=object_key,
                content=log_content
            )
            if response.status < 300:
                self.buffer = []
            else:
                # สามารถเพิ่มกลไก retry ได้ที่นี่
                pass

    def close(self):
        self.flush()
        super().close()

# ---------------------------------------------------------
# 2. Configure Logging (Console + OBS)
# ---------------------------------------------------------
# กำหนด logging พื้นฐาน (console)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# กำหนดค่า OBS สำหรับ logging (ใช้ credential เดียวกันกับ ETL)
# โดย log จะถูกเก็บใน bucket "dip-prod-log" ภายในโฟลเดอร์ "Airflow_log/GI"
log_bucket_name = "dip-prod-log"      # Bucket สำหรับ log
log_base_folder = "Airflow_log/GI"      # โฟลเดอร์สำหรับเก็บ log

# สร้าง OBS client สำหรับ logging
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

# สร้างและแนบ ObsLoggingHandler เข้ากับ logger
obs_log_handler = ObsLoggingHandler(obs_log_client, log_bucket_name, log_base_folder)
obs_log_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
obs_log_handler.setFormatter(formatter)
logger.addHandler(obs_log_handler)

# ---------------------------------------------------------
# 3. กำหนด Default Arguments และ Configuration สำหรับ DAG
# ---------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 7),
    'retries': 1,
}

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

# ---------------------------------------------------------
# 4. กำหนด DAG
# ---------------------------------------------------------
dag = DAG(
    'GI_direct_to_Huawei_cloud_update',
    default_args=default_args,
    description='ETL from SQL Server and upload directly to Huawei Cloud with Batch Loading',
    schedule='10 15 * * *'
    #schedule=None
)

# ---------------------------------------------------------
# 5. กำหนดฟังก์ชัน ETL และ Upload ข้อมูล
# ---------------------------------------------------------
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


def etl_and_upload(**kwargs):
    try:
        upload_count = 0
        # คำนวณวันที่ปัจจุบันลบออกไป 2 วัน
        date_now = (datetime.now() - timedelta(2)).strftime('%Y-%m-%d')
        logger.info(f"Fetching data updated after: {date_now}")
        
            # สร้าง connection string
        conn_str = ";".join([f"{key}={value}" for key, value in sql_server_config.items()])
        
        # Step 1: ดึง unique FileName จาก GI_Product (รวมกับ GI_Publication เพื่อใช้เงื่อนไข)
        unique_query = f"""
        SELECT DISTINCT CONCAT('GI_', prd.PROD_NUMBER) AS FileName
        FROM [gi_pre].[dbo].[GI_Product] AS prd
        LEFT JOIN [gi_pre].[dbo].[GI_Publication] AS pub ON prd.id = pub.prod_id
        WHERE 
            prd.REGIS_NUMBER IS NOT NULL
            AND (
                prd.SUBMIT_DATE > '{date_now}' OR
                prd.PUBLIC_DATE > '{date_now}' OR
                prd.REGIS_DATE > '{date_now}' OR
                CAST(pub.ITEM1_DEFINITION AS NVARCHAR(MAX)) > '{date_now}' OR
                CAST(pub.ITEM4_LOCATION AS NVARCHAR(MAX)) > '{date_now}' OR
                CAST(pub.ITEM3_TOPOGRAPHY AS NVARCHAR(MAX)) > '{date_now}' OR
                CAST(pub.ITEM3_HISTORY AS NVARCHAR(MAX)) > '{date_now}' OR
                CAST(pub.ITEM6_SECTION15 AS NVARCHAR(MAX)) > '{date_now}' OR
                CAST(pub.ITEM5_ORIGIN AS NVARCHAR(MAX)) > '{date_now}' OR
                prd.UPDATE_ON > '{date_now}'
            )
        ORDER BY CONCAT('GI_', prd.PROD_NUMBER)
        """
        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()
        cursor.execute(unique_query)
        unique_rows = cursor.fetchall()
        unique_file_names = [row[0] for row in unique_rows]
        total_unique = len(unique_file_names)
        logger.info(f"Fetched {total_unique} unique GI file names from database.")
        cursor.close()
        connection.close()
        
        unique_file_names.sort()
        
        batch_size = 1000
        n = 0
        while True:
            batch_list = unique_file_names[batch_size * n : batch_size * (n+1)]
            if not batch_list:
                logger.info("No more file names to process. Exiting batch loop.")
                break
            file_filter = "'" + "','".join(batch_list) + "'"
            
            # ดึงข้อมูลรายละเอียดสำหรับ GI ใน batch นี้
            connection = pyodbc.connect(conn_str)
            cursor = connection.cursor()
            detailed_query = f"""
            SELECT
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
                cn.NAME_TH AS COUNTRY_NAME,
                prd.UPDATE_ON
            FROM [gi_pre].[dbo].[GI_Product] AS prd
            LEFT JOIN [gi_pre].[dbo].[GI_ProductLocation] AS pl ON prd.id = pl.prod_id
            LEFT JOIN [gi_pre].[dbo].[GI_Category] AS cat ON cat.id = prd.cat_id
            LEFT JOIN [gi_pre].[dbo].[GI_Type] AS typ ON typ.id = prd.gi_type
            LEFT JOIN [gi_pre].[dbo].[GI_Publication] AS pub ON prd.id = pub.prod_id
            LEFT JOIN [gi_pre].[dbo].[MS_Province] AS prv ON pl.province_id = prv.id
            LEFT JOIN [gi_pre].[dbo].[MS_Region] AS reg ON prv.region_id = reg.id
            LEFT JOIN [gi_pre].[dbo].[MS_Country] AS cn ON pl.country_id = cn.id
            WHERE CONCAT('GI_', prd.PROD_NUMBER) IN ({file_filter})
            ORDER BY prd.REGIS_NUMBER, prd.PROD_NUMBER
            """
            cursor.execute(detailed_query)
            rows = cursor.fetchall()
            num_rows = len(rows)
            logger.info(f"Fetched {num_rows} rows for batch {n+1}.")
            cursor.close()
            connection.close()
            
            columns = ["FileName", "PROD_NUMBER", "SUBMIT_DATE", "PUBLIC_DATE", "REGIS_NUMBER",
                    "GI_NAME", "GI_PRODUCT", "GI_NAME_EN", "REGIS_DATE", "GI_CATEGORY_NAME",
                    "TYPE_NAME", "PROVINCE_NAME", "REGION_NAME", "ITEM1_DEFINITION",
                    "ITEM4_LOCATION", "ITEM3_TOPOGRAPHY", "ITEM3_HISTORY", "ITEM6_SECTION15",
                    "ITEM5_ORIGIN", "APPLICANT", "COUNTRY_NAME", "UPDATE_ON"]
            data_dict = {}
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

                for key, value in row_dict.items():
                    if isinstance(value, (datetime, date)):
                        row_dict[key] = value.strftime('%Y-%m-%d')
                file_key = row_dict["FileName"]
                data_dict[file_key] = row_dict
            
            # สร้าง OBS Client สำหรับการอัปโหลด
            client = ObsClient(
                access_key_id=huawei_cloud_config['access_key'],
                secret_access_key=huawei_cloud_config['secret_key'],
                server=huawei_cloud_config['endpoint']
            )
            logger.info("OBS Client (ETL) initialized successfully.")
            
            logger.info(f"Injecting data to Cloud for batch {n+1}.")
            folder_name = "gi/"
            for file_key, data in data_dict.items():
                file_name = file_key + ".json"
                json_data = json.dumps(data, ensure_ascii=False, indent=4)
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
            
            client.close()
            logger.info("OBS Client (ETL) connection closed.")
            n += 1
        
        logger.info("All batches processed successfully.")
        
    except pyodbc.Error as e:
        logger.error(f"Error while connecting to SQL Server: {e}")
        raise
    except Exception as e:
        logger.error(f"Exception occurred during ETL and upload: {e}")
        raise
    finally:
        try:
            if connection:
                connection.close()
                logger.info("SQL Server connection closed.")
        except Exception:
            pass

# ---------------------------------------------------------
# 6. สร้าง Task สำหรับ ETL and Upload
# ---------------------------------------------------------
etl_and_upload_task = PythonOperator(
    task_id='etl_and_upload_to_cloud',
    python_callable=etl_and_upload,
    dag=dag,
)
