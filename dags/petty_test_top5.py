from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
import pyodbc
import json
import re
from obs import ObsClient
import logging
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
    'PPATENT_direct_to_Huawei_cloud',
    default_args=default_args,
    description='ETL from SQL Server PPATENT data and upload to Huawei Cloud OBS (with aggregation)',
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

# Huawei Cloud OBS Configuration
huawei_cloud_config = {
    "access_key": Variable.get("HUAWEI_ACCESS_KEY"),
    "secret_key": Variable.get("HUAWEI_SECRET_KEY"),
    "endpoint": Variable.get("HUAWEI_ENDPOINT"),
    "bucket_name": "dip-eexchange-data"
}

def clean_html(text):
    """
    ทำความสะอาด HTML tags และตัวอักษรพิเศษ
    """
    if not text:
        return text
    # ลบ HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # แทนที่ &nbsp; และ &#160; ด้วยเว้นวรรค
    text = re.sub(r'&nbsp;|&#160;', ' ', text)
    # ลบ carriage return และ newline characters
    text = re.sub(r'[\r\n]+', ' ', text)
    # ลดเว้นวรรคที่ซ้ำกัน
    text = re.sub(r'\s+', ' ', text)
    # ลบ escaped double quotes
    text = re.sub(r'\"', ' ', text)
    # ลบ <p> tags
    text = re.sub(r'</?p[^>]*>', '', text)
    return text.strip()

def etl_and_upload_ppatent(**kwargs):
    try:
        # เชื่อมต่อกับ SQL Server
        connection = pyodbc.connect(";".join([f"{key}={value}" for key, value in sql_server_config.items()]))
        logger.info("Connected to SQL Server successfully!")
        cursor = connection.cursor()

        # SQL Query สำหรับ PPATENT
        query = """
        SELECT TOP 5
            CONCAT('PPATENT_', ApplicationNumber) AS FileName,
            ApplicationNumber,
            RelatedApplication,
            RelatedApplicatioDetails,
            ApplicationDate,
            PublicationNumber,
            PatentNumber,
            Title,
            PublicationPage,
            PriorityApplicationNumber,
            FirstPriorityDate as PriorityDate,
            PriorityApplicationCountry,
            PCT,
            PCTApplicationNumber,
            TAGS_NAME as Tag,
            Claim,
            Abstract,
            CurrentStatusCode,
            CurrentStatusDate,
            Applicant,
            Creator as Inventor,
            Agent,
            IPCClasses,
            ApplicantNationalityCode,
            InventorNationalityCode,
            'https://patentpub.ipthailand.go.th/_AppPublic/PublicAttach.aspx?appno=' + ApplicationNumber AS PatentGAZ,
            'https://patentsearch.ipthailand.go.th/DIPSearch/Tools/DisplayImage.aspx?appno=' + ApplicationNumber + '&type=2' AS Description,
            'https://patentsearch.ipthailand.go.th/DIPSearch/Tools/DisplayImage.aspx?appno=' + ApplicationNumber + '&type=97' AS PatentCertification,
            'https://patentpub.ipthailand.go.th/_AppPublic/public_doc.aspx?appno=' + ApplicationNumber AS PatentDocument,
            PublicationRegistrationDate as PublicationDate,
            CheckDate,
            ApplicationRejectDate,
            ApplicationLeaveDate,
            GrantedDate,
            PaymentDate,
            ExpiryDate,
            ReceivingOfficeDate,
            PCTApplicationDate,
            AGENTID as AgentID,
            LicenceDate
        FROM DWH.dbo.API_Exchange_PATENT3
        ORDER BY ApplicationDate DESC;
        """
        cursor.execute(query)

        # กำหนดชื่อ column ตามลำดับใน Query
        columns = [
            "FileName",
            "ApplicationNumber",
            "RelatedApplication",
            "RelatedApplicatioDetails",
            "ApplicationDate",
            "PublicationNumber",
            "PatentNumber",
            "Title",
            "PublicationPage",
            "PriorityApplicationNumber",
            "FirstPriorityDate",
            "PriorityApplicationCountry",
            "PCT",
            "PCTApplicationNumber",
            "TAGS_NAME",
            "Claim",
            "Abstract",
            "CurrentStatusCode",
            "CurrentStatusDate",
            "Applicant",
            "Creator",
            "Agent",
            "IPCClasses",
            "ApplicantNationalityCode",
            "InventorNationalityCode",
            "PublicAttachURL",
            "DisplayImageURL1",
            "DisplayImageURL2",
            "PublicDocURL",
            "PublicationDate",
            "OppositionDate",
            "CheckDate",
            "RegistrationDate",
            "ApplicationRejectDate",
            "ApplicationLeaveDate",
            "GrantedDate",
            "PaymentDate",
            "ExpiryDate",
            "ReceivingOfficeDate",
            "PCTApplicationDate",
            "AGENTID",
            "LicenceDate"
        ]

        # ใช้ dictionary เพื่อรวมข้อมูล (aggregation) โดย key คือ ApplicationNumber
        aggregated_ppatents = {}

        batch_size = 10000
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                logger.info("No more rows to process.")
                break

            for row in rows:
                row_dict = dict(zip(columns, row))
                
                # แปลงวันที่ให้เป็น string
                for key, value in row_dict.items():
                    if isinstance(value, (datetime, date)):
                        row_dict[key] = value.strftime('%Y-%m-%d')
                
                # ทำความสะอาดข้อมูลใน field ที่อาจมี HTML หรือ special characters
                for field in ["Title", "Claim", "Abstract", "RelatedApplicatioDetails"]:
                    row_dict[field] = clean_html(row_dict.get(field))
                
                app_no = row_dict.get("ApplicationNumber")
                if not app_no:
                    continue

                if app_no not in aggregated_ppatents:
                    aggregated_ppatents[app_no] = {
                        "ApplicationNumber": app_no,
                        "RelatedApplication": row_dict.get("RelatedApplication"),
                        "RelatedApplicatioDetails": row_dict.get("RelatedApplicatioDetails"),
                        "ApplicationDate": row_dict.get("ApplicationDate"),
                        "PublicationNumber": row_dict.get("PublicationNumber"),
                        "PatentNumber": row_dict.get("PatentNumber"),
                        "Title": row_dict.get("Title"),
                        "PublicationPage": row_dict.get("PublicationPage"),
                        "PriorityApplicationNumber": row_dict.get("PriorityApplicationNumber"),
                        "FirstPriorityDate": row_dict.get("FirstPriorityDate"),
                        "PriorityApplicationCountry": row_dict.get("PriorityApplicationCountry"),
                        "PCT": row_dict.get("PCT"),
                        "PCTApplicationNumber": row_dict.get("PCTApplicationNumber"),
                        "TAGS_NAME": row_dict.get("TAGS_NAME"),
                        "Claim": row_dict.get("Claim"),
                        "Abstract": row_dict.get("Abstract") or "ไม่ระบุ",
                        "CurrentStatusCode": row_dict.get("CurrentStatusCode"),
                        "CurrentStatusDate": row_dict.get("CurrentStatusDate"),
                        # many-to-many fields
                        "ApplicantInformation": [],
                        "InventorInformation": [],
                        "AgenInformation": [],
                        "IPCClasses": row_dict.get("IPCClasses"),
                        "ApplicantNationalityCode": row_dict.get("ApplicantNationalityCode"),
                        "InventorNationalityCode": row_dict.get("InventorNationalityCode"),
                        "PublicAttachURL": row_dict.get("PublicAttachURL"),
                        "DisplayImageURL1": row_dict.get("DisplayImageURL1"),
                        "DisplayImageURL2": row_dict.get("DisplayImageURL2"),
                        "PublicDocURL": row_dict.get("PublicDocURL"),
                        "PublicationDate": row_dict.get("PublicationDate"),
                        "OppositionDate": row_dict.get("OppositionDate"),
                        "CheckDate": row_dict.get("CheckDate"),
                        "RegistrationDate": row_dict.get("RegistrationDate"),
                        "ApplicationRejectDate": row_dict.get("ApplicationRejectDate"),
                        "ApplicationLeaveDate": row_dict.get("ApplicationLeaveDate"),
                        "GrantedDate": row_dict.get("GrantedDate"),
                        "PaymentDate": row_dict.get("PaymentDate"),
                        "ExpiryDate": row_dict.get("ExpiryDate"),
                        "ReceivingOfficeDate": row_dict.get("ReceivingOfficeDate"),
                        "PCTApplicationDate": row_dict.get("PCTApplicationDate"),
                        "AGENTID": row_dict.get("AGENTID"),
                        "LicenceDate": row_dict.get("LicenceDate")
                    }
                
                # รวมข้อมูล ApplicantInformation (Applicant + ApplicantNationalityCode)
                applicant_info = {}
                if row_dict.get("Applicant"):
                    applicant_info["Applicant"] = row_dict.get("Applicant")
                if row_dict.get("ApplicantNationalityCode"):
                    applicant_info["ApplicantNationalityCode"] = row_dict.get("ApplicantNationalityCode")
                if applicant_info and applicant_info not in aggregated_ppatents[app_no]["ApplicantInformation"]:
                    aggregated_ppatents[app_no]["ApplicantInformation"].append(applicant_info)

                # รวมข้อมูล InventorInformation (ใช้ Creator เป็น Inventor)
                inventor_info = {}
                if row_dict.get("Creator"):
                    inventor_info["Inventor"] = row_dict.get("Creator")
                if row_dict.get("InventorNationalityCode"):
                    inventor_info["InventorNationalityCode"] = row_dict.get("InventorNationalityCode")
                if inventor_info and inventor_info not in aggregated_ppatents[app_no]["InventorInformation"]:
                    aggregated_ppatents[app_no]["InventorInformation"].append(inventor_info)

                # รวมข้อมูล AgenInformation (Agent)
                agent_info = {}
                if row_dict.get("Agent"):
                    agent_info["Agent"] = row_dict.get("Agent")
                if agent_info and agent_info not in aggregated_ppatents[app_no]["AgenInformation"]:
                    aggregated_ppatents[app_no]["AgenInformation"].append(agent_info)

        # Initialize Huawei Cloud OBS Client
        client = ObsClient(
            access_key_id=huawei_cloud_config['access_key'],
            secret_access_key=huawei_cloud_config['secret_key'],
            server=huawei_cloud_config['endpoint']
        )
        logger.info("OBS Client initialized successfully.")

        # อัปโหลดข้อมูล PPATENT ที่รวมแล้วเป็นไฟล์ JSON ทีละ record
        for app_no, pp_data in aggregated_ppatents.items():
            file_name = "PPATENT_" + app_no + ".json"
            json_data = json.dumps(pp_data, ensure_ascii=False, indent=4)
            folder_name = "petty_patent/"
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

        logger.info("All aggregated PPATENT records processed successfully.")

    except pyodbc.Error as e:
        logger.error(f"Error while connecting to SQL Server: {e}")
        raise
    except Exception as e:
        logger.error(f"Exception occurred during PPATENT ETL and upload: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()
            logger.info("SQL Server connection closed.")
        if 'client' in locals():
            client.close()
            logger.info("ObsClient connection closed.")

# Define the Airflow task
etl_and_upload_ppatent_task = PythonOperator(
    task_id='etl_and_upload_ppatent',
    python_callable=etl_and_upload_ppatent,
    dag=dag,
)
