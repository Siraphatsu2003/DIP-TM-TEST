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
    'PATENT_direct_to_Huawei_cloud',
    default_args=default_args,
    description='ETL from SQL Server PATENT data and upload to Huawei Cloud OBS (with aggregation)',
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

def etl_and_upload_patent(**kwargs):
    try:
        # เชื่อมต่อกับ SQL Server
        connection = pyodbc.connect(";".join([f"{key}={value}" for key, value in sql_server_config.items()]))
        logger.info("Connected to SQL Server successfully!")
        cursor = connection.cursor()

        # SQL Query ดึงข้อมูล PATENT (Query นี้อาจมี field เพิ่มเติมในอนาคต เช่น Role)
        query = """
        SELECT TOP 5
            CONCAT('PATENT_', ApplicationNumber) AS FileName,
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
            Claim,
            Abstract,
            CurrentStatusCode,
            CurrentStatusDate,
            Applicant,
            Creator,
            Agent,
            IPCClasses,
            'https://patentpub.ipthailand.go.th/_AppPublic/PublicAttach.aspx?appno=' + ApplicationNumber AS PatentGAZ,
            'https://patentsearch.ipthailand.go.th/DIPSearch/Tools/DisplayImage.aspx?appno=' + ApplicationNumber + '&type=2' AS Description,
            'https://patentsearch.ipthailand.go.th/DIPSearch/Tools/DisplayImage.aspx?appno=' + ApplicationNumber + '&type=97' AS PatentCertification,
            'https://patentsearch.ipthailand.go.th/DIPSearch/Tools/DisplayImage.aspx?appno=' + ApplicationNumber + '&type=41' AS PatentDocument,
            PublicationDate,
            RegistrationDate,
            ExpiryDate,
            ReceivingOfficeDate,
            PCTApplicationDate,
            AGENTID as AgentID,
            LicenceDate,
            TAGS_NAME as Tag,
            ApplicantNationalityCode,
            InventorNationalityCode,
            OppositionDate,
            CheckDate,
            ApplicationRejectDate,
            ApplicationLeaveDate,
            GrantedDate,
            PaymentDate,
            ApplicationWithdrawnDate,
            PriorityDate,
            'https://patentpub.ipthailand.go.th/_AppPublic/public_doc.aspx?appno=' + ApplicationNumber AS RequestDocument
        FROM DWH.dbo.API_Exchange_PATENT1
        ORDER BY ApplicationDate DESC;
        """
        cursor.execute(query)

        # กำหนดชื่อ column ให้ตรงกับลำดับใน Query
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
            "Claim",
            "Abstract",
            "CurrentStatusCode",
            "CurrentStatusDate",
            "Applicant",
            "Creator",
            "Agent",
            "IPCClasses",
            "PublicAttachURL",
            "DisplayImageURL1",
            "DisplayImageURL2",
            "DisplayImageURL3",
            "PublicationDate",
            "RegistrationDate",
            "ExpiryDate",
            "ReceivingOfficeDate",
            "PCTApplicationDate",
            "AGENTID",
            "LicenceDate",
            "TAGS_NAME",
            "ApplicantNationalityCode",
            "InventorNationalityCode",
            "OppositionDate",
            "CheckDate",
            "ApplicationRejectDate",
            "ApplicationLeaveDate",
            "GrantedDate",
            "PaymentDate",
            "PriorityDate",
            "PublicDocURL"
        ]

        # ใช้ dictionary เพื่อรวม (aggregate) ข้อมูลโดยใช้ ApplicationNumber เป็น key
        aggregated_patents = {}

        batch_size = 10000
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                logger.info("No more rows to process.")
                break

            for row in rows:
                row_dict = dict(zip(columns, row))
                
                # แปลงข้อมูลวันที่เป็น string
                for key, value in row_dict.items():
                    if isinstance(value, (datetime, date)):
                        row_dict[key] = value.strftime('%Y-%m-%d')
                
                # ทำความสะอาดข้อมูลใน field ที่อาจมี HTML หรือ special characters
                for field in ["Title", "Claim", "Abstract", "RelatedApplicatioDetails"]:
                    row_dict[field] = clean_html(row_dict.get(field))
                
                app_no = row_dict.get("ApplicationNumber")
                if not app_no:
                    continue  # ข้ามหากไม่มี ApplicationNumber

                # ถ้ายังไม่มีข้อมูลสำหรับ ApplicationNumber นี้ ให้อนุมัติ record ใหม่
                if app_no not in aggregated_patents:
                    aggregated_patents[app_no] = {
                        "ApplicationNumber": app_no,
                        "RelatedApplication": row_dict.get("RelatedApplication"),
                        "RelatedApplicatioDetails": row_dict.get("RelatedApplicatioDetails"),
                        "ApplicationDate": row_dict.get("ApplicationDate"),
                        "PublicationNumber": row_dict.get("PublicationNumber"),
                        "PatentNumberr": row_dict.get("PatentNumber"),
                        "Title": row_dict.get("Title"),
                        "PublicationPage": row_dict.get("PublicationPage"),
                        "Claim": row_dict.get("Claim"),
                        "Abstract": row_dict.get("Abstract") or "ไม่ระบุ",
                        "CurrentStatusCode": row_dict.get("CurrentStatusCode"),
                        "CurrentStatusDate": row_dict.get("CurrentStatusDate"),
                        # ข้อมูล many-to-many
                        "ApplicantInformation": [],
                        "InventorInformation": [],
                        "AgenInformation": [],
                        "PatentGAZ": row_dict.get("PublicAttachURL"),
                        "Description": row_dict.get("DisplayImageURL1"),
                        "PatentAbstract": row_dict.get("Abstract") or "ไม่ระบุ",
                        "PatentCertification": row_dict.get("DisplayImageURL2"),
                        "PatentDocument": row_dict.get("DisplayImageURL3"),
                        "PublicationDate": row_dict.get("PublicationDate"),
                        "RegistrationDate": row_dict.get("RegistrationDate"),
                        "ExpiryDate": row_dict.get("ExpiryDate"),
                        "ReceivingOfficeDate": row_dict.get("ReceivingOfficeDate"),
                        "PCTApplicationDate": row_dict.get("PCTApplicationDate"),
                        "AgentID": row_dict.get("AGENTID"),
                        "LicenceDate": row_dict.get("LicenceDate")
                    }
                    # หากมี field พิเศษอย่าง "Role" (จาก source อื่น) ให้เก็บด้วย
                    if "Role" in row_dict:
                        aggregated_patents[app_no]["Role"] = row_dict["Role"]

                # รวมข้อมูล ApplicantInformation (เก็บทั้งชื่อและรหัสสัญชาติ)
                applicant_info = {}
                if row_dict.get("Applicant"):
                    applicant_info["Applicant"] = row_dict.get("Applicant")
                if row_dict.get("ApplicantNationalityCode"):
                    applicant_info["ApplicantNationalityCode"] = row_dict.get("ApplicantNationalityCode")
                if applicant_info and applicant_info not in aggregated_patents[app_no]["ApplicantInformation"]:
                    aggregated_patents[app_no]["ApplicantInformation"].append(applicant_info)

                # รวมข้อมูล InventorInformation (ใช้ Creator เป็น Inventor)
                inventor_info = {}
                if row_dict.get("Creator"):
                    inventor_info["Inventor"] = row_dict.get("Creator")
                if row_dict.get("InventorNationalityCode"):
                    inventor_info["InventorNationalityCode"] = row_dict.get("InventorNationalityCode")
                if inventor_info and inventor_info not in aggregated_patents[app_no]["InventorInformation"]:
                    aggregated_patents[app_no]["InventorInformation"].append(inventor_info)

                # รวมข้อมูล AgenInformation (ใช้ Agent)
                agent_info = {}
                if row_dict.get("Agent"):
                    agent_info["Agent"] = row_dict.get("Agent")
                if agent_info and agent_info not in aggregated_patents[app_no]["AgenInformation"]:
                    aggregated_patents[app_no]["AgenInformation"].append(agent_info)

        # Initialize Huawei Cloud OBS Client
        client = ObsClient(
            access_key_id=huawei_cloud_config['access_key'],
            secret_access_key=huawei_cloud_config['secret_key'],
            server=huawei_cloud_config['endpoint']
        )
        logger.info("OBS Client initialized successfully.")

        # อัปโหลดไฟล์ JSON สำหรับแต่ละ PATENT (จาก aggregated_patents)
        for app_no, patent_data in aggregated_patents.items():
            file_name = "PATENT_" + app_no + ".json"
            json_data = json.dumps(patent_data, ensure_ascii=False, indent=4)
            folder_name = "patent/"
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

        logger.info("All aggregated patents processed successfully.")

    except pyodbc.Error as e:
        logger.error(f"Error while connecting to SQL Server: {e}")
        raise
    except Exception as e:
        logger.error(f"Exception occurred during PATENT ETL and upload: {e}")
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
etl_and_upload_patent_task = PythonOperator(
    task_id='etl_and_upload_patent',
    python_callable=etl_and_upload_patent,
    dag=dag,
)