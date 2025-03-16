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
    'DPATENT_direct_to_Huawei_cloud',
    default_args=default_args,
    description='ETL from SQL Server DPATENT data and upload to Huawei Cloud OBS (with aggregation)',
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

def etl_and_upload_dpatent(**kwargs):
    try:
        # เชื่อมต่อ SQL Server
        connection = pyodbc.connect(";".join([f"{key}={value}" for key, value in sql_server_config.items()]))
        logger.info("Connected to SQL Server successfully!")
        cursor = connection.cursor()

        # SQL Query สำหรับ DPATENT
        query = """
        SELECT TOP 5
            CONCAT('PPATENT_', ApplicationNumber) AS FileName,
            ApplicationNumber as DesignApplicationNumber,
            RelatedApplication,
            RelatedApplicatioDetails,
            ApplicationDate as DesignApplicationDate,
            PublicationNumber,
            PatentNumber,
            Title as DesignTitle,
            PublicationPage ,
            Claim,
            Abstract,
            CurrentStatusCode as DesignCurrentStatusCode,
            CurrentStatusDate as DesignCurrentStatusDate,
            Applicant,
            Creator as Inventor,
            Agent,
            ApplicantNationalityCode,
            InventorNationalityCode as DesignerNationalityCode,
            'https://patentpub.ipthailand.go.th/_AppPublic/PublicAttach.aspx?appno=' + ApplicationNumber AS PatentGAZ,
            'https://patentsearch.ipthailand.go.th/DIPSearch/Tools/DisplayImage.aspx?appno=' + ApplicationNumber + '&type=2' AS DesignDescription,
            'https://patentsearch.ipthailand.go.th/DIPSearch/Tools/DisplayImage.aspx?appno=' + ApplicationNumber + '&type=97' AS PatentCertification,
            PublicationDate,
            OppositionDate,
            DesignRegistrationDate,
            ApplicationRejectDate,
            DesignApplicationLeaveDate,
            LicenceDate,
            PaymentDate,
            DesignApplicationWithdrawnDate,
            PriorityDate,
            ExpiryDate,
            AGENTID as AgentID,
            ReceivingOfficeDate,
            GrantedDate
        FROM DWH.dbo.API_Exchange_PATENT2
        ORDER BY ApplicationDate DESC;
        """
        cursor.execute(query)

        # กำหนดชื่อ column ตามลำดับใน Query
        columns = [
            "FileName",
            "ApplicationNumber",
            "RelatedApplication",
            "RelatedApplicationDetails",
            "ApplicationDate",
            "PublicationNumber",
            "PatentNumber",
            "Title",
            "PublicationPage",
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
            "PublicationDate",
            "OppositionDate",
            "DesignRegistrationDate",
            "ApplicationRejectDate",
            "DesignApplicationLeaveDate",
            "LicenceDate",
            "PaymentDate",
            "DesignApplicationWithdrawnDate",
            "PriorityDate",
            "ExpiryDate",
            "AGENTID",
            "ReceivingOfficeDate",
            "GrantedDate",
            "SpecimenDetails"
        ]

        # ใช้ dictionary เพื่อรวมข้อมูล (aggregation) ตาม ApplicationNumber
        aggregated_dpatents = {}

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
                for field in ["Title", "Claim", "Abstract", "RelatedApplicationDetails"]:
                    row_dict[field] = clean_html(row_dict.get(field))
                
                app_no = row_dict.get("ApplicationNumber")
                if not app_no:
                    continue

                if app_no not in aggregated_dpatents:
                    aggregated_dpatents[app_no] = {
                        "ApplicationNumber": app_no,
                        "RelatedApplication": row_dict.get("RelatedApplication"),
                        "RelatedApplicationDetails": row_dict.get("RelatedApplicationDetails"),
                        "ApplicationDate": row_dict.get("ApplicationDate"),
                        "PublicationNumber": row_dict.get("PublicationNumber"),
                        "PatentNumber": row_dict.get("PatentNumber"),
                        "Title": row_dict.get("Title"),
                        "PublicationPage": row_dict.get("PublicationPage"),
                        "Claim": row_dict.get("Claim"),
                        "Abstract": row_dict.get("Abstract") or "ไม่ระบุ",
                        "CurrentStatusCode": row_dict.get("CurrentStatusCode"),
                        "CurrentStatusDate": row_dict.get("CurrentStatusDate"),
                        # many-to-many fields
                        "ApplicantInformation": [],
                        "InventorInformation": [],
                        "AgenInformation": [],
                        "IPCClasses": row_dict.get("IPCClasses"),
                        "PublicAttachURL": row_dict.get("PublicAttachURL"),
                        "DisplayImageURL1": row_dict.get("DisplayImageURL1"),
                        "DisplayImageURL2": row_dict.get("DisplayImageURL2"),
                        "PublicationDate": row_dict.get("PublicationDate"),
                        "OppositionDate": row_dict.get("OppositionDate"),
                        "DesignRegistrationDate": row_dict.get("DesignRegistrationDate"),
                        "ApplicationRejectDate": row_dict.get("ApplicationRejectDate"),
                        "DesignApplicationLeaveDate": row_dict.get("DesignApplicationLeaveDate"),
                        "LicenceDate": row_dict.get("LicenceDate"),
                        "PaymentDate": row_dict.get("PaymentDate"),
                        "DesignApplicationWithdrawnDate": row_dict.get("DesignApplicationWithdrawnDate"),
                        "PriorityDate": row_dict.get("PriorityDate"),
                        "ExpiryDate": row_dict.get("ExpiryDate"),
                        "AGENTID": row_dict.get("AGENTID"),
                        "ReceivingOfficeDate": row_dict.get("ReceivingOfficeDate"),
                        "GrantedDate": row_dict.get("GrantedDate"),
                        "SpecimenDetails": row_dict.get("SpecimenDetails")
                    }

                # รวมข้อมูล ApplicantInformation (Applicant + ApplicantNationalityCode)
                applicant_info = {}
                if row_dict.get("Applicant"):
                    applicant_info["Applicant"] = row_dict.get("Applicant")
                if row_dict.get("ApplicantNationalityCode"):
                    applicant_info["ApplicantNationalityCode"] = row_dict.get("ApplicantNationalityCode")
                if applicant_info and applicant_info not in aggregated_dpatents[app_no]["ApplicantInformation"]:
                    aggregated_dpatents[app_no]["ApplicantInformation"].append(applicant_info)

                # รวมข้อมูล InventorInformation (ใช้ Creator เป็น Inventor)
                inventor_info = {}
                if row_dict.get("Creator"):
                    inventor_info["Inventor"] = row_dict.get("Creator")
                if row_dict.get("InventorNationalityCode"):
                    inventor_info["InventorNationalityCode"] = row_dict.get("InventorNationalityCode")
                if inventor_info and inventor_info not in aggregated_dpatents[app_no]["InventorInformation"]:
                    aggregated_dpatents[app_no]["InventorInformation"].append(inventor_info)

                # รวมข้อมูล AgenInformation (Agent)
                agent_info = {}
                if row_dict.get("Agent"):
                    agent_info["Agent"] = row_dict.get("Agent")
                if agent_info and agent_info not in aggregated_dpatents[app_no]["AgenInformation"]:
                    aggregated_dpatents[app_no]["AgenInformation"].append(agent_info)

        # Initialize Huawei Cloud OBS Client
        client = ObsClient(
            access_key_id=huawei_cloud_config['access_key'],
            secret_access_key=huawei_cloud_config['secret_key'],
            server=huawei_cloud_config['endpoint']
        )
        logger.info("OBS Client initialized successfully.")

        # อัปโหลดข้อมูล DPATENT ที่รวมแล้วเป็นไฟล์ JSON ทีละ record
        for app_no, dpatent_data in aggregated_dpatents.items():
            file_name = "DPATENT_" + app_no + ".json"
            json_data = json.dumps(dpatent_data, ensure_ascii=False, indent=4)
            folder_name = "design_patent/"
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

        logger.info("All aggregated DPATENT records processed successfully.")

    except pyodbc.Error as e:
        logger.error(f"Error while connecting to SQL Server: {e}")
        raise
    except Exception as e:
        logger.error(f"Exception occurred during DPATENT ETL and upload: {e}")
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
etl_and_upload_dpatent_task = PythonOperator(
    task_id='etl_and_upload_dpatent',
    python_callable=etl_and_upload_dpatent,
    dag=dag,)