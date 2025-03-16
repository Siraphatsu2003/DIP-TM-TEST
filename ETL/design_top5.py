import pyodbc
import os
from dotenv import dotenv_values

# ระบุ path ไปยังไฟล์ .env ของคุณ
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir) 
env_path = os.path.join(project_root, '.env')

# โหลด environment variables จากไฟล์ .env โดยใช้ dotenv_values (จะได้ค่าในรูปแบบ dictionary)
config = dotenv_values(env_path)

# โหลดการตั้งค่าการเชื่อมต่อ SQL Server จาก dictionary ที่ได้
sql_server_config = {
    "DRIVER": config.get("SQL_SERVER_DRIVER"),
    "SERVER": config.get("SQL_SERVER_SERVER"),
    "UID": config.get("SQL_SERVER_UID"),
    "PWD": config.get("SQL_SERVER_PWD"),
    "TrustServerCertificate": config.get("SQL_SERVER_TRUST")
}

# สร้าง connection string สำหรับการเชื่อมต่อ SQL Server
connection_string = (
    f"DRIVER={{{sql_server_config['DRIVER']}}};"
    f"SERVER={sql_server_config['SERVER']};"
    f"UID={sql_server_config['UID']};"
    f"PWD={sql_server_config['PWD']};"
    f"TrustServerCertificate={sql_server_config['TrustServerCertificate']};"
)

# สร้างการเชื่อมต่อกับฐานข้อมูล
try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    print("เชื่อมต่อ SQL Server สำเร็จ")
except Exception as e:
    print("ไม่สามารถเชื่อมต่อ SQL Server ได้:", e)
    exit(1)

# SQL Query สำหรับดึงข้อมูล (Extract)
query = """
SELECT TOP 5 
    ApplicationNumber as DesignApplicationNumber,
    RelatedApplication,
    RelatedApplicatioDetails,
    ApplicationDate as DesignApplicationDate,
    PublicationNumber,
    PatentNumber,
    Title as DesignTitle,
    PublicationPage,
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
    IDCClasses,
    AGENTID as AgentID,
    ReceivingOfficeDate,
    GrantedDate
FROM DWH.dbo.API_Exchange_PATENT2
ORDER BY ApplicationDate DESC;
"""

# ดำเนินการ query (Extract)
try:
    cursor.execute(query)
    row = cursor.fetchone()
except Exception as e:
    print("เกิดข้อผิดพลาดระหว่างการ query:", e)
    cursor.close()
    conn.close()
    exit(1)

# แปลงผลลัพธ์ (Transform) ให้เป็น dictionary (ถ้ามีข้อมูล)
if row:
    columns = [column[0] for column in cursor.description]
    result = dict(zip(columns, row))
else:
    result = None

# แสดงผลลัพธ์ (Load)
if result:
    print("ผลลัพธ์ที่ได้:")
    for key, value in result.items():
        print(f"{key}: {value}")
else:
    print("ไม่พบข้อมูลที่ต้องการ")

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()