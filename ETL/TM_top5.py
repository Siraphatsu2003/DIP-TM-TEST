import os
import pyodbc
import json
import base64
import logging
from datetime import datetime, date
from tqdm import tqdm
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
# Use a more flexible path approach
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)  # Go up one level from the script directory
env_path = os.path.join(project_root, '.env')

# Attempt to load environment variables
if os.path.exists(env_path):
    logger.info(f"Loading environment variables from: {env_path}")
    load_dotenv(dotenv_path=env_path)
else:
    logger.warning(f".env file not found at {env_path}, trying current directory")
    load_dotenv()  # Try to load from current directory

# SQL Server Connection Details
sql_server_config = {
    "DRIVER": os.environ.get("SQL_SERVER_DRIVER", "SQL Server"),  # Default to 'SQL Server' driver
    "SERVER": os.environ.get("SQL_SERVER_SERVER"),
    "UID": os.environ.get("SQL_SERVER_UID"),
    "PWD": os.environ.get("SQL_SERVER_PWD"),
    "TrustServerCertificate": os.environ.get("SQL_SERVER_TRUST", "Yes")
}

def get_image(physical_path):
    """
    Convert an image file to base64-encoded string.
    
    Args:
        physical_path (str): Path to the image file, may include '/image/' prefix
        
    Returns:
        str: Base64-encoded string of the image or None if file doesn't exist
    """
    if not physical_path:
        logger.debug("No physical path provided")
        return None
    
    try:
        # Remove '/image/' prefix if present
        if physical_path.startswith('/image/'):
            physical_path = physical_path[7:]  # Remove the '/image/' prefix
        
        # Check if the file exists and encode it
        if os.path.isfile(physical_path):
            with open(physical_path, "rb") as image_file:
                base64_encoded = base64.b64encode(image_file.read()).decode('utf-8')
                logger.debug(f"Successfully encoded image: {physical_path}")
                return base64_encoded
        else:
            logger.warning(f"Invalid or missing file path: {physical_path}")
            return None
    except Exception as e:
        logger.error(f"Error processing file at {physical_path}: {e}")
        return None

def etl_process(limit=5):
    """
    Extract, Transform, Load process for trademark data.
    
    Args:
        limit (int): Number of distinct trademarks to process
    
    Returns:
        dict: Processed data in a structured format
    """
    try:
        # STEP 1: Connect to SQL Server
        logger.info("Step 1: Connecting to SQL Server...")
        connection_string = ";".join([f"{key}={value}" for key, value in sql_server_config.items()])
        connection = pyodbc.connect(connection_string)
        logger.info("Connected to SQL Server successfully!")
        
        # STEP 2: First get the top X distinct trademark IDs
        logger.info(f"Step 2: Fetching top {limit} distinct trademark IDs...")
        cursor = connection.cursor()
        
        # Query to get distinct trademark IDs
        query_ids = f"""
        SELECT DISTINCT TOP {limit} id
        FROM TM_STG_BIBLIO
        ORDER BY id DESC
        """
        cursor.execute(query_ids)
        
        # Get the IDs as a list
        ids = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(ids)} distinct trademark IDs: {ids}")
        
        # Convert IDs to a comma-separated string for the next query
        ids_str = ",".join(str(id) for id in ids)
        
        # STEP 3: Get detailed data for these specific trademark IDs
        logger.info("Step 3: Fetching detailed data for the selected trademarks...")
        
        # Query to get all related data for the selected IDs
        query = f"""
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
            TM_STG_AGENT.updated_date AS updated_date
        FROM TM_STG_BIBLIO
        LEFT JOIN TM_STG_OWNER ON TM_STG_BIBLIO.id = TM_STG_OWNER.save_id
        LEFT JOIN TM_STG_GOODS ON TM_STG_BIBLIO.id = TM_STG_GOODS.save_id
        LEFT JOIN TM_STG_AGENT ON TM_STG_BIBLIO.id = TM_STG_AGENT.save_id
        LEFT JOIN TM_STG_File ON TM_STG_BIBLIO.id = TM_STG_File.save_id
        WHERE TM_STG_BIBLIO.id IN ({ids_str})
        ORDER BY TM_STG_BIBLIO.id DESC
        """
        
        cursor.execute(query)
        
        # Define column names for the result set
        columns = ["FileName", "BiblioID", "ApplicationNumber", "Expiration_Date", "Filing_Date",
                "IRN_Number", "No_Permit", "Publication_Date", "Registration_Date",
                "Registration_Number", "Status", "Trademark_Type", "OWNER_NAME",
                "Nationallity_Code", "Class", "GOODS", "Agent_Name", "physical_path", "Updated_date"]
        
        # Fetch and convert to dictionaries
        rows = cursor.fetchall()
        logger.info(f"Step 3 Results: Fetched {len(rows)} detailed rows for {len(ids)} trademarks")
        rows_dict = [dict(zip(columns, row)) for row in rows]
        
        # Sample data from the first row to verify data retrieval
        if rows_dict:
            logger.info(f"Sample data - First row: {list(rows_dict[0].items())[:5]}")
        
        cursor.close()
        
        # STEP 4: Transform data into nested structure
        logger.info("Step 4: Transforming data...")
        trademarks = {}
        
        for row in tqdm(rows_dict, desc="Processing records", unit="record"):
            # Extract key fields
            tm_file = row.get("FileName")
            class_id = row.get("Class")
            goods = row.get("GOODS")
            owner_name = row.get("OWNER_NAME")
            nationality = row.get("Nationallity_Code")
            agent_name = row.get("Agent_Name")
            physical_path = row.get("physical_path")
            app_number = row.get("ApplicationNumber")
            biblio_id = row.get("BiblioID")
            status = row.get("Status")
            trademark_type = row.get("Trademark_Type")
            reg_number = row.get("Registration_Number")
            no_permit = row.get("No_Permit")
            irn_number = row.get("IRN_Number")
            
            # Convert date fields to strings
            update_date = row.get("Updated_date")
            if isinstance(update_date, (datetime, date)):
                update_date = update_date.strftime('%Y-%m-%d')
                
            filing_date = row.get("Filing_Date")
            if isinstance(filing_date, (datetime, date)):
                filing_date = filing_date.strftime('%Y-%m-%d')
            
            pub_date = row.get("Publication_Date")
            if isinstance(pub_date, (datetime, date)):
                pub_date = pub_date.strftime('%Y-%m-%d')
            
            reg_date = row.get("Registration_Date")
            if isinstance(reg_date, (datetime, date)):
                reg_date = reg_date.strftime('%Y-%m-%d')
            
            exp_date = row.get("Expiration_Date")
            if isinstance(exp_date, (datetime, date)):
                exp_date = exp_date.strftime('%Y-%m-%d')
            
            # If this trademark already exists, update it
            if tm_file in trademarks:
                # Update ClassList - find existing class or add new one
                class_found = False
                for i, item in enumerate(trademarks[tm_file]["ClassList"]):
                    if class_id == item["Class"]:
                        if {"GOODS": goods} not in trademarks[tm_file]["ClassList"][i]["GOODSList"]:
                            trademarks[tm_file]["ClassList"][i]["GOODSList"].append({"GOODS": goods})
                        class_found = True
                        break
                
                if not class_found:
                    trademarks[tm_file]["ClassList"].append(
                        {
                            "Class": class_id,
                            "GOODSList": [{"GOODS": goods}]
                        }
                    )
                
                # Update Owner list
                owner = {"OWNER_NAME": owner_name, "Nationallity": nationality}
                if owner not in trademarks[tm_file]["OWNER_NAMEList"]:
                    trademarks[tm_file]["OWNER_NAMEList"].append(owner)
                
                # Update Agent list
                agent = {"AGENT_NAME": agent_name}
                if agent not in trademarks[tm_file]["AGENT_NAMEList"]:
                    trademarks[tm_file]["AGENT_NAMEList"].append(agent)
                
                # Update ApplicationNumber
                trademarks[tm_file]["ApplicationNumber"] = app_number
                
                # Update BiblioID list
                if biblio_id not in trademarks[tm_file]["Biblio_ID"]:
                    trademarks[tm_file]["Biblio_ID"].append(biblio_id)
                
                # Update other lists if values are not already there
                for field, value in [
                    ("Status", status),
                    ("No_Permit", no_permit),
                    ("IRN_Number", irn_number),
                    ("Trademark_Type", trademark_type),
                    ("Registration_Number", reg_number),
                    ("Update_date", update_date),
                    ("Filing_Date", filing_date),
                    ("Publication_Date", pub_date),
                    ("Registration_Date", reg_date),
                    ("Expiration_Date", exp_date)
                ]:
                    if value not in trademarks[tm_file][field]:
                        trademarks[tm_file][field].append(value)
            
            # If this is a new trademark, create a new entry
            else:
                trademarks[tm_file] = {
                    "ClassList": [
                        {
                            "Class": class_id,
                            "GOODSList": [{"GOODS": goods}]
                        }
                    ],
                    "OWNER_NAMEList": [
                        {
                            "OWNER_NAME": owner_name,
                            "Nationallity": nationality
                        }
                    ],
                    "AGENT_NAMEList": [
                        {
                            "AGENT_NAME": agent_name
                        }
                    ],
                    "Physical_Path": physical_path,
                    "ApplicationNumber": app_number,
                    "Biblio_ID": [biblio_id],
                    "Status": [status],
                    "No_Permit": [no_permit],
                    "IRN_Number": [irn_number],
                    "Trademark_Type": [trademark_type],
                    "Registration_Number": [reg_number],
                    "Update_date": [update_date],
                    "Filing_Date": [filing_date],
                    "Publication_Date": [pub_date],
                    "Registration_Date": [reg_date],
                    "Expiration_Date": [exp_date]
                }
        
        # STEP 5: Retrieve and add image data
        logger.info("Step 5: Retrieving image data...")
        for tm_key, tm_data in tqdm(trademarks.items(), desc="Processing images"):
            physical_path = tm_data['Physical_Path']
            logger.info(f"Processing image from path: {physical_path}")
            
            # Define base directory where images are stored
            # Modify this path to match your actual image storage location
            base_dir = r'C:\path\to\images'  # Replace with your actual path to the image directory
            
            # Remove '/image/' prefix if present
            if physical_path.startswith('/image/'):
                image_path = physical_path[7:]  # Remove the '/image/' prefix
            else:
                image_path = physical_path
            
            # Convert from Unix-style to Windows path if needed
            image_path = image_path.replace('/', '\\')
            
            # Combine with base directory
            full_path = os.path.join(base_dir, image_path)
            logger.info(f"Full image path: {full_path}")
            
            # Check if file exists before attempting to read
            if os.path.isfile(full_path):
                try:
                    with open(full_path, "rb") as image_file:
                        base64_encoded = base64.b64encode(image_file.read()).decode('utf-8')
                        tm_data['TM_image'] = base64_encoded
                        logger.info(f"Successfully encoded image for {tm_key}")
                except Exception as e:
                    logger.error(f"Error reading image file for {tm_key}: {e}")
                    tm_data['TM_image'] = None
            else:
                logger.warning(f"Image file not found: {full_path}")
                tm_data['TM_image'] = None
        
        # STEP 6: Show transformation results
        logger.info(f"Step 6: Transformation complete - {len(trademarks)} trademarks processed")
        
        # Verify we have exactly the right number of trademarks
        if len(trademarks) == limit:
            logger.info(f"Successfully processed exactly {limit} distinct trademarks")
        else:
            logger.warning(f"Expected {limit} trademarks but processed {len(trademarks)}")
            
        for tm_key in trademarks.keys():
            logger.info(f"Processed trademark: {tm_key}")
            
            # Count classes and goods for each trademark
            classes_count = len(trademarks[tm_key]['ClassList'])
            goods_count = sum(len(c['GOODSList']) for c in trademarks[tm_key]['ClassList'])
            logger.info(f" - Contains {classes_count} classes and {goods_count} goods items")
            
            # Print first class and goods as sample
            if classes_count > 0:
                first_class = trademarks[tm_key]['ClassList'][0]
                logger.info(f" - Sample: Class {first_class['Class']} with {len(first_class['GOODSList'])} goods")
                if first_class['GOODSList']:
                    logger.info(f" - First good: {first_class['GOODSList'][0]['GOODS'][:50]}...")
        
        # STEP 7: Save results to file
        logger.info("Step 7: Saving results to file...")
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        
        # Save all data to a single file
        with open(os.path.join(output_dir, 'top_trademarks.json'), 'w', encoding='utf-8') as f:
            json.dump(trademarks, f, ensure_ascii=False, indent=2)
        
        # Save each trademark to its own file
        for tm_key, tm_data in trademarks.items():
            with open(os.path.join(output_dir, f"{tm_key}.json"), 'w', encoding='utf-8') as f:
                json.dump(tm_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved {len(trademarks)} files to {output_dir}/")
        
        # Print some statistics about the processed data
        logger.info("ETL process completed successfully!")
        logger.info(f"Total trademarks processed: {len(trademarks)}")
        logger.info(f"Total classes: {sum(len(tm['ClassList']) for tm in trademarks.values())}")
        logger.info(f"Total owners: {sum(len(tm['OWNER_NAMEList']) for tm in trademarks.values())}")
        
        return trademarks
    
    except pyodbc.Error as e:
        logger.error(f"Error while connecting to SQL Server: {e}")
        raise
    except Exception as e:
        logger.error(f"Exception occurred during ETL process: {e}")
        raise
    finally:
        if 'connection' in locals() and connection:
            connection.close()
            logger.info("SQL Server connection closed.")

if __name__ == "__main__":
    # Process exactly 5 distinct trademark records
    results = etl_process(limit=5)
    
    # Print sample output
    print("\nSample of processed data:")
    if results:
        sample_key = next(iter(results))
        pretty_json = json.dumps(results[sample_key], indent=2, ensure_ascii=False)
        
        # Print first 500 characters of the sample data
        print(f"Trademark: {sample_key}")
        print(pretty_json[:500] + "..." if len(pretty_json) > 500 else pretty_json)
        
        print(f"\nTotal trademarks processed: {len(results)}")