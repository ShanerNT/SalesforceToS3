import os
import datetime
import io
import boto3
import pandas as pd
import snowflake.connector
from simple_salesforce import Salesforce  # Install via pip if needed

# --- Environment Variables ---
SF_USERNAME       = os.environ.get("SF_USERNAME")
SF_PASSWORD       = os.environ.get("SF_PASSWORD")
SF_SECURITY_TOKEN = os.environ.get("SF_SECURITY_TOKEN")
SF_DOMAIN         = os.environ.get("SF_DOMAIN", "login")  # defaults to login.salesforce.com

SNOWFLAKE_USER      = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT   = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA")

S3_BUCKET    = os.environ.get("S3_BUCKET")
# FILE_SIZE_MB: if 0 then write one big file, else chunk data into files of roughly this many MB.
FILE_SIZE_MB = int(os.environ.get("FILE_SIZE_MB", "0"))

# --- Helper: Snowflake Connection ---
def get_snowflake_connection():
    """Create and return a Snowflake connection using environment credentials."""
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    return conn

# --- Logging Function ---
def log_event(process_name, event_type, event_detail, user_name="lambda_function"):
    """
    Writes a log record into a Snowflake logging table.
    Expected columns: ProcessName, EventType, EventDetail, EventDate, UserName.
    """
    try:
        conn = get_snowflake_connection()
        cs = conn.cursor()
        sql = """
            INSERT INTO LOGGING_TABLE (ProcessName, EventType, EventDetail, EventDate, UserName)
            VALUES (%s, %s, %s, %s, %s)
        """
        event_date = datetime.datetime.now()
        cs.execute(sql, (process_name, event_type, event_detail, event_date, user_name))
        conn.commit()
    except Exception as e:
        # If logging fails, print the error (since there’s nowhere else to log it)
        print(f"Logging error in {process_name}: {str(e)}")
    finally:
        try:
            cs.close()
            conn.close()
        except Exception:
            pass

# --- Retrieve Salesforce Objects List ---
def get_salesforce_object_list():
    """
    Connects to Snowflake to retrieve a list of Salesforce objects that should be pulled.
    Assumes there is a table (e.g., SALESFORCE_OBJECTS) with at least one column: object_name.
    """
    process_name = "get_salesforce_object_list"
    try:
        conn = get_snowflake_connection()
        cs = conn.cursor()
        cs.execute("SELECT object_name FROM SALESFORCE_OBJECTS")
        results = cs.fetchall()
        object_list = [row[0] for row in results]
        log_event(process_name, "insert", f"Retrieved objects: {object_list}")
        return object_list
    except Exception as e:
        log_event(process_name, "error", f"Error retrieving object list: {str(e)}")
        raise
    finally:
        try:
            cs.close()
            conn.close()
        except Exception:
            pass

# --- Pull Salesforce Data ---
def pull_salesforce_data(object_name):
    """
    Connects to Salesforce and pulls all records for the given object.
    Uses simple_salesforce's query_all function.
    """
    process_name = "pull_salesforce_data"
    try:
        sf = Salesforce(username=SF_USERNAME,
                        password=SF_PASSWORD,
                        security_token=SF_SECURITY_TOKEN,
                        domain=SF_DOMAIN)
        query = f"SELECT * FROM {object_name}"
        result = sf.query_all(query)
        records = result.get('records', [])
        # Remove the 'attributes' key from each record (if present)
        for rec in records:
            rec.pop('attributes', None)
        df = pd.DataFrame(records)
        log_event(process_name, "insert", f"Pulled {len(df)} records from {object_name}")
        return df
    except Exception as e:
        log_event(process_name, "error", f"Error pulling data for {object_name}: {str(e)}")
        raise

# --- Save Data to S3 as Parquet ---
def save_data_to_s3(df, object_name):
    """
    Converts the DataFrame to Parquet and uploads it to S3.
    If FILE_SIZE_MB is greater than zero, the DataFrame is split into chunks (naively estimated by row count).
    The filename follows the format: object_MMDDYY_HHMMSS or object_MMDDYY_HHMMSS_partX.parquet.
    """
    process_name = "save_data_to_s3"
    try:
        s3_client = boto3.client('s3')
        timestamp = datetime.datetime.now().strftime("%m%d%y_%H%M%S")
        base_filename = f"{object_name}_{timestamp}"

        if FILE_SIZE_MB == 0:
            # Write one big file
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            s3_key = f"{base_filename}.parquet"
            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
            log_event(process_name, "insert", f"Saved {s3_key} to S3")
        else:
            # Naively split the DataFrame into chunks.
            # Note: Determining the exact file size in MB before writing is non-trivial.
            # Here we assume ~1KB per row (adjust this logic as needed).
            rows_per_file = FILE_SIZE_MB * 1024
            total_rows = len(df)
            num_chunks = (total_rows // rows_per_file) + (1 if total_rows % rows_per_file else 0)
            for i in range(num_chunks):
                chunk_df = df.iloc[i * rows_per_file: (i + 1) * rows_per_file]
                buffer = io.BytesIO()
                chunk_df.to_parquet(buffer, index=False)
                buffer.seek(0)
                s3_key = f"{base_filename}_part{i+1}.parquet"
                s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
                log_event(process_name, "insert", f"Saved {s3_key} to S3")
        return True
    except Exception as e:
        log_event(process_name, "error", f"Error saving data for {object_name}: {str(e)}")
        raise

# --- Lambda Handler ---
def lambda_handler(event, context):
    """
    Main entry point for the Lambda function.
    Retrieves the list of Salesforce objects, pulls their data, and saves each to S3.
    All steps are logged, and errors are caught so that processing can continue for remaining objects.
    """
    process_name = "lambda_handler"
    try:
        # Retrieve the list of Salesforce objects from Snowflake
        object_list = get_salesforce_object_list()
        for object_name in object_list:
            try:
                df = pull_salesforce_data(object_name)
                save_data_to_s3(df, object_name)
            except Exception as e:
                log_event(process_name, "error", f"Error processing {object_name}: {str(e)}")
                continue  # Process next object even if one fails
        return {
            'statusCode': 200,
            'body': 'Salesforce data successfully pulled and stored in S3.'
        }
    except Exception as e:
        log_event(process_name, "error", f"General error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }

# --- For Local Testing ---
if __name__ == "__main__":
    lambda_handler({}, None)

'''
Explanation
Environment Variables:
All credentials (Salesforce, Snowflake) and configuration (S3 bucket name, file size limit) are read from environment variables. Adjust these names as needed for your deployment.

Snowflake Connection & Logging:
The helper function get_snowflake_connection() centralizes connection creation. The log_event() function writes log records to a Snowflake table named LOGGING_TABLE (modify as necessary).

Salesforce Objects & Data:
The function get_salesforce_object_list() queries Snowflake to retrieve the names of Salesforce objects to pull. Then, for each object, pull_salesforce_data() connects to Salesforce (using simple_salesforce) and runs a SELECT * query.

Saving to S3:
save_data_to_s3() converts the pulled data (assumed to be in a Pandas DataFrame) into Parquet format. If the environment variable FILE_SIZE_MB is set to a nonzero value, the data is split into chunks (here, a naive estimation based on rows is used) so that each file is roughly within the desired size.

Error Handling & Logging:
Each function is wrapped in try/except blocks. Any errors are logged to Snowflake via the log_event() function and then re-raised or handled so that the Lambda function can continue processing other objects.

This script is designed to be modular and clear so you can adjust it further to suit your exact requirements and operational environment.
'''
