import io
import os
import json
import zipfile
import pandas as pd
import boto3
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
load_dotenv()

SERVER_IP = os.getenv("SERVER_IP")
MINIO_PORT_API = os.getenv("MINIO_PORT_API")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
INFLUXDB_PORT = os.getenv("INFLUXDB_PORT")
INFLUXDB_ROOT_TOKEN = os.getenv("INFLUXDB_ROOT_TOKEN")

HISTORY_FILE = "processed_zips.log"
MINIO_BUCKET = "class-data"

# --- CONFIGURATION ---
MINIO_CONFIG = {
    "endpoint": f"http://{SERVER_IP}:{MINIO_PORT_API}",
    "access_key": f"{MINIO_ROOT_USER}",
    "secret_key": f"{MINIO_ROOT_PASSWORD}",
    "bucket": "class-data"
}

INFLUX_CONFIG = {
    "url": f"http://{SERVER_IP}:{INFLUXDB_PORT}",
    "token": f"{INFLUXDB_ROOT_TOKEN}",
    "org": "my-class",
    "bucket": "sensor-data"
}

s3 = boto3.client('s3', 
    endpoint_url=MINIO_CONFIG["endpoint"],
    aws_access_key_id=MINIO_CONFIG["access_key"],
    aws_secret_access_key=MINIO_CONFIG["secret_key"]
)

client = InfluxDBClient(url=INFLUX_CONFIG["url"], token=INFLUX_CONFIG["token"])
# Use Batching instead of SYNCHRONOUS to ensure no data points are missed
write_api = client.write_api(write_options=WriteOptions(
    batch_size=5000,      # Send in chunks of 5000 rows
    flush_interval=1000,  # Or every 1 second
    retry_interval=5000,  # If it fails, wait 5s to retry
    max_retries=3
))

def get_processed_files():
    if not os.path.exists(HISTORY_FILE):
        return set()
    with open(HISTORY_FILE, "r") as f:
        return set(line.strip() for line in f)

def mark_as_processed(filename):
    with open(HISTORY_FILE, "a") as f:
        f.write(f"{filename}\n")

def get_participant_info(metadata_json):
    # Hardcoded defaults as a safety net
    info = {"name": "Abdurrahman Ibrahim", "roll_number": "25100204"}
    
    if isinstance(metadata_json, list):
        for entry in metadata_json:
            # .strip() removes hidden spaces like "Name " -> "Name"
            title = str(entry.get("title", "")).strip()
            val = entry.get("value")
            
            if title == "Name" and val:
                info["name"] = str(val).strip()
            elif title == "Roll number" and val:
                info["roll_number"] = str(val).strip()
    return info

def ingest_data():
    processed_files = get_processed_files()
    response = s3.list_objects_v2(Bucket=MINIO_BUCKET)
    
    for obj in response.get('Contents', []):
        file_key = obj['Key']
        if file_key in processed_files or not file_key.endswith('.zip'):
            continue
            
        print(f"Ingesting ALL points from: {file_key}")
        zip_data = s3.get_object(Bucket=MINIO_BUCKET, Key=file_key)
        
        try:
            with zipfile.ZipFile(io.BytesIO(zip_data['Body'].read())) as z:
                with z.open('StudyMetadata.json') as f:
                    participant = get_participant_info(json.load(f))

                for filename in z.namelist():
                    if filename.endswith('.csv') and "Metadata" not in filename:
                        if z.getinfo(filename).file_size == 0: continue
                        
                        measurement = filename.replace('.csv', '')
                        with z.open(filename) as f:
                            try:
                                # Low memory mode helps with large CSVs on 16GB RAM
                                df = pd.read_csv(f, low_memory=False)
                                if df.empty: continue

                                # Data Prep
                                df['time'] = pd.to_datetime(df['time'], unit='ns')
                                df['name'] = participant['name']
                                df['roll_number'] = participant['roll_number']
                                df['file_key'] = file_key
                                
                                # WRITE ALL DATA
                                write_api.write(
                                    bucket=INFLUX_CONFIG["bucket"],
                                    org=INFLUX_CONFIG["org"],
                                    record=df,
                                    data_frame_measurement_name=measurement,
                                    data_frame_tag_columns=['name', 'roll_number', 'file_key'],
                                    data_frame_timestamp_column='time'
                                )
                            except Exception as e:
                                print(f"  Error in {filename}: {e}")
                                continue
                
                # IMPORTANT: Wait for the buffer to clear before marking as processed
                write_api.flush() 
                mark_as_processed(file_key)
                print(f"Successfully ingested {file_key} for {participant['name']}")

        except Exception as e:
            print(f"Critical error processing {file_key}: {e}")

if __name__ == "__main__":
    try:
        ingest_data()
    finally:
        print("Shutting down... ensuring all data points are sent.")
        # 1. Force the write_api to send anything remaining in the buffer
        write_api.flush()
        
        # 2. Close the write_api to stop the background threads gracefully
        write_api.close()
        
        # 3. Close the main client connection
        client.close()
        print("Shutdown complete. All batches processed.")