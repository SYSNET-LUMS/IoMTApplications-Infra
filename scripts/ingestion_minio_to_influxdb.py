import io
import os
import json
import zipfile
import argparse

import boto3
import pandas as pd
import dateutil.parser
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, WriteOptions

load_dotenv()

# --- CONFIG ---
START_FILTER_DATE = pd.to_datetime("2026-02-24").tz_localize("UTC")
MAPPING_FILE = "mappings.json"
LOG_FILE = "processed_zips.log"
BUCKET_NAME = "class-data"

s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{os.getenv('SERVER_IP')}:{os.getenv('MINIO_PORT_API')}",
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
)

client = InfluxDBClient(
    url=f"http://{os.getenv('SERVER_IP')}:{os.getenv('INFLUXDB_PORT')}",
    token=os.getenv("INFLUXDB_ROOT_TOKEN"),
    org="my-class",
)


def error_callback(conf, data, exception):
    print(f"FAILED TO WRITE BATCH: {exception}")


write_api = client.write_api(
    write_options=WriteOptions(
        batch_size=2000,
        flush_interval=1000,
        retry_interval=5000,
    ),
    success_callback=lambda c, d: print("Batch written!"),
    error_callback=error_callback,
)


def get_processed_files():
    if not os.path.exists(LOG_FILE):
        return set()
    with open(LOG_FILE, "r") as f:
        return set(line.strip() for line in f)


def mark_as_processed(file_key):
    with open(LOG_FILE, "a") as f:
        f.write(f"{file_key}\n")


def load_mappings():
    if os.path.exists(MAPPING_FILE):
        with open(MAPPING_FILE, "r") as f:
            return json.load(f)
    return {}


def save_mappings(mappings):
    with open(MAPPING_FILE, "w") as f:
        json.dump(mappings, f, indent=4)


def parse_zip_name(filename):
    parts = filename.replace(".zip", "").split("-")
    device_id = parts[-1]
    return device_id.replace("-", "")


def extract_email_from_study_metadata(zip_file):
    email = "UNKNOWN"

    if "StudyMetadata.json" not in zip_file.namelist():
        return email

    with zip_file.open("StudyMetadata.json") as f:
        meta = json.load(f)

    for entry in meta:
        title = str(entry.get("title", "")).strip().lower()
        if title == "email":
            email = str(entry.get("value", "")).strip()
            break

    return email.lower()


def update_mapping(device_id, email):
    email = email.lower()
    mappings = load_mappings()
    if mappings.get(device_id) != email:
        mappings[device_id] = email
        save_mappings(mappings)


def write_sensor_csvs_from_zip(zip_file, device_id, email):
    for csv_file in zip_file.namelist():
        if not csv_file.endswith(".csv") or "Metadata" in csv_file:
            continue

        measurement = csv_file.replace(".csv", "")

        with zip_file.open(csv_file) as f:
            try:
                df = pd.read_csv(f)
            except Exception as e:
                print(f"Error reading CSV {csv_file}: {e}")
                continue

        if df.empty:
            continue

        if "utc_time" not in df.columns:
            print(f"Skipping {csv_file}: no utc_time column")
            continue

        df["time"] = df["utc_time"].apply(dateutil.parser.isoparse)
        df.drop("utc_time", axis=1, inplace=True)

        df["deviceId"] = device_id
        df["email"] = email

        for col in df.columns:
            if col not in ["time", "deviceId", "email"]:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

        print(f"Writing to InfluxDB: {measurement.lower()}")
        print(device_id, email, measurement.lower(), "\n", df.head())

        write_api.write(
            bucket="sensor-data",
            record=df,
            data_frame_measurement_name=measurement.lower(),
            data_frame_tag_columns=["deviceId", "email"],
            data_frame_timestamp_column="time",
        )


def should_process_object(file_key, last_modified=None, processed_files=None):
    if not file_key.endswith(".zip"):
        return False

    if processed_files is not None and file_key in processed_files:
        return False

    if last_modified is not None and last_modified < START_FILTER_DATE:
        return False

    return True


def process_object(file_key, bucket_name=BUCKET_NAME, skip_if_processed=True):
    processed_files = get_processed_files()

    if skip_if_processed and file_key in processed_files:
        print(f"Skipping already processed file: {file_key}")
        return {"ok": True, "skipped": True, "reason": "already_processed", "file_key": file_key}

    if not file_key.endswith(".zip"):
        print(f"Skipping non-zip object: {file_key}")
        return {"ok": True, "skipped": True, "reason": "not_zip", "file_key": file_key}

    print(f"Processing ZIP: {file_key}")
    device_id = parse_zip_name(file_key)
    print(f"Device ID: {device_id}")

    zip_obj = s3.get_object(Bucket=bucket_name, Key=file_key)

    with zipfile.ZipFile(io.BytesIO(zip_obj["Body"].read())) as z:
        email = extract_email_from_study_metadata(z)
        update_mapping(device_id, email)
        write_sensor_csvs_from_zip(z, device_id, email)

    mark_as_processed(file_key)
    print(f"Done: {file_key} mapped to {email}")

    return {
        "ok": True,
        "skipped": False,
        "file_key": file_key,
        "device_id": device_id,
        "email": email,
    }


def process_all_new_files(bucket_name=BUCKET_NAME):
    processed_files = get_processed_files()
    response = s3.list_objects_v2(Bucket=bucket_name)

    results = []

    for obj in response.get("Contents", []):
        file_key = obj["Key"]
        last_modified = obj["LastModified"]

        if not should_process_object(
            file_key=file_key,
            last_modified=last_modified,
            processed_files=processed_files,
        ):
            continue

        result = process_object(
            file_key=file_key,
            bucket_name=bucket_name,
            skip_if_processed=True,
        )
        results.append(result)

    return results


def close_connections():
    print("Flushing buffers and closing connections...")
    write_api.flush()
    write_api.close()
    client.close()
    print("Safe to exit.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-key", help="Single MinIO object key to process")
    parser.add_argument("--bucket", default=BUCKET_NAME, help="MinIO bucket name")
    args = parser.parse_args()

    try:
        if args.file_key:
            result = process_object(args.file_key, bucket_name=args.bucket)
            print(json.dumps(result, indent=2))
        else:
            results = process_all_new_files(bucket_name=args.bucket)
            print(json.dumps(results, indent=2))
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise
    finally:
        close_connections()