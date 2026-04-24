import os
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WriteOptions
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
URL = "http://influxdb:8086"
ORG = "my-class"
BUCKET = "sensor-data"
TOKEN = os.getenv("INFLUXDB_ROOT_TOKEN")

client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
query_api = client.query_api()
write_api = client.write_api(write_options=WriteOptions(batch_size=2000))

def reindex_emails():
    print(f"Starting re-indexing of emails in bucket '{BUCKET}'...")
    
    # 1. Find all distinct email tags that have at least one uppercase letter
    # Flux can filter tags using regex
    flux_distinct = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(bucket: "{BUCKET}", tag: "email")
    '''
    
    tables = query_api.query(flux_distinct)
    emails_to_fix = []
    
    for table in tables:
        for record in table.records:
            email = record.get_value()
            if email and any(c.isupper() for c in email):
                emails_to_fix.append(email)
    
    if not emails_to_fix:
        print("No emails with uppercase characters found. Everything is already normalized.")
        return

    print(f"Found {len(emails_to_fix)} emails to normalize: {emails_to_fix}")

    for email in emails_to_fix:
        new_email = email.lower()
        print(f"Normalizing '{email}' -> '{new_email}'...")
        
        # 2. Query all data for this email
        # We need to preserve measurements and fields
        # Note: This might be data-heavy if there's a lot of data. 
        # For simplicity, we'll iterate through measurements.
        
        flux_measurements = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{BUCKET}")
        '''
        measurements = [r.get_value() for table in query_api.query(flux_measurements) for r in table.records]
        
        for m in measurements:
            print(f"  Processing measurement: {m}")
            # Get all data for this email and measurement
            flux_data = f'''
            from(bucket: "{BUCKET}")
              |> range(start: -1y) // Go back 1 year
              |> filter(fn: (r) => r["_measurement"] == "{m}")
              |> filter(fn: (r) => r["email"] == "{email}")
            '''
            
            # Use dataframes for easier re-writing
            try:
                df = query_api.query_data_frame(flux_data)
                
                if df.empty:
                    continue
                
                # If plural (list of DFs returned sometimes)
                if isinstance(df, list):
                    for sub_df in df:
                        if not sub_df.empty:
                            process_df(sub_df, m, new_email)
                else:
                    process_df(df, m, new_email)
                    
            except Exception as e:
                print(f"    Error processing {m}: {e}")

    print("Re-indexing complete. Note: Old uppercase tags still exist in InfluxDB, but new data has been written with lowercase tags.")

def process_df(df, measurement, new_email):
    # Prepare DF for writing
    # InfluxDB dataframes are expected to have a DatetimeIndex
    if "_time" in df.columns:
        df = df.set_index("_time")
    
    # Update the email tag
    df["email"] = new_email
    
    # Remove metadata columns that shouldn't be fields
    cols_to_drop = ["result", "table", "_start", "_stop", "_measurement"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    # Identify tag columns (assuming deviceId and email are the tags)
    tag_cols = ["deviceId", "email"]
    
    write_api.write(
        bucket=BUCKET,
        record=df,
        data_frame_measurement_name=measurement,
        data_frame_tag_columns=[c for c in tag_cols if c in df.columns],
    )
    print(f"    Re-wrote {len(df)} points for '{measurement}' with email='{new_email}'")

if __name__ == "__main__":
    try:
        reindex_emails()
    finally:
        write_api.flush()
        write_api.close()
        client.close()
