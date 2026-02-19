import streamlit as st
import pandas as pd
from influxdb_client import InfluxDBClient
import os
from dotenv import load_dotenv

load_dotenv()

# --- INFLUXDB CONNECTION ---
url = f"http://{os.getenv('SERVER_IP')}:{os.getenv('INFLUXDB_PORT')}"
token = os.getenv("INFLUXDB_ROOT_TOKEN")
org = "my-class"
bucket = "sensor-data"

client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

st.set_page_config(page_title="Doctor Data Portal", layout="wide")
st.title("Sensor Dashboard")

roll_number = st.sidebar.text_input("Enter Roll Number", type="password")

def get_sensor_data(meas, roll, window="1h", aggregate="100ms"): # Changed from 5m to 100ms
    """Fetches high-frequency data without over-averaging."""
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -{window})
      |> filter(fn: (r) => r.roll_number == "{roll}")
      |> filter(fn: (r) => r._measurement == "{meas}")
      |> filter(fn: (r) => r._field == "x" or r._field == "y" or r._field == "z" or r._field == "steps")
      |> drop(columns: ["name", "roll_number", "file_key"])
      // Use a tiny window like 100ms to see the sensor movement
      |> aggregateWindow(every: {aggregate}, fn: mean, createEmpty: false)
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    return query_api.query_data_frame(query)

if roll_number:
    with st.spinner('Loading last hour of data...'):
        # 1. Accelerometer Plot
        accel_df = get_sensor_data("Accelerometer", roll_number)
        if not accel_df.empty:
            st.subheader("Accelerometer (m/s²)")
            st.line_chart(accel_df[['_time', 'x', 'y', 'z']].set_index('_time'))

        # 2. Gyroscope Plot
        gyro_df = get_sensor_data("Gyroscope", roll_number)
        if not gyro_df.empty:
            st.subheader("Gyroscope (rad/s)")
            st.line_chart(gyro_df[['_time', 'x', 'y', 'z']].set_index('_time'))

        # 3. Pedometer Plot
        pedometer_df = get_sensor_data("Pedometer", roll_number)
        if not pedometer_df.empty:
            st.subheader("Steps (Cumulative)")
            st.area_chart(pedometer_df[['_time', 'steps']].set_index('_time'))

    # 4. Activity Log (Table)
    act_query = f'''
    from(bucket: "{bucket}")
    |> range(start: -1h)
    |> filter(fn: (r) => r.roll_number == "{roll_number}")
    |> filter(fn: (r) => r._measurement == "Activity")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    activity_df = query_api.query_data_frame(act_query)

    if not activity_df.empty:
        st.subheader("Activity Log")
        
        # Sensor Logger columns are usually 'activity' and 'confidence'
        # We dynamically select only the columns that actually exist
        available_cols = activity_df.columns.tolist()
        target_cols = ['_time', 'activity', 'confidence']
        
        # Only show columns that are present in the dataframe
        display_cols = [c for c in target_cols if c in available_cols]
        
        st.dataframe(activity_df[display_cols], use_container_width=True)
    else:
        st.info("No activity transitions recorded in the last hour.")