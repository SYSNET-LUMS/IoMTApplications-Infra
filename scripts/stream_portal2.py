from flask import Flask, request, jsonify, render_template_string
import json
import os
from datetime import datetime, timezone, timedelta

import dateutil.parser
from dotenv import load_dotenv
from influxdb_client import Point, WriteOptions, InfluxDBClient

from urllib.parse import unquote_plus
from ingestion_minio_to_influxdb import process_object

app = Flask(__name__)

load_dotenv()

# ---------- device mapping ----------

_cached_mappings = {}
_mappings_mtime = 0

def get_mappings():
    global _cached_mappings, _mappings_mtime
    try:
        if os.path.exists("mappings.json"):
            mtime = os.path.getmtime("mappings.json")
            if mtime > _mappings_mtime:
                with open("mappings.json", "r") as f:
                    _cached_mappings = json.load(f)
                _mappings_mtime = mtime
    except Exception as e:
        print(f"Error loading mappings.json: {e}")
    return _cached_mappings

# Initial load
get_mappings()

def get_device_id_for_email(email: str):
    email = str(email).strip().lower()
    mappings = get_mappings()
    for dev_id, mapped_email in mappings.items():
        if str(mapped_email).strip().lower() == email:
            return dev_id
    return None

# ---------- influxdb ----------

client = InfluxDBClient(
    url=os.getenv("INFLUXDB_URL", "http://influxdb:8086"),
    token=os.getenv("INFLUXDB_ROOT_TOKEN"),
    org=os.getenv("INFLUXDB_ORG", "my-class")
)

write_api = client.write_api(write_options=WriteOptions(
    batch_size=2000,
    flush_interval=1000,
    retry_interval=5000
))

query_api = client.query_api()

BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor-data")
ORG = os.getenv("INFLUXDB_ORG", "my-class")

# ---------- ingest endpoint ----------

@app.route('/ddh/stream', methods=['POST'])
def handle_stream():
    data = request.get_json(silent=True) or {}

    raw_device_id = data.get('deviceId', '')
    device_id = raw_device_id.replace('-', '')
    mappings = get_mappings()
    email = mappings.get(device_id, "PENDING").lower()

    payload = data.get("payload", [])
    if not payload:
        return jsonify({"error": "Empty payload"}), 400

    points = []

    for entry in payload:
        sensor_name = entry.get("name")
        values = entry.get("values", {})

        if not sensor_name:
            continue

        utc_str = values.get("utc_time")
        if not utc_str:
            continue

        try:
            timestamp = dateutil.parser.isoparse(utc_str)
        except Exception:
            continue

        point = (
            Point(sensor_name)
            .tag("deviceId", device_id)
            .tag("email", email)
            .time(timestamp)
        )

        added_numeric_field = False
        for field_name, field_value in values.items():
            if field_name == "utc_time":
                continue

            if isinstance(field_value, (int, float)):
                point.field(field_name, float(field_value))
                added_numeric_field = True

        if added_numeric_field:
            points.append(point)

    if not points:
        return jsonify({"error": "No valid sensor points found"}), 400

    try:
        write_api.write(bucket=BUCKET, org=ORG, record=points)
    except Exception as e:
        print(f"InfluxDB write error: {e}")
        return jsonify({"error": "Database write failed"}), 500

    return jsonify({
        "ok": True,
        "deviceId": device_id,
        "email": email,
        "points_written": len(points)
    }), 200

# ----------

@app.route('/ddh/minio-events', methods=['POST'])
def minio_events():
    print("HELLO")
    data = request.get_json(silent=True) or {}
    print("MINIO WEBHOOK PAYLOAD:", json.dumps(data, indent=2))

    records = data.get("Records", [])
    if not isinstance(records, list):
        print("Invalid webhook payload: no Records list")
        return jsonify({"ok": False, "error": "Invalid payload"}), 400

    results = []

    for record in records:
        event_name = str(record.get("eventName", ""))
        bucket = record.get("s3", {}).get("bucket", {}).get("name", "class-data")
        raw_key = record.get("s3", {}).get("object", {}).get("key")

        print("EVENT:", event_name)
        print("BUCKET:", bucket)
        print("RAW KEY:", raw_key)

        if "ObjectCreated" not in event_name:
            print("Skipping: not ObjectCreated")
            results.append({
                "skipped": True,
                "reason": "not_object_created",
                "eventName": event_name
            })
            continue

        if not raw_key:
            print("Skipping: missing key")
            results.append({
                "skipped": True,
                "reason": "missing_key"
            })
            continue

        file_key = unquote_plus(raw_key)
        print("DECODED KEY:", file_key)

        if not file_key.endswith(".zip"):
            print("Skipping: not a zip")
            results.append({
                "skipped": True,
                "reason": "not_zip",
                "file_key": file_key
            })
            continue

        try:
            print(f"Calling process_object for {file_key}")
            result = process_object(file_key=file_key, bucket_name=bucket, skip_if_processed=True)
            print("INGESTION RESULT:", result)
            results.append(result)
        except Exception as e:
            print(f"Failed processing {file_key}: {e}")
            results.append({
                "ok": False,
                "file_key": file_key,
                "error": str(e)
            })

    print("WEBHOOK RESULTS:", results)
    return jsonify({"ok": True, "results": results}), 200

# ---------- query helpers ----------

def query_latest_accel_points(device_id: str, seconds: int = 20, limit: int = 2000):
    stop = datetime.now(timezone.utc)
    start = stop - timedelta(seconds=seconds)

    flux = f'''
from(bucket: "{BUCKET}")
  |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
  |> filter(fn: (r) => r["_measurement"] == "accelerometer")
  |> filter(fn: (r) => r["deviceId"] == "{device_id}")
  |> filter(fn: (r) => r["_field"] == "x" or r["_field"] == "y" or r["_field"] == "z")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "x", "y", "z"])
  |> sort(columns: ["_time"], desc: false)
  |> limit(n: {limit})
'''

    tables = query_api.query(flux, org=ORG)

    out = []
    for table in tables:
        for rec in table.records:
            t = rec.get_time()
            out.append({
                "time": t.isoformat().replace("+00:00", "Z") if t else None,
                "x": float(rec.values.get("x")) if rec.values.get("x") is not None else None,
                "y": float(rec.values.get("y")) if rec.values.get("y") is not None else None,
                "z": float(rec.values.get("z")) if rec.values.get("z") is not None else None,
            })

    out.sort(key=lambda d: d["time"] or "")
    return out

# ---------- api routes ----------

@app.route('/latest/<path:email>', methods=['GET'])
def get_latest(email):
    email = email.lower()
    device_id = get_device_id_for_email(email)
    if not device_id:
        return jsonify({"error": "Unknown email"}), 404

    points = query_latest_accel_points(device_id=device_id, seconds=20, limit=4000)

    return jsonify({
        "email": email,
        "deviceId": device_id,
        "window_seconds": 20,
        "points": points,
        "count": len(points),
    }), 200

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"ok": True}), 200

# ---------- graph page ----------

@app.route('/view/<path:email>')
def view_data(email):
    email = email.lower()
    # The file will be reloaded by get_mappings() if it has changed
    mappings = get_mappings()

    device_id = get_device_id_for_email(email)
    if not device_id:
        return "Unknown email", 404

    html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>{{ email }} - Last 20s Accelerometer</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .meta { margin-bottom: 12px; }
    .wrap { max-width: 1100px; }
    canvas { background: #fff; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="meta">
      <h2>{{ email }} — Accelerometer (last 20s)</h2>
      <div><b>Device:</b> {{ device_id }}</div>
      <div id="status">Loading…</div>
    </div>

    <canvas id="accelChart" height="120"></canvas>
  </div>

  <script>
    const email = "{{ email }}";
    const statusEl = document.getElementById("status");
    const ctx = document.getElementById("accelChart").getContext("2d");

    const chart = new Chart(ctx, {
      type: "line",
      data: {
        labels: [],
        datasets: [
          { label: "x", data: [], tension: 0.2, pointRadius: 0 },
          { label: "y", data: [], tension: 0.2, pointRadius: 0 },
          { label: "z", data: [], tension: 0.2, pointRadius: 0 },
        ],
      },
      options: {
        responsive: true,
        animation: false,
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true } },
          y: { title: { display: true, text: "m/s² (raw)" } }
        },
        plugins: {
          legend: { position: "top" },
          tooltip: { mode: "index", intersect: false }
        },
        interaction: { mode: "index", intersect: false }
      }
    });

    function fmtTime(iso) {
      if (!iso) return "";
      const d = new Date(iso);
      const hh = String(d.getUTCHours()).padStart(2,"0");
      const mm = String(d.getUTCMinutes()).padStart(2,"0");
      const ss = String(d.getUTCSeconds()).padStart(2,"0");
      const ms = String(d.getUTCMilliseconds()).padStart(3,"0");
      return `${hh}:${mm}:${ss}.${ms}`;
    }

    async function refresh() {
      try {
        const res = await fetch(`/latest/${encodeURIComponent(email)}`);
        const j = await res.json();
        if (!res.ok) throw new Error(j.error || "request failed");

        const pts = j.points || [];
        statusEl.textContent = `Samples: ${j.count || 0} | Updated: ${new Date().toLocaleTimeString()}`;

        chart.data.labels = pts.map(p => fmtTime(p.time));
        chart.data.datasets[0].data = pts.map(p => p.x);
        chart.data.datasets[1].data = pts.map(p => p.y);
        chart.data.datasets[2].data = pts.map(p => p.z);
        chart.update();
      } catch (e) {
        statusEl.textContent = "Error: " + e.message;
      }
    }

    refresh();
    setInterval(refresh, 1000);
  </script>
</body>
</html>
"""
    return render_template_string(html, email=email, device_id=device_id)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)