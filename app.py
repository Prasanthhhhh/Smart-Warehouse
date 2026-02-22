import os
import sqlite3
import threading
import time
from datetime import datetime

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

FIREBASE_DB_URL = (
    "https://warehouse-monitoring-52717-default-rtdb.asia-southeast1.firebasedatabase.app"
)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "warehouse.db")
POLL_INTERVAL = 30  # seconds between Firebase polls

# ─────────────────────────────────────────────────────────────────────────────
# Sensor configuration: proper industrial sensor names + thresholds
# ─────────────────────────────────────────────────────────────────────────────
SENSOR_CONFIG = {
    "temp": {
        "display_name": "DHT22 Temperature",
        "model": "DHT22",
        "unit": "°C",
        "max_threshold": 35.0,
        "min_threshold": 5.0,
        "category": "Environmental",
        "description": "Ambient air temperature",
        "icon": "thermometer",
    },
    "hum": {
        "display_name": "DHT22 Humidity",
        "model": "DHT22",
        "unit": "%RH",
        "max_threshold": 80.0,
        "min_threshold": 20.0,
        "category": "Environmental",
        "description": "Relative humidity level",
        "icon": "droplets",
    },
    "motion": {
        "display_name": "PIR Motion Sensor",
        "model": "HC-SR501",
        "unit": "",
        "max_threshold": 0.5,
        "min_threshold": None,
        "category": "Security",
        "description": "Passive infrared motion detector",
        "icon": "radar",
    },
    "air_real": {
        "display_name": "MQ135 Air Quality",
        "model": "MQ135",
        "unit": "ppm",
        "max_threshold": 200.0,
        "min_threshold": None,
        "category": "Air Quality",
        "description": "CO₂ / ammonia / benzene sensor",
        "icon": "wind",
    },
    "gas_fake": {
        "display_name": "MQ2 Gas Sensor",
        "model": "MQ2",
        "unit": "ppm",
        "max_threshold": 300.0,
        "min_threshold": None,
        "category": "Safety",
        "description": "LPG / smoke / propane detector",
        "icon": "flame",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# SQLite helpers
# ─────────────────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_key   TEXT    NOT NULL,
            sensor_name  TEXT    NOT NULL,
            value        REAL    NOT NULL,
            unit         TEXT    DEFAULT '',
            timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_key   TEXT    NOT NULL,
            sensor_name  TEXT    NOT NULL,
            value        REAL    NOT NULL,
            threshold    REAL    NOT NULL,
            alert_type   TEXT    NOT NULL,
            message      TEXT    NOT NULL,
            timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP,
            acknowledged INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS cctv_events (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            person_count   INTEGER NOT NULL,
            max_confidence REAL,
            zone           TEXT    DEFAULT 'ZONE-A',
            timestamp      DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit()
    conn.close()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def store_readings(sensor_data: dict):
    """Persist sensor readings and generate alerts if thresholds are breached."""
    conn = get_db()
    c = conn.cursor()
    for key, raw_value in sensor_data.items():
        if raw_value is None:
            continue
        cfg = SENSOR_CONFIG.get(key, {})
        name = cfg.get("display_name", key.upper())
        unit = cfg.get("unit", "")
        try:
            val = float(raw_value)
        except (ValueError, TypeError):
            continue

        c.execute(
            "INSERT INTO sensor_readings (sensor_key, sensor_name, value, unit) VALUES (?,?,?,?)",
            (key, name, val, unit),
        )

        max_t = cfg.get("max_threshold")
        min_t = cfg.get("min_threshold")
        if max_t is not None and val > max_t:
            c.execute(
                "INSERT INTO alerts (sensor_key, sensor_name, value, threshold, alert_type, message) "
                "VALUES (?,?,?,?,?,?)",
                (key, name, val, max_t, "HIGH",
                 f"{name} HIGH ALERT: {val}{unit} exceeds limit {max_t}{unit}"),
            )
        if min_t is not None and val < min_t:
            c.execute(
                "INSERT INTO alerts (sensor_key, sensor_name, value, threshold, alert_type, message) "
                "VALUES (?,?,?,?,?,?)",
                (key, name, val, min_t, "LOW",
                 f"{name} LOW ALERT: {val}{unit} below limit {min_t}{unit}"),
            )
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Firebase fetch
# ─────────────────────────────────────────────────────────────────────────────
def fetch_firebase_root():
    try:
        r = requests.get(f"{FIREBASE_DB_URL}/.json", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        print(f"[Firebase] {exc}")
        return None


def get_sensors_from_root(root_data):
    if not root_data or not isinstance(root_data, dict):
        return {}
    return root_data.get("sensors", root_data)


# ─────────────────────────────────────────────────────────────────────────────
# Background polling thread
# ─────────────────────────────────────────────────────────────────────────────
def background_poller():
    while True:
        try:
            root = fetch_firebase_root()
            sensors = get_sensors_from_root(root)
            if sensors:
                store_readings(sensors)
        except Exception as exc:
            print(f"[Poller] {exc}")
        time.sleep(POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# Page routes
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/")
def dashboard():
    root = fetch_firebase_root()
    live_sensors = get_sensors_from_root(root)

    conn = get_db()
    total_readings = conn.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0]
    unacked_alerts = conn.execute(
        "SELECT COUNT(*) FROM alerts WHERE acknowledged=0"
    ).fetchone()[0]
    recent_alerts = conn.execute(
        "SELECT * FROM alerts ORDER BY timestamp DESC LIMIT 8"
    ).fetchall()

    # Latest value per sensor from SQLite
    latest_db = {}
    for key in SENSOR_CONFIG:
        row = conn.execute(
            "SELECT value, timestamp FROM sensor_readings WHERE sensor_key=? ORDER BY timestamp DESC LIMIT 1",
            (key,),
        ).fetchone()
        latest_db[key] = dict(row) if row else None

    # Record count per sensor
    sensor_counts = {}
    for key in SENSOR_CONFIG:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM sensor_readings WHERE sensor_key=?", (key,)
        ).fetchone()[0]
        sensor_counts[key] = cnt

    # Recent CCTV detections
    last_detection = conn.execute(
        "SELECT * FROM cctv_events ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()

    conn.close()
    return render_template(
        "dashboard.html",
        live_sensors=live_sensors,
        sensor_config=SENSOR_CONFIG,
        total_readings=total_readings,
        unacked_alerts=unacked_alerts,
        recent_alerts=recent_alerts,
        latest_db=latest_db,
        sensor_counts=sensor_counts,
        last_detection=last_detection,
    )


@app.route("/sensors")
def sensors_page():
    root = fetch_firebase_root()
    live_sensors = get_sensors_from_root(root)
    return render_template(
        "sensors.html",
        live_sensors=live_sensors,
        sensor_config=SENSOR_CONFIG,
    )


@app.route("/cctv")
def cctv_page():
    conn = get_db()
    recent_events = conn.execute(
        "SELECT * FROM cctv_events ORDER BY timestamp DESC LIMIT 30"
    ).fetchall()
    total_detections = conn.execute("SELECT COUNT(*) FROM cctv_events").fetchone()[0]
    today_detections = conn.execute(
        "SELECT COUNT(*) FROM cctv_events WHERE date(timestamp)=date('now')"
    ).fetchone()[0]
    conn.close()
    return render_template(
        "cctv.html",
        recent_events=recent_events,
        total_detections=total_detections,
        today_detections=today_detections,
    )


@app.route("/alerts")
def alerts_page():
    conn = get_db()
    all_alerts = conn.execute(
        "SELECT * FROM alerts ORDER BY timestamp DESC LIMIT 200"
    ).fetchall()
    unacked = conn.execute(
        "SELECT COUNT(*) FROM alerts WHERE acknowledged=0"
    ).fetchone()[0]
    conn.close()
    return render_template("alerts.html", alerts=all_alerts, unacked=unacked)


# ─────────────────────────────────────────────────────────────────────────────
# JSON API endpoints
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/data")
def api_all_data():
    return jsonify(fetch_firebase_root())


@app.route("/api/sensors/live")
def api_sensors_live():
    root = fetch_firebase_root()
    live = get_sensors_from_root(root)
    result = {}
    for key, cfg in SENSOR_CONFIG.items():
        result[key] = {**cfg, "current_value": live.get(key) if live else None}
    return jsonify(result)


@app.route("/api/history/<sensor_key>")
def api_history(sensor_key):
    limit = request.args.get("limit", 50, type=int)
    conn = get_db()
    rows = conn.execute(
        "SELECT value, timestamp FROM sensor_readings WHERE sensor_key=? ORDER BY timestamp DESC LIMIT ?",
        (sensor_key, limit),
    ).fetchall()
    conn.close()
    return jsonify([{"value": r["value"], "timestamp": r["timestamp"]} for r in reversed(rows)])


@app.route("/api/alerts")
def api_alerts():
    unack_only = request.args.get("unack", "false").lower() == "true"
    conn = get_db()
    q = "SELECT * FROM alerts" + (" WHERE acknowledged=0" if unack_only else "")
    q += " ORDER BY timestamp DESC LIMIT 100"
    rows = conn.execute(q).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/alerts/<int:alert_id>/acknowledge", methods=["POST"])
def api_acknowledge(alert_id):
    conn = get_db()
    conn.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (alert_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/api/alerts/acknowledge-all", methods=["POST"])
def api_acknowledge_all():
    conn = get_db()
    conn.execute("UPDATE alerts SET acknowledged=1")
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/api/clear-db", methods=["POST"])
def api_clear_db():
    """Delete all historical data from every SQLite table."""
    conn = get_db()
    conn.execute("DELETE FROM sensor_readings")
    conn.execute("DELETE FROM alerts")
    conn.execute("DELETE FROM cctv_events")
    # Reset auto-increment counters
    conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('sensor_readings','alerts','cctv_events')")
    conn.commit()
    conn.close()
    return jsonify({"success": True, "message": "All database records cleared."})


@app.route("/api/sync", methods=["POST"])
def api_sync():
    root = fetch_firebase_root()
    sensors = get_sensors_from_root(root)
    if sensors:
        store_readings(sensors)
        return jsonify({"success": True, "synced_keys": list(sensors.keys())})
    return jsonify({"success": False, "error": "No data from Firebase"})


@app.route("/api/detection", methods=["POST"])
def api_detection():
    body = request.get_json(silent=True) or {}
    person_count = int(body.get("person_count", 0))
    max_confidence = float(body.get("max_confidence", 0.0))
    zone = str(body.get("zone", "ZONE-A"))
    conn = get_db()
    conn.execute(
        "INSERT INTO cctv_events (person_count, max_confidence, zone) VALUES (?,?,?)",
        (person_count, max_confidence, zone),
    )
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/api/detection/history")
def api_detection_history():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM cctv_events ORDER BY timestamp DESC LIMIT 50"
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/stats")
def api_stats():
    conn = get_db()
    stats = {
        "total_readings": conn.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0],
        "unacked_alerts": conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE acknowledged=0"
        ).fetchone()[0],
        "total_alerts": conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0],
        "total_detections": conn.execute("SELECT COUNT(*) FROM cctv_events").fetchone()[0],
        "today_detections": conn.execute(
            "SELECT COUNT(*) FROM cctv_events WHERE date(timestamp)=date('now')"
        ).fetchone()[0],
    }
    conn.close()
    return jsonify(stats)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    # Only start the background thread in the Werkzeug child process (not the
    # file-watcher process) to avoid running two pollers in debug/reload mode.
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        t = threading.Thread(target=background_poller, daemon=True)
        t.start()
        print("[Poller] Background Firebase polling started.")
    app.run(debug=True, host="0.0.0.0", port=5000)
