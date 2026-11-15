
import os
import math
import sqlite3
import threading
import time

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

FIREBASE_DB_URL = (
    "https://warehouse-monitoring-52717-default-rtdb.asia-southeast1.firebasedatabase.app"
)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "warehouse.db")
POLL_INTERVAL = 30

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


# -------------------------------------------------------------------
# DATABASE
# -------------------------------------------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_key TEXT NOT NULL,
                sensor_name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT DEFAULT '',
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_key TEXT NOT NULL,
                sensor_name TEXT NOT NULL,
                value REAL NOT NULL,
                threshold REAL NOT NULL,
                alert_type TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                acknowledged INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS cctv_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_count INTEGER NOT NULL,
                max_confidence REAL,
                zone TEXT DEFAULT 'ZONE-A',
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_sensor_history
            ON sensor_readings(sensor_key, timestamp);

            CREATE INDEX IF NOT EXISTS idx_alerts_timestamp
            ON alerts(timestamp);

            CREATE INDEX IF NOT EXISTS idx_cctv_timestamp
            ON cctv_events(timestamp);
            """
        )


# Initialize database even when the app is started through a WSGI server.
init_db()


# -------------------------------------------------------------------
# SENSOR DATA
# -------------------------------------------------------------------

def get_alert_type(value, config):
    max_threshold = config.get("max_threshold")
    min_threshold = config.get("min_threshold")

    if max_threshold is not None and value > max_threshold:
        return "HIGH", max_threshold

    if min_threshold is not None and value < min_threshold:
        return "LOW", min_threshold

    return None, None


def store_readings(sensor_data):
    """Store valid sensor readings and create alerts on threshold transitions."""

    if not isinstance(sensor_data, dict):
        return

    with get_db() as conn:
        for key, raw_value in sensor_data.items():

            config = SENSOR_CONFIG.get(key)

            if config is None or raw_value is None:
                continue

            try:
                value = float(raw_value)
            except (ValueError, TypeError):
                continue

            if not math.isfinite(value):
                continue

            name = config["display_name"]
            unit = config["unit"]

            previous = conn.execute(
                """
                SELECT value FROM sensor_readings
                WHERE sensor_key = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (key,),
            ).fetchone()

            conn.execute(
                """
                INSERT INTO sensor_readings
                (sensor_key, sensor_name, value, unit)
                VALUES (?, ?, ?, ?)
                """,
                (key, name, value, unit),
            )

            current_alert, threshold = get_alert_type(value, config)

            previous_alert = None

            if previous:
                previous_alert, _ = get_alert_type(
                    float(previous["value"]), config
                )

            # Create an alert only when the sensor enters a new alert state.
            if current_alert and current_alert != previous_alert:

                if current_alert == "HIGH":
                    message = (
                        f"{name} HIGH ALERT: {value}{unit} "
                        f"exceeds limit {threshold}{unit}"
                    )
                else:
                    message = (
                        f"{name} LOW ALERT: {value}{unit} "
                        f"below limit {threshold}{unit}"
                    )

                conn.execute(
                    """
                    INSERT INTO alerts
                    (sensor_key, sensor_name, value, threshold,
                     alert_type, message)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        key,
                        name,
                        value,
                        threshold,
                        current_alert,
                        message,
                    ),
                )


# -------------------------------------------------------------------
# FIREBASE
# -------------------------------------------------------------------

def fetch_firebase_root():
    try:
        response = requests.get(
            f"{FIREBASE_DB_URL}/.json",
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        return data

    except requests.RequestException as exc:
        print(f"[Firebase Error] {exc}")
        return None

    except ValueError as exc:
        print(f"[Firebase JSON Error] {exc}")
        return None


def get_sensors_from_root(root_data):
    if not isinstance(root_data, dict):
        return {}

    sensors = root_data.get("sensors", root_data)

    if not isinstance(sensors, dict):
        return {}

    return {
        key: value
        for key, value in sensors.items()
        if key in SENSOR_CONFIG
    }


# -------------------------------------------------------------------
# BACKGROUND POLLING
# -------------------------------------------------------------------

poller_started = False
poller_lock = threading.Lock()


def background_poller():
    while True:
        try:
            root = fetch_firebase_root()
            sensors = get_sensors_from_root(root)

            if sensors:
                store_readings(sensors)

        except Exception as exc:
            print(f"[Poller Error] {exc}")

        time.sleep(POLL_INTERVAL)


def start_background_poller():
    global poller_started

    with poller_lock:
        if poller_started:
            return

        thread = threading.Thread(
            target=background_poller,
            daemon=True,
            name="FirebasePoller",
        )
        thread.start()
        poller_started = True

        print("[Poller] Background polling started.")


# -------------------------------------------------------------------
# PAGE ROUTES
# -------------------------------------------------------------------

@app.route("/")
def dashboard():

    root = fetch_firebase_root()
    live_sensors = get_sensors_from_root(root)

    with get_db() as conn:

        total_readings = conn.execute(
            "SELECT COUNT(*) FROM sensor_readings"
        ).fetchone()[0]

        unacked_alerts = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE acknowledged = 0"
        ).fetchone()[0]

        recent_alerts = conn.execute(
            "SELECT * FROM alerts ORDER BY timestamp DESC, id DESC LIMIT 8"
        ).fetchall()

        latest_db = {}
        sensor_counts = {}

        for key in SENSOR_CONFIG:

            row = conn.execute(
                """
                SELECT value, timestamp
                FROM sensor_readings
                WHERE sensor_key = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (key,),
            ).fetchone()

            latest_db[key] = dict(row) if row else None

            count = conn.execute(
                "SELECT COUNT(*) FROM sensor_readings WHERE sensor_key = ?",
                (key,),
            ).fetchone()[0]

            sensor_counts[key] = count

        last_detection = conn.execute(
            """
            SELECT * FROM cctv_events
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

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

    with get_db() as conn:

        recent_events = conn.execute(
            """
            SELECT * FROM cctv_events
            ORDER BY id DESC
            LIMIT 30
            """
        ).fetchall()

        total_detections = conn.execute(
            "SELECT COUNT(*) FROM cctv_events"
        ).fetchone()[0]

        today_detections = conn.execute(
            """
            SELECT COUNT(*) FROM cctv_events
            WHERE date(timestamp) = date('now', '+5 hours', '+30 minutes')
            """
        ).fetchone()[0]

    return render_template(
        "cctv.html",
        recent_events=recent_events,
        total_detections=total_detections,
        today_detections=today_detections,
    )


@app.route("/alerts")
def alerts_page():

    with get_db() as conn:

        all_alerts = conn.execute(
            """
            SELECT * FROM alerts
            ORDER BY id DESC
            LIMIT 200
            """
        ).fetchall()

        unacked = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE acknowledged = 0"
        ).fetchone()[0]

    return render_template(
        "alerts.html",
        alerts=all_alerts,
        unacked=unacked,
    )


# -------------------------------------------------------------------
# JSON API
# -------------------------------------------------------------------

@app.route("/api/data")
def api_all_data():

    data = fetch_firebase_root()

    if data is None:
        return jsonify({
            "success": False,
            "error": "Unable to fetch Firebase data",
        }), 502

    return jsonify(data)


@app.route("/api/sensors/live")
def api_sensors_live():

    root = fetch_firebase_root()
    live = get_sensors_from_root(root)

    result = {}

    for key, config in SENSOR_CONFIG.items():
        result[key] = {
            **config,
            "current_value": live.get(key),
        }

    return jsonify(result)


@app.route("/api/history/<sensor_key>")
def api_history(sensor_key):

    if sensor_key not in SENSOR_CONFIG:
        return jsonify({
            "success": False,
            "error": "Invalid sensor key",
        }), 404

    limit = request.args.get("limit", default=50, type=int)

    if limit is None:
        limit = 50

    limit = max(1, min(limit, 200))

    with get_db() as conn:

        rows = conn.execute(
            """
            SELECT value, timestamp
            FROM sensor_readings
            WHERE sensor_key = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (sensor_key, limit),
        ).fetchall()

    return jsonify([
        {
            "value": row["value"],
            "timestamp": row["timestamp"],
        }
        for row in reversed(rows)
    ])


@app.route("/api/alerts")
def api_alerts():

    unack_only = (
        request.args.get("unack", "false").lower() == "true"
    )

    query = "SELECT * FROM alerts"

    if unack_only:
        query += " WHERE acknowledged = 0"

    query += " ORDER BY id DESC LIMIT 100"

    with get_db() as conn:
        rows = conn.execute(query).fetchall()

    return jsonify([dict(row) for row in rows])


@app.route("/api/alerts/<int:alert_id>/acknowledge", methods=["POST"])
def api_acknowledge(alert_id):

    with get_db() as conn:

        cursor = conn.execute(
            "UPDATE alerts SET acknowledged = 1 WHERE id = ?",
            (alert_id,),
        )

        if cursor.rowcount == 0:
            return jsonify({
                "success": False,
                "error": "Alert not found",
            }), 404

    return jsonify({"success": True})


@app.route("/api/alerts/acknowledge-all", methods=["POST"])
def api_acknowledge_all():

    with get_db() as conn:
        conn.execute(
            "UPDATE alerts SET acknowledged = 1"
        )

    return jsonify({"success": True})


@app.route("/api/clear-db", methods=["POST"])
def api_clear_db():
    """
    Database clearing is disabled by default.
    Enable it only when required using ALLOW_DATABASE_CLEAR=true.
    """

    if os.environ.get("ALLOW_DATABASE_CLEAR", "false").lower() != "true":
        return jsonify({
            "success": False,
            "error": "Database clearing is disabled.",
        }), 403

    with get_db() as conn:

        conn.execute("DELETE FROM sensor_readings")
        conn.execute("DELETE FROM alerts")
        conn.execute("DELETE FROM cctv_events")

        conn.execute(
            """
            DELETE FROM sqlite_sequence
            WHERE name IN (
                'sensor_readings',
                'alerts',
                'cctv_events'
            )
            """
        )

    return jsonify({
        "success": True,
        "message": "All database records cleared.",
    })


@app.route("/api/sync", methods=["POST"])
def api_sync():

    root = fetch_firebase_root()

    if root is None:
        return jsonify({
            "success": False,
            "error": "Unable to connect to Firebase",
        }), 502

    sensors = get_sensors_from_root(root)

    if not sensors:
        return jsonify({
            "success": False,
            "error": "No valid sensor data found",
        }), 404

    store_readings(sensors)

    return jsonify({
        "success": True,
        "synced_keys": list(sensors.keys()),
    })


@app.route("/api/detection", methods=["POST"])
def api_detection():

    body = request.get_json(silent=True)

    if not isinstance(body, dict):
        return jsonify({
            "success": False,
            "error": "A valid JSON object is required",
        }), 400

    try:
        person_count = int(body.get("person_count", 0))
        max_confidence = float(body.get("max_confidence", 0.0))
    except (ValueError, TypeError):
        return jsonify({
            "success": False,
            "error": "Invalid detection values",
        }), 400

    if not 0 <= person_count <= 1000:
        return jsonify({
            "success": False,
            "error": "Invalid person count",
        }), 400

    if (
        not math.isfinite(max_confidence)
        or not 0 <= max_confidence <= 1
    ):
        return jsonify({
            "success": False,
            "error": "Confidence must be between 0 and 1",
        }), 400

    zone = str(body.get("zone", "ZONE-A")).strip()

    if not zone or len(zone) > 100:
        return jsonify({
            "success": False,
            "error": "Invalid zone",
        }), 400

    with get_db() as conn:

        conn.execute(
            """
            INSERT INTO cctv_events
            (person_count, max_confidence, zone)
            VALUES (?, ?, ?)
            """,
            (person_count, max_confidence, zone),
        )

    return jsonify({"success": True}), 201


@app.route("/api/detection/history")
def api_detection_history():

    with get_db() as conn:

        rows = conn.execute(
            """
            SELECT * FROM cctv_events
            ORDER BY id DESC
            LIMIT 50
            """
        ).fetchall()

    return jsonify([dict(row) for row in rows])


@app.route("/api/stats")
def api_stats():

    with get_db() as conn:

        stats = {
            "total_readings": conn.execute(
                "SELECT COUNT(*) FROM sensor_readings"
            ).fetchone()[0],

            "unacked_alerts": conn.execute(
                "SELECT COUNT(*) FROM alerts WHERE acknowledged = 0"
            ).fetchone()[0],

            "total_alerts": conn.execute(
                "SELECT COUNT(*) FROM alerts"
            ).fetchone()[0],

            "total_detections": conn.execute(
                "SELECT COUNT(*) FROM cctv_events"
            ).fetchone()[0],

            "today_detections": conn.execute(
                """
                SELECT COUNT(*) FROM cctv_events
                WHERE date(timestamp) = date('now', '+5 hours', '+30 minutes')
                """
            ).fetchone()[0],
        }

    return jsonify(stats)


# -------------------------------------------------------------------
# APPLICATION ENTRY POINT
# -------------------------------------------------------------------

if __name__ == "__main__":

    debug_mode = (
        os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    )

    # Start the poller only in the actual Werkzeug child process
    # when debug reloading is enabled.
    if not debug_mode or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        start_background_poller()

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=debug_mode,
        use_reloader=debug_mode,
    )
