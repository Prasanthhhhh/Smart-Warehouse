# Warehouse Monitoring System

A professional Industrial IoT dashboard built with **Flask** + **Firebase Realtime Database** + **SQLite**, featuring live sensor monitoring, threshold alerts, and AI-powered CCTV human detection.

---

## Features

| Page | URL | Description |
|---|---|---|
| Dashboard | `/` | Live sensor overview, stats, recent alerts |
| Sensors | `/sensors` | Detailed cards for each sensor with history charts |
| CCTV | `/cctv` | Webcam + TensorFlow.js COCO-SSD human detection |
| Alerts | `/alerts` | Threshold breach log with acknowledge & clear options |

### Sensors
| Sensor | Model | Firebase Key |
|---|---|---|
| Temperature | DHT22 | `temp` |
| Humidity | DHT22 | `hum` |
| Motion | HC-SR501 (PIR) | `motion` |
| Air Quality | MQ135 | `air_real` |
| Gas | MQ2 | `gas_fake` |

---

## Prerequisites

- Python 3.10+ installed
- Internet connection (to reach Firebase)

---

## How to Run

### 1. Install dependencies

```powershell
# Run once to install Flask and requests
& "C:\Users\Nirmal S\AppData\Local\Programs\Python\Python313\python.exe" -m pip install flask requests
```

> If `pip` is not found, bootstrap it first:
> ```powershell
> & "C:\Users\Nirmal S\AppData\Local\Programs\Python\Python313\python.exe" -m ensurepip --upgrade
> ```

### 2. Start the server

```powershell
cd "C:\Users\Nirmal S\OneDrive\Desktop\warehouse monitoring"
& "C:\Users\Nirmal S\AppData\Local\Programs\Python\Python313\python.exe" app.py
```

### 3. Open in browser

```
http://127.0.0.1:5000
```

---

## Project Structure

```
warehouse monitoring/
├── app.py              # Flask app — Firebase fetch, SQLite storage, all routes & APIs
├── requirements.txt    # Python dependencies
├── warehouse.db        # SQLite database (auto-created on first run)
├── README.md
└── templates/
    ├── dashboard.html  # Main overview page
    ├── sensors.html    # Per-sensor detail cards with charts
    ├── cctv.html       # Webcam + TF.js human detection
    └── alerts.html     # Alert log with acknowledge / clear
```

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/sensors/live` | Current live sensor values |
| GET | `/api/history/<key>` | Historical readings for a sensor |
| GET | `/api/alerts` | All alerts (add `?unack=true` for unacknowledged only) |
| POST | `/api/alerts/<id>/acknowledge` | Acknowledge a single alert |
| POST | `/api/alerts/acknowledge-all` | Acknowledge all alerts |
| POST | `/api/clear-db` | **Delete all** SQLite records (readings, alerts, CCTV) |
| POST | `/api/sync` | Manually trigger a Firebase → SQLite sync |
| POST | `/api/detection` | Log a CCTV person-detection event |
| GET | `/api/stats` | Counts: readings, alerts, detections |

---

## Notes

- The app **auto-polls Firebase every 30 seconds** in the background and stores readings to SQLite.
- `warehouse.db` is created automatically — no setup needed.
- CCTV human detection runs **entirely in the browser** (TensorFlow.js) — no extra Python packages needed.
- The database can be cleared any time from the **Alerts** page → **Clear Database** button.
