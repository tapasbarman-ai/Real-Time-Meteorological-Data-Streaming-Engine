
import json
import sys
import os
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone

from confluent_kafka import Producer

from client import producer_config, BOOTSTRAP_SERVERS

KAFKA_TOPIC = "radio-sonde"
KAFKA_BROKER = BOOTSTRAP_SERVERS

# =========================================================
# CONFIGURATION
# =========================================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BOUNDARY_FILE = os.path.join(
    PROJECT_ROOT,
    "data/boundary_layer_data_altreport_20220328_1431.txt"
)
SIM_FILE = os.path.join(
    PROJECT_ROOT,
    "data/radiosonde_sim_RS92SGP_20220328.txt"
)

SEND_INTERVAL_SEC = 1  # 1 row per second

# =========================================================
# LOGGING
# =========================================================
log_dir = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger("COMBINED_PRODUCER")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

file_handler = RotatingFileHandler(
    os.path.join(log_dir, "combined_producer.log"),
    maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.info("=================================================")
logger.info("🔗 Combined Producer (Boundary + Sim) → Kafka")
logger.info("=================================================")


# =========================================================
# PARSE BOUNDARY LAYER FILE
# =========================================================
def parse_boundary_header(lines):
    """Extract the 6 static metadata fields from the boundary-layer file."""
    header = {}
    key_map = {
        "sonde serial number":                "sonde_serial_number",
        "sonde type":                          "sonde_type",
        "release point longitude":             "release_longitude",
        "release point latitude":              "release_latitude",
        "release point height from sea level": "release_height_msl",
        "balloon release date and time":       "balloon_release_datetime",
    }
    for line in lines:
        if "\t" in line:
            parts = line.split("\t", 1)
            raw_key = parts[0].strip().lower()
            value   = parts[1].strip() if len(parts) > 1 else ""
            for pattern, field in key_map.items():
                if pattern in raw_key:
                    header[field] = value
                    break
    return header


def parse_boundary_data(lines):
    """
    Parse data rows from the boundary-layer file.
    Columns: n  HeightMSL  Temp  P  RH  Dir  Speed  MRI
    Returns list of dicts keyed by row_n.
    """
    rows = []
    data_started = False
    for line in lines:
        stripped = line.strip()

        if not data_started:
            if "\xb0C" in line or "mb" in line:
                data_started = True
            continue

        if not stripped:
            continue

        parts = stripped.split()
        if len(parts) < 2 or not parts[0].lstrip("-").isdigit():
            continue

        try:
            rows.append({
                "row_n":          int(parts[0]),
                "height_msl_m":  float(parts[1]),
                "temp_c":        float(parts[2]) if len(parts) > 2 else None,
                "pressure_mb":   float(parts[3]) if len(parts) > 3 else None,
                "rh_pct":        float(parts[4]) if len(parts) > 4 else None,
                "wind_dir_deg":  float(parts[5]) if len(parts) > 5 else None,
                "wind_speed_ms": float(parts[6]) if len(parts) > 6 else None,
                "mri":           float(parts[7]) if len(parts) > 7 else None,
            })
        except (ValueError, IndexError):
            continue

    return rows
