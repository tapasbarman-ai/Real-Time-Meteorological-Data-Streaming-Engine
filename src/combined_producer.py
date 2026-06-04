
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


# =========================================================
# PARSE RADIOSONDE SIM FILE
# =========================================================
def parse_sim_file(filepath):
    """
    Parse the tab-separated radiosonde_sim file.
    Returns list of dicts, one per data row.
    """
    with open(filepath, "r", encoding="latin-1") as f:
        content = f.read()

    lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    # First line is the header
    header_cols = lines[0].split("\t")

    rows = []
    for i in range(1, len(lines)):
        stripped = lines[i].strip()
        if not stripped:
            continue

        values = lines[i].split("\t")
        if len(values) < len(header_cols):
            continue

        row = {}
        for col, val in zip(header_cols, values):
            val = val.strip()
            if val == "" or val == "N/A":
                row[col] = None
                continue
            # Try numeric conversion
            try:
                if "." in val:
                    row[col] = float(val)
                else:
                    row[col] = int(val)
            except ValueError:
                row[col] = val   # keep as string (e.g. phase, manufacturing_lot)
        rows.append(row)

    return rows


# =========================================================
# MERGE ONE BOUNDARY ROW + ONE SIM ROW → UNIFIED MESSAGE
# =========================================================
def merge_row(header, bl_row, sim_row):
    """
    Combine boundary-layer header, boundary-layer data row,
    and radiosonde_sim data row into a single flat message.

    Boundary-layer values take priority for overlapping measurements
    (temp, pressure, humidity, wind) since they are the 'ground truth'
    from the altreport file.  Sim fields fill in everything else.
    """
    msg = {}

    # 1. Static header from boundary-layer file
    msg.update(header)

    # 2. All sim columns (renamed to match existing DB schema where needed)
    msg["profile"]           = sim_row.get("profile")
    msg["frame_counter"]     = sim_row.get("frame_counter")
    msg["ascent_rate_ms"]    = sim_row.get("ascent_rate_ms")
    msg["lat_deg"]           = sim_row.get("latitude")
    msg["lon_deg"]           = sim_row.get("longitude")
    msg["alt_m"]             = sim_row.get("altitude_m")
    msg["vn_ms"]             = sim_row.get("vn_ms")
    msg["ve_ms"]             = sim_row.get("ve_ms")
    msg["vu_ms"]             = sim_row.get("vu_ms")
    msg["battery_mv"]        = sim_row.get("battery_mv")
    msg["tx_ma"]             = sim_row.get("tx_ma")
    msg["system_temp_c"]     = sim_row.get("system_temp_c")
    msg["sats_used"]         = sim_row.get("sats_used")
    msg["fix_type"]          = sim_row.get("fix_type")
    msg["pdop"]              = sim_row.get("pdop")
    msg["hdop"]              = sim_row.get("hdop")
    msg["vdop"]              = sim_row.get("vdop")
    msg["gps_week"]          = sim_row.get("gps_week")
    msg["tow_ms"]            = sim_row.get("tow_ms")
    msg["cal_version"]       = sim_row.get("cal_version")
    msg["pressure_offset"]   = sim_row.get("pressure_offset")
    msg["temp_offset"]       = sim_row.get("temp_offset")
    msg["humidity_offset"]   = sim_row.get("humidity_offset")
    msg["rf_tune_id"]        = sim_row.get("rf_tune_id")
    msg["manufacturing_lot"] = sim_row.get("manufacturing_lot")
    msg["raw_rssi_dbm"]      = sim_row.get("raw_rssi_dbm")
    msg["raw_snr_db"]        = sim_row.get("raw_snr_db")
    msg["phase"]             = sim_row.get("phase")

    # Sim also has temperature_c, pressure_hpa, relative_humidity_pct,
    # wind_dir_deg, wind_speed_ms — store them as sim_* for reference
    msg["sim_temperature_c"]       = sim_row.get("temperature_c")
    msg["sim_pressure_hpa"]        = sim_row.get("pressure_hpa")
    msg["sim_humidity_pct"]        = sim_row.get("relative_humidity_pct")
    msg["sim_wind_dir_deg"]        = sim_row.get("wind_dir_deg")
    msg["sim_wind_speed_ms"]       = sim_row.get("wind_speed_ms")

    # 3. Boundary-layer measurements (ground truth, overwrites sim duplicates)
    msg["row_n"]         = bl_row["row_n"]
    msg["height_msl_m"]  = bl_row["height_msl_m"]
    msg["temp_c"]        = bl_row["temp_c"]
    msg["pressure_mb"]   = bl_row["pressure_mb"]
    msg["rh_pct"]        = bl_row["rh_pct"]
    msg["wind_dir_deg"]  = bl_row["wind_dir_deg"]
    msg["wind_speed_ms"] = bl_row["wind_speed_ms"]
    msg["mri"]           = bl_row["mri"]

    return msg


# =========================================================
# READ BOTH FILES
# =========================================================
# --- Boundary Layer ---
if not os.path.isfile(BOUNDARY_FILE):
    logger.critical(f"❌ Boundary-layer file not found: {BOUNDARY_FILE}")
    sys.exit(1)

logger.info(f"📄 Boundary file : {BOUNDARY_FILE}")

with open(BOUNDARY_FILE, "r", encoding="latin-1") as f:
    bl_raw = f.read()

bl_lines    = bl_raw.replace("\r\n", "\n").replace("\r", "\n").split("\n")
bl_header   = parse_boundary_header(bl_lines)
bl_rows     = parse_boundary_data(bl_lines)

if not bl_header:
    logger.critical("❌ Could not parse boundary-layer header.")
    sys.exit(1)
if not bl_rows:
    logger.critical("❌ No data rows in boundary-layer file.")
    sys.exit(1)

logger.info(f"   Boundary rows  : {len(bl_rows)}")

# --- Radiosonde Sim ---
if not os.path.isfile(SIM_FILE):
    logger.critical(f"❌ Sim file not found: {SIM_FILE}")
    sys.exit(1)

logger.info(f"📄 Sim file      : {SIM_FILE}")

sim_rows = parse_sim_file(SIM_FILE)

if not sim_rows:
    logger.critical("❌ No data rows in sim file.")
    sys.exit(1)

logger.info(f"   Sim rows       : {len(sim_rows)}")

# --- Match by same row count ---
matched_count = min(len(bl_rows), len(sim_rows))
logger.info(f"✅ Matched rows   : {matched_count}")

logger.info("")
logger.info("📋 Static header fields:")
for k, v in bl_header.items():
    logger.info(f"   {k:<35} = {v}")


# =========================================================
# KAFKA PRODUCER
# =========================================================
prod_conf = producer_config.copy()
prod_conf.update({
    "client.id":         "radiosonde-combined-producer",
    "acks":              "all",
    "retries":           5,
})
producer = Producer(prod_conf)


def delivery_report(err, msg):
    if err:
        logger.error(f"❌ Kafka delivery failed: {err}")
    else:
        logger.debug(
            f"✔ Delivered → {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
        )


logger.info(f"📡 Kafka broker  : {KAFKA_BROKER}")
logger.info(f"📡 Kafka topic   : {KAFKA_TOPIC}")
logger.info(f"⏱️  Interval      : {SEND_INTERVAL_SEC}s per row")
logger.info("=================================================")
logger.info("🔄 Streaming combined data — Ctrl+C to stop")
logger.info("=================================================")


# =========================================================
# MAIN SEND LOOP — 1 merged row per second
# =========================================================
messages_sent = 0

# ── File to save every sent message ──────────────────────
RECEIVED_FILE = os.path.join(PROJECT_ROOT, "received.txt")
logger.info(f"📝 Saving sent data to: {RECEIVED_FILE}")

# Clear the file at start
with open(RECEIVED_FILE, "w", encoding="utf-8") as rf:
    rf.write(f"# Combined Producer — Sent Messages Log\n")
    rf.write(f"# Started at: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}\n")
    rf.write(f"# Total rows to send: {matched_count}\n")
    rf.write("=" * 80 + "\n\n")

try:
    for i in range(matched_count):
        bl_row  = bl_rows[i]
        sim_row = sim_rows[i]

        message = merge_row(bl_header, bl_row, sim_row)
        message["event_time"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        # Tag so consumers can identify the source
        message["source"] = "combined"

        payload = json.dumps(message, ensure_ascii=False).encode("utf-8")

        producer.produce(
            topic=KAFKA_TOPIC,
            key=bl_header.get("sonde_serial_number", "unknown"),
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)

        # ── Write to received.txt ─────────────────────────
        with open(RECEIVED_FILE, "a", encoding="utf-8") as rf:
            rf.write(f"--- Message #{messages_sent + 1} "
                     f"| {message['event_time']} ---\n")
            rf.write(json.dumps(message, indent=2, ensure_ascii=False))
            rf.write("\n\n")

        messages_sent += 1
        logger.info(
            f"📤 #{messages_sent:4d}/{matched_count} | "
            f"Row={bl_row['row_n']:5d} | "
            f"H={bl_row['height_msl_m']:8.1f}m | "
            f"T={bl_row['temp_c']}°C | "
            f"P={bl_row['pressure_mb']}mb | "
            f"Frame={sim_row.get('frame_counter')} | "
            f"Bat={sim_row.get('battery_mv')}mV | "
            f"Sats={sim_row.get('sats_used')} | "
            f"Phase={sim_row.get('phase')}"
        )

        time.sleep(SEND_INTERVAL_SEC)

except KeyboardInterrupt:
    logger.info("🛑 Stopped by user (Ctrl+C)")

except Exception as e:
    logger.exception(f"🔥 Fatal error: {e}")

finally:
    logger.info("⏳ Flushing remaining messages...")
    producer.flush()
    logger.info(f"✅ Done — {messages_sent} / {matched_count} messages sent")
