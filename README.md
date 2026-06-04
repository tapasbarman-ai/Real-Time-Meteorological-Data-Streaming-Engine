# 📡 SondeFlow: Real-Time Radiosonde Telemetry Pipeline

SondeFlow is a real-time meteorological data streaming and processing pipeline. It showcases the integration of **Apache Kafka** as a distributed message broker and **Apache Flink (via PyFlink)** for stream processing, all containerized using **Docker**.

The primary data flow streams telemetry from atmospheric weather balloons (radiosondes), merges ground-truth boundary layer data with flight simulation metrics in real-time, publishes it to Kafka, and processes it downstream. A secondary learning playground tracks real-time driver/rider location updates.

---

## 🏗️ Architecture & Data Flow

```mermaid
graph TD
    subgraph Local Data Sources
        BL["Boundary Layer Data<br/>boundary_layer_data_altreport.txt"]
        SIM["Flight Simulation Data<br/>radiosonde_sim_RS92SGP.txt"]
    end

    subgraph Producers (Python Host)
        P_Comb["Combined Producer<br/>combined_producer.py"]
        P_Rider["Rider Producer<br/>producer.py"]
    end

    subgraph Streaming Broker (Docker)
        K_Broker[Kafka Broker]
        T_Sonde[Topic: radio-sonde]
        T_Rider[Topic: rider-updates]
    end

    subgraph Processing Cluster (Docker)
        F_JM[Flink JobManager]
        F_TM[Flink TaskManager]
        PyFlink_C["PyFlink Consumer<br/>flink_consumer.py"]
    end

    subgraph Output Dest
        Log["Received Message Log<br/>received.txt"]
        Cons[Stdout / Print Sink]
    end

    BL --> P_Comb
    SIM --> P_Comb
    P_Comb --> T_Sonde
    P_Comb --> Log

    P_Rider --> T_Rider
    T_Rider --> PyFlink_C
    F_JM <--> F_TM
    PyFlink_C <--> F_JM
    PyFlink_C --> Cons
```

---

## 📂 Project Structure

```text
c:/Users/tb619/Videos/Projects/kafka/
├── data/                       # Raw meteorological logs and flight simulations
│   ├── boundary_layer_data_altreport_20220328_1431.txt
│   └── radiosonde_sim_RS92SGP_20220328.txt
├── docker/                     # Docker configuration files
│   ├── Dockerfile              # Basic Python app container config
│   └── Dockerfile.flink        # Custom PyFlink container (with python3 & apache-flink dependency)
├── jobs/                       # Flink stream processing jobs
│   ├── flink-sql-connector-kafka-1.17.1.jar  # Kafka connector dependency
│   ├── flink_consumer.py       # PyFlink stream consumer from Kafka
│   ├── test.py                 # PyFlink testing playground (maps, windows, watermarks)
│   └── word_count.py           # Standard Flink word-count script
├── scripts/
│   └── flink_setup             # Script & instructions to run/deploy Flink containers
├── src/                        # Data producers & admin scripts (Host/Client side)
│   ├── producers/              # Isolated location producers (North, South, Random)
│   ├── admin.py                # AdminClient script to programmatically create Kafka topics
│   ├── client.py               # Shared Kafka bootstrap server & client configurations
│   ├── combined_producer.py    # Integrates, parses, and streams the radiosonde telemetry
│   ├── consumer.py             # Simple console consumer for rider-updates
│   └── producer.py             # CLI driver for producing manual rider-updates
├── docker-compose.yml          # Spins up Zookeeper, Kafka, and the Flink clusters
├── requirements.txt            # Local Python dependencies
└── received.txt                # Log of streamed combined producer payloads
```

---

## 🚀 Getting Started

### Prerequisites
Make sure you have the following installed on your system:
- **Docker & Docker Compose**
- **Python 3.11+**

---

### Step 1: Start the Infrastructure
Spin up Zookeeper, Kafka, Flink JobManager, and Flink TaskManager:
```bash
docker-compose up -d
```
Verify they are running by checking `docker ps`. You can access the **Flink Web Dashboard** at `http://localhost:8081`.

---

### Step 2: Build the PyFlink Container
We need a custom Flink image equipped with Python 3 and PyFlink libraries to execute our stream-processing jobs:
```bash
# Build the image
docker build -f docker/Dockerfile.flink --no-cache -t pyflink:latest .

# Run the PyFlink interactive container mounting the jobs folder
docker run -it -v ${PWD}/jobs:/app/jobs pyflink:latest /bin/bash
```

---

### Step 3: Run the Telemetry Streams

#### Option A: Stream Combined Radiosonde Data
Run the combined producer locally on your host environment to parse weather balloon logs and stream them to Kafka:
```bash
# Setup virtual environment and dependencies
python -m venv .venv
source .venv/bin/activate  # Or .venv\Scripts\activate on Windows
pip install -r requirements.txt

# Run the combined producer
python src/combined_producer.py
```
This will parse the files inside `data/`, merge rows in real-time, stream them to Kafka under topic `radio-sonde`, and log them to `received.txt`.

#### Option B: Stream Manual Rider Updates
1. Programmatically create the `rider-updates` topic:
   ```bash
   python src/admin.py
   ```
2. Start the interactive producer:
   ```bash
   python src/producer.py
   ```
   *Example input:* `Bob,north` or `Alice,south`

---

### Step 4: Run Flink Stream Processing
Inside your running `pyflink` Docker container, start the stream consumer to ingest the data from Kafka in real-time:
```bash
python3 /app/jobs/flink_consumer.py
```
This job connects to the Flink engine, loads the Kafka connector JAR, subscribes to the Kafka topic, and prints the incoming messages to standard output.

---

## 🛠️ PyFlink Playground & Testing
If you are learning Flink, check out [jobs/test.py](file:///c:/Users/tb619/Videos/Projects/kafka/jobs/test.py). It includes standalone exercises demonstrating:
1. **Map/Filter transformations** on streams.
2. **Keyed Streams** (`key_by`).
3. **Watermarking** for out-of-orderness.
4. **Window processing** (Tumbling Event-Time Windows).
5. **Process Functions** (applying custom logic/state management).
