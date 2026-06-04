# 📡 SondeFlow: Real-Time Radiosonde Telemetry Pipeline

SondeFlow is a real-time meteorological data streaming and processing pipeline. It showcases the integration of **Apache Kafka** as a distributed message broker and **Apache Flink (via PyFlink)** for stream processing, all containerized using **Docker**.

The primary data flow streams telemetry from atmospheric weather balloons (radiosondes), merges ground-truth boundary layer data with flight simulation metrics in real-time, publishes it to Kafka, and processes it downstream. A secondary learning playground tracks real-time driver/rider location updates.

---

## 🏗️ Architecture & Data Flow