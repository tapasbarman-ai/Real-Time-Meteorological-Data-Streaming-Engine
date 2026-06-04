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