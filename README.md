#  StreamShield – Scalable Real-Time Network Anomaly Detection Platform  

**StreamShield** is a **scalable , containerized streaming pipeline** for real-time intrusion detection.  
It ingests network traffic data, applies **machine learning–based anomaly detection**, and visualizes results through **interactive Kibana dashboards** — all designed for **scalability and low-latency analytics**.  

![Kibana Dashboard](https://github.com/Siya016/StreamShield-/blob/main/src/assets/kibana.jpg)  

---

## 🚀 Key Features  
- **Real-Time Streaming** – Processes live network traffic via **TCP/IP socket** and **Spark Structured Streaming**.  
- **ML-Powered Detection** – Integrates an **Isolation Forest model** trained on the [Network Intrusion Detection Dataset](https://www.kaggle.com/datasets/sampadab17/network-intrusion-detection) to classify traffic as normal or anomalous.  
- **Scalable Messaging** – Uses **Apache Kafka** for distributed, fault-tolerant event streaming.  
- **Low-Latency Analytics** – Streams enriched events into **Elasticsearch** via Kafka Connect for instant search and visualization.  
- **Security Dashboards** – Real-time anomaly insights and trend visualizations in **Kibana**.  
- **Fully Containerized** – Deployed via **Docker Compose** (Zookeeper, Kafka, Spark master/worker, Kafka Connect, Elasticsearch, Kibana) for easy portability and scaling.  

---

## 🏗 System Architecture  

``` plaintext
          ┌────────────────────────┐
          │  Network Intrusion Data │
          └────────────┬───────────┘
                       │ (TCP/IP Socket)
                       ▼
         ┌───────────────────────────────┐
         │ Spark Structured Streaming    │
         │  - Isolation Forest Inference │
         └────────────┬──────────────────┘
                      │ (Enriched Events)
                      ▼
             ┌─────────────────┐
             │  Apache Kafka    │
             └───────┬─────────┘
                     │ (Kafka Connect)
                     ▼
          ┌────────────────────────┐
          │    Elasticsearch       │
          └───────────┬────────────┘
                      │
                      ▼
           ┌───────────────────────┐
           │       Kibana           │
           │ Real-Time Dashboards   │
           └───────────────────────┘



📊 Use Cases
Network Intrusion Detection for SOC teams


Real-Time Monitoring in high-volume data systems

📦 Tech Stack


Data Processing: Apache Spark Structured Streaming

Messaging: Apache Kafka, Zookeeper

Machine Learning: Isolation Forest (scikit-learn)

Storage & Search: Elasticsearch

Visualization: Kibana

Containerization: Docker & Docker Compose

⚡ Getting Started


1️⃣ Clone the repository

git clone https://github.com/Siya016/StreamShield-.git
cd StreamShield-


2️⃣ Start the Docker services

docker-compose up --build

3️⃣ Stream the dataset over TCP Socket

python socket_stream.py

4️⃣ View Kibana Dashboard
Open http://localhost:5601 and explore the anomaly detection dashboard.


