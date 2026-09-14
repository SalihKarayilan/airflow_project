# Airflow ETL: MongoDB to PostgreSQL Data Transfer & Encryption Benchmarking

This project is a robust Data Engineering pipeline orchestrated by **Apache Airflow**, designed to measure and benchmark the performance overhead of data encryption during large-scale ETL processes. It simulates migrating raw customer records (1M, 10M, and 50M rows) from **MongoDB** to **PostgreSQL**. 

The project evaluates two main scenarios for academic and analytical purposes:
1. **Baseline Transfer:** Direct, unmasked data ingestion to measure raw I/O throughput and baseline memory usage.
2. **Encrypted Transfer:** On-the-fly data encryption using **Fernet (AES-128)** to measure the exact computational cost (time and RAM) of securing sensitive PII data during transit.

## 🚀 Key Features

* **Automated Data Generation:** Includes seed DAGs that use the `Faker` library (Turkish locale) to populate MongoDB with 1M, 10M, or 50M synthetic customer records.
* **Dynamic Scaling:** Custom DAGs optimized for handling different data volume tiers seamlessly with dynamic batch sizing (default 10,000 rows/batch).
* **Performance Monitoring:** In-built `psutil` tracking logs total elapsed time, rows processed per second (throughput), and exact RAM consumption differences directly into Airflow logs.
* **Fully Containerized:** The entire infrastructure (Airflow LocalExecutor, MongoDB source, PostgreSQL target) is containerized via Docker Compose for one-click deployment.

## 🛠️ Tech Stack

* **Orchestration:** Apache Airflow 2.10.4 (LocalExecutor)
* **Source Database:** MongoDB
* **Target Database:** PostgreSQL 13
* **Language & Libraries:** Python 3, `psycopg2`, `faker`, `cryptography` (Fernet), `psutil`
* **Infrastructure:** Docker & Docker Compose

## ⚙️ Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/SalihKarayilan/airflow_project.git](https://github.com/SalihKarayilan/airflow_project.git)
   cd airflow_project