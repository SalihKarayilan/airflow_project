# Airflow ETL: MongoDB to PostgreSQL Data Transfer & Encryption Benchmarking

## 📖 Introduction
This project is a robust Data Engineering pipeline orchestrated by **Apache Airflow**, designed to measure and benchmark the performance overhead of data encryption during large-scale ETL processes. It simulates migrating raw customer records (1M, 10M, and 50M rows) from **MongoDB** to **PostgreSQL**. 

The project evaluates two main scenarios for academic and analytical purposes:
1. **Baseline Transfer:** Direct, unmasked data ingestion to measure raw I/O throughput and baseline memory usage.
2. **Encrypted Transfer:** On-the-fly data encryption using **Fernet (AES-128)** to measure the exact computational cost (time and RAM) of securing sensitive PII data during transit.

## 🏗️ Architecture & Key Features
- **Automated Data Generation:** Includes seed DAGs that use the `Faker` library (Turkish locale) to populate MongoDB with 1M, 10M, or 50M synthetic customer records.
- **Dynamic Scaling:** Custom DAGs optimized for handling different data volume tiers seamlessly with dynamic batch sizing (default 10,000 rows/batch).
- **Performance Monitoring:** In-built `psutil` tracking logs total elapsed time, rows processed per second (throughput), and exact RAM consumption differences directly into Airflow logs.
- **Fully Containerized:** The entire infrastructure (Airflow LocalExecutor, MongoDB source, PostgreSQL target) is containerized via Docker Compose for one-click deployment.

## 🛠️ Tech Stack
- **Orchestration:** Apache Airflow 2.10.4 (LocalExecutor)
- **Source Database:** MongoDB
- **Target Database:** PostgreSQL 13
- **Language & Libraries:** Python 3, `psycopg2`, `faker`, `cryptography` (Fernet), `psutil`
- **Infrastructure:** Docker & Docker Compose

## 📋 Prerequisites
Before you begin, ensure you have the following installed:
- **Docker** and **Docker Compose** (for containerized execution)
- Git

## ⚙️ Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/SalihKarayilan/airflow_project.git
   cd airflow_project
   ```

2. **Start the Infrastructure:**
   Ensure Docker is running, then execute the following command to spin up Airflow, MongoDB, and PostgreSQL containers:
   ```bash
   docker-compose up -d
   ```
   This will build and start all required services, including initializing the Airflow database and creating an Admin user (`admin` / `admin`).

3. **Access Airflow UI:**
   Once the containers are up, access the Airflow web interface at:
   - **URL:** `http://localhost:8085`
   - **Username:** `admin`
   - **Password:** `admin`

## 🔄 DAG Overview
The project contains multiple Airflow DAGs organized into logical groups based on their function:

- **Seed DAGs:** Populate the MongoDB database with synthetic records.
  - `seed_1M_mongodb_dag`
  - `seed_10M_mongodb_dag`
  - `seed_50M_mongodb_dag`

- **Baseline Unmasked Transfer DAGs:** Transfer unmasked data directly to PostgreSQL.
  - `mongo_to_postgres_baseline_unmasked_1m`
  - `mongo_to_postgres_baseline_unmasked_10m`
  - `mongo_to_postgres_baseline_unmasked_50m`

- **Encrypted Transfer DAGs:** Transfer data and apply Fernet encryption on sensitive fields before inserting into PostgreSQL.
  - `mongo_to_postgres_encrypted_1M`
  - `mongo_to_postgres_encrypted_10M`
  - `mongo_to_postgres_encrypted_50M`

## 📊 Benchmarking Details
Each transfer DAG includes an initial monitoring step that calculates baseline resource usage. As the extraction, encryption (if applicable), and loading occurs, `psutil` continuously measures RAM usage. Upon task completion, total elapsed time, average throughput, and RAM differences are recorded in the task logs. This allows for rigorous comparison and reporting on the impact of on-the-fly encryption on an ETL workload.
