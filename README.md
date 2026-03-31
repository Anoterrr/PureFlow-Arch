# 🌀 PureFlow-Arch: Medallion Lakehouse with Data Gatekeeper

**PureFlow-Arch** is a high-performance data engineering platform designed to ensure data integrity and quality in a local Lakehouse environment. The project implements the **Medallion Architecture** pattern (Bronze, Silver, Gold) with an active **Gatekeeper** (Circuit Breaker), utilizing the modern Python ecosystem.

---

## 🏗️ Architecture and Data Flow

The project operates in an isolated environment via **Docker** on **Arch WSL**, simulating a real corporate data pipeline. The image below details how the components interact:

![PureFlow-Arch Architecture](docs/pureflow_architecture.png)

### The Flow in Detail:

1.  **Landing Zone (MinIO/S3):** The entry point. Raw files (CSV/JSON) are received via a simulated S3 API provided by MinIO.
2.  **Ingestion (DuckDB):** DuckDB reads files directly from S3 (via the `httpfs` extension), converting them to **Parquet**.
3.  **Bronze Layer (Raw):** Efficient storage of Parquet files, maintaining full source fidelity (no transformations).
4.  **Gatekeeper (Great Expectations):** The trust layer. Validates types, nulls, and business rules. If the data fails the "Expectations", the pipeline is interrupted and the file is moved to `/quarantine`.
5.  **Silver Layer (Cleansed):** Validated and normalized data, persisted in **Delta Lake** format, ensuring ACID transactions and versioning (Time Travel).
6.  **Gold Layer (Curated):** Final transformations and business aggregations executed by **dbt**. The final product is materialized into analytical tables within the DuckDB `.db` file, ready for consumption.

---

## 🛠️ Technology Stack

| Component | Technology | Main Role |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow | Coordinates task scheduling and execution (DAGs). |
| **Data Engine** | DuckDB | High-performance in-process OLAP processing. |
| **Transformation** | dbt (duckdb-adapter) | Manages lineage and SQL models. |
| **Quality** | Great Expectations | Data contract validation (Gatekeeper). |
| **Storage** | MinIO + Delta Lake | S3-compatible storage and high-performance tables. |
| **Environment** | Docker + Poetry | Infrastructure isolation and dependency management. |

---

## 🚀 How to Run the Project

### Prerequisites
* Docker & Docker Compose installed.
* WSL2 (Environment tested: Arch Linux).
* Poetry (v2.0+) installed on host (Arch).

### 1. Preparation
In your Arch WSL terminal, prepare permissions and the environment:
```bash
# Create volume and documentation folders
mkdir -p data/minio_data docs
touch README.md
poetry lock
```

### 2. Initialization
Spin up the services defined in docker-compose.yml:

```bash
docker-compose up -d --build
```

### 3. Access the Interfaces
* Airflow UI: http://localhost:8080 (user: admin / pass: admin)
* MinIO Console: http://localhost:9001 (configured via STORAGE_USER and STORAGE_PASSWORD in .env)
* Data Docs (Quality Reports): Located at gx/uncommitted/data_docs/local_site/index.html

---

### 🛡️ The Differentiator: Gatekeeper in Action
Unlike common ETL pipelines, PureFlow-Arch focuses on observability. During execution:
* If corrupted data (e.g., negative sales value) tries to enter Silver, Great Expectations detects the anomaly.
* Airflow receives the failure signal and prevents dbt from processing the Gold layer with incorrect data.
* The data engineer receives an alert and can consult the HTML Data Doc to see exactly which row and column caused the error.

---

### 📄 License
This project is under the MIT license. See the LICENSE file for more details.
