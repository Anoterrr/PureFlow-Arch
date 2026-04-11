# PureFlow-Arch: Modern Data Lakehouse
### Senior Capstone Project (TCC) - Medallion Architecture

**PureFlow-Arch** is a high-performance, modular data engineering framework designed to demonstrate the "Modern Data Lakehouse" paradigm. It implements a full Medallion Architecture using a containerized stack focused on scalability, data quality, and ACID-like properties in a local environment.

---

## 🏗️ Architecture Overview

The project follows the **Medallion Architecture**, ensuring data evolves through progressive layers of cleanliness and complexity:

1.  **Landing (Raw):** Immutable storage of source files (CSV/JSON) in MinIO.
2.  **Bronze (Ingested):** Raw data converted to compressed Parquet with technical metadata (`_ingested_at`, `_source_file`).
3.  **Silver (Filtered & Cleaned):** 
    *   **Gatekeeper 1:** Great Expectations (GX) validates raw Bronze quality.
    *   **Business Rules:** Data is filtered and standardized via DuckDB transformations.
    *   **Gatekeeper 2:** GX validates post-transformation schema and business constraints.
4.  **Gold (Analytical):** Final materialized summary tables in a local DuckDB file (`pureflow_lakehouse.db`) via dbt-style modeling.

---

## 🛠️ Tech Stack

*   **Orchestration:** Apache Airflow (LocalExecutor)
*   **Storage (S3):** MinIO (Landing, Bronze, Silver buckets)
*   **Processing Engine:** DuckDB (High-performance OLAP)
*   **Data Quality:** Great Expectations (GX)
*   **Transformation Layer:** dbt (DuckDB adapter)
*   **Environment:** Docker & Poetry (Python 3.11)

---

## 📂 Project Structure

```text
PureFlow-Arch/
├── dags/                   # Airflow DAGs (Refined Medallion Flow)
├── dbt/                    # dbt Project for Gold Layer modeling
├── src/
│   ├── core/               # Shared connections and logging logic
│   ├── ingestion/          # Domain-driven ingestion (Sales)
│   ├── quality/            # Multi-stage GX validation rules
│   ├── transformations/    # Bronze-to-Silver & Silver-to-Gold logic
│   └── utils/              # Data generators and helpers
├── data/                   # Local volumes for MinIO and Gold DB
└── scripts/                # Infrastructure & permission setup
```

---

## 🚀 Getting Started

### 1. Prerequisites
*   Docker & Docker Compose
*   (Optional) Poetry for local development

### 2. Setup Permissions (Arch/WSL)
To ensure the `analyst` user (UID 1000) inside the container can manage the volumes, run:
```bash
chmod +x scripts/setup_perms.sh
./scripts/setup_perms.sh
```

### 3. Spin up Infrastructure
```bash
docker-compose up -d --build
```

### 4. Trigger the Pipeline
*   **Airflow UI:** [http://localhost:8080](http://localhost:8080)
*   Unpause and trigger the `pureflow_sales_pipeline` DAG.

---

## 🛡️ Data Quality & Security

*   **Circuit Breaker:** If data fails Great Expectations validation at any stage (Bronze or Silver), the pipeline automatically halts and moves the offending data to a `quarantine/` folder.
*   **Auditability:** Every row in the lakehouse includes technical metadata for lineage tracking.
*   **Security-by-Design:** All processes run under a dedicated non-root `analyst` user. Credentials are managed via environment variables and `profiles.yml` jinja templates.

---

## 📊 Monitoring

| Tool | Endpoint |
| :--- | :--- |
| **Airflow** | [http://localhost:8080](http://localhost:8080) |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) |
| **GX Data Docs** | `gx/uncommitted/data_docs/local_site/index.html` |
| **Gold DB** | `data/pureflow_lakehouse.db` |

---
*Developed as a Senior Capstone Project in Data Engineering.*
