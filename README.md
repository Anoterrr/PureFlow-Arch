# PureFlow-Arch: Modern S3-Native Data Lakehouse
### Senior Capstone Project (TCC) - Medallion Architecture

**PureFlow-Arch** is a high-performance, **S3-Native** data engineering framework designed to demonstrate the "Modern Data Lakehouse" paradigm. It implements a full Medallion Architecture using a 100% open-source stack focused on scalability, data quality, and high resource efficiency, making it replicable even on modest hardware.

---

## 🏗️ Architecture Overview

The project follows the **Medallion Architecture**, ensuring data evolves through progressive layers of cleanliness and complexity, stored entirely in **S3 (MinIO)**:

1.  **Landing (Raw):** Immutable storage of source files (CSV/JSON) in MinIO.
2.  **Bronze (Ingested):** Raw data converted to compressed Parquet with technical metadata (`_ingested_at`, `_source_file`) using **DuckDB**.
3.  **Silver (Filtered & Cleaned):** 
    *   **Gatekeeper:** **Great Expectations (GX)** validates raw data quality.
    *   **Transformation:** Data is filtered and standardized via **dbt (DuckDB adapter)** and stored back to S3.
4.  **Gold (Business/Insights):** Final aggregated datasets materialized as **External Parquet Tables** on S3, partitioned by execution date, ready for BI and AI consumption.

---

## 🛠️ Tech Stack (100% Open Source)

*   **Orchestration:** **Dagster** (Asset-Based, Lightweight Modern Orchestrator)
*   **Storage (S3):** **MinIO** (Landing, Bronze, Silver, Gold, and Quarantine buckets)
*   **Processing Engine:** **DuckDB** (High-performance In-Memory OLAP)
*   **Data Quality:** **Great Expectations (GX)** (Integrated into Dagster via Asset Checks)
*   **Transformation Layer:** **dbt** (DuckDB adapter for S3-native modeling)
*   **Visualization (BI):** **Streamlit** (Real-time dashboard consuming Gold S3 data)
*   **Environment:** Docker & Poetry (Python 3.11)

---

## 📂 Project Structure

```text
PureFlow-Arch/
├── dbt/                    # dbt Project (Bronze -> Silver -> Gold S3 models)
├── src/
│   ├── orchestration.py    # Dagster Assets & Pipeline Definitions
│   ├── dashboard.py        # Streamlit BI Dashboard logic
│   ├── core/               # Shared S3 connections and logging logic
│   ├── ingestion/          # Domain-driven ingestion classes (Sales)
│   ├── validation/         # Great Expectations validation logic
│   └── quality/            # Domain-specific quality rules
├── data/
│   └── minio_data/         # Local volume for MinIO (S3 Buckets)
└── scripts/                # Infrastructure & permission setup
```

---

## 🚀 Getting Started

### 1. Prerequisites
*   Docker & Docker Compose
*   Python 3.11 (Optional for local development)

### 2. Setup Permissions
To ensure the `analyst` user (UID 1000) can manage the data volumes:
```bash
chmod +x scripts/setup_perms.sh
./scripts/setup_perms.sh
```

### 3. Spin up Infrastructure
```bash
docker-compose up -d --build
```
*This setup is optimized to run with ~1GB of RAM, significantly lighter than traditional Airflow stacks.*

### 4. Run the Pipeline
1.  Access the **Dagster UI** at [http://localhost:3000](http://localhost:3000).
2.  Navigate to **Assets** or **Jobs** and trigger the `full_pipeline_job`.
3.  Monitor the **Asset Checks** to see Great Expectations validation results in real-time.

---

## 🛡️ Data Quality & Observability

*   **Integrated Quality:** Great Expectations is embedded into the Dagster graph. Every run generates a visual **Data Doc** report.
*   **Circuit Breaker:** If data fails validation, the pipeline halts and moves offending data to the **S3 Quarantine bucket**.
*   **S3-Native:** No local databases. Every layer is stored in S3, ensuring the project reflects a real-world cloud architecture.

---

## 📊 Monitoring & BI

| Tool | Endpoint | Description |
| :--- | :--- | :--- |
| **Dagster** | [http://localhost:3000](http://localhost:3000) | Pipeline, Lineage, and Quality Checks |
| **dbt Docs** | [http://localhost:8081](http://localhost:8081) | Data Lineage and Metadata Documentation |
| **Streamlit BI** | [http://localhost:8501](http://localhost:8501) | Business Dashboard (Gold Layer Insights) |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | S3 Object Browser (Storage) |
| **GX Data Docs** | `gx/uncommitted/data_docs/local_site/index.html` | Detailed Quality Reports |

---
*Developed as a Senior Capstone Project in Data Engineering.*
