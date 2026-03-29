"""Module for generating clean synthetic big data without anomalies in a Hive-partitioned structure."""
import os
from datetime import datetime
import pandas as pd
import numpy as np


def generate_clean_big_data():
    """Generates clean sales and customer data (no anomalies) using Hive-style partitioning."""
    # Configuration
    base_date = "2024-03-29"
    # Unified landing zone
    landing_zone = "data/minio_data/landing-zone"
    
    # Hive-style partitioning
    vendas_path = f"{landing_zone}/erp_vendas/dt={base_date}"
    clientes_path = f"{landing_zone}/crm_clientes/dt={base_date}"

    # Create directories
    os.makedirs(vendas_path, exist_ok=True)
    os.makedirs(clientes_path, exist_ok=True)

    # 1. Generate clean crm_clientes (~100k rows)
    n_customers = 100_000
    print(f"🚀 Generating {n_customers} customers (CLEAN)...")
    customers = {
        "customer_id": range(1000, 1000 + n_customers),
        "name": [f"Customer_{i}" for i in range(n_customers)],
        "email": [f"customer_{i}@example.com" for i in range(n_customers)],
        "region": np.random.choice(["North", "South", "East", "West", "Central"], n_customers),
        "created_at": [datetime.strptime(base_date, "%Y-%m-%d") for _ in range(n_customers)]
    }
    df_customers = pd.DataFrame(customers)
    # Keeping JSON for consistency with the flow
    df_customers.to_json(f"{clientes_path}/clientes.json", orient="records", lines=True)

    # 2. Generate clean erp_vendas (~1M rows)
    n_vendas = 1_000_000
    print(f"🚀 Generating {n_vendas} sales records (CLEAN)...")
    vendas = {
        "order_id": range(1, n_vendas + 1),
        "customer_id": np.random.randint(1000, 1000 + n_customers, size=n_vendas), # All valid IDs
        "amount": np.random.uniform(10.0, 5000.0, size=n_vendas), # All positive and realistic
        "category": np.random.choice(["Electronics", "Home", "Fashion", "Grocery", "Garden"], n_vendas),
        "timestamp": [datetime.strptime(base_date, "%Y-%m-%d") for _ in range(n_vendas)]
    }
    df_vendas = pd.DataFrame(vendas)

    # No anomalies inserted here - this is "Good Data"
    print("✨ Data generated without anomalies.")

    df_vendas.to_csv(f"{vendas_path}/vendas.csv", index=False)

    print(f"✅ Clean Big Data generated successfully in: {landing_zone}")


if __name__ == "__main__":
    generate_clean_big_data()
