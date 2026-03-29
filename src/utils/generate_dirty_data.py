"""Module for generating synthetic big data with intentional anomalies for the Medallion architecture."""

import os
from datetime import datetime
import pandas as pd
import numpy as np


def generate_dirty_big_data():
    """Generates partitioned sales and customer data with anomalies for testing scale and quality."""
    # Configuration
    base_date = "2024-03-29"
    landing_zone = "data/landing-zone"
    vendas_path = f"{landing_zone}/erp_vendas/{base_date}"
    clientes_path = f"{landing_zone}/crm_clientes/{base_date}"

    # Create directories
    os.makedirs(vendas_path, exist_ok=True)
    os.makedirs(clientes_path, exist_ok=True)

    # 1. Generate crm_clientes (~100k rows)
    n_customers = 100_000
    print(f"🚀 Generating {n_customers} customers...")
    customers = {
        "customer_id": range(1000, 1000 + n_customers),
        "name": [f"Customer_{i}" for i in range(n_customers)],
        "email": [f"customer_{i}@example.com" for i in range(n_customers)],
        "region": np.random.choice(
            ["North", "South", "East", "West", "Central"], n_customers
        ),
    }
    df_customers = pd.DataFrame(customers)
    df_customers.to_json(f"{clientes_path}/clientes.json", orient="records", lines=True)

    # 2. Generate erp_vendas (~1M rows)
    n_vendas = 1_000_000
    print(f"🚀 Generating {n_vendas} sales records...")
    vendas = {
        "order_id": range(1, n_vendas + 1),
        "customer_id": np.random.randint(1000, 1000 + n_customers + 500, size=n_vendas),
        "amount": np.random.uniform(5.0, 2000.0, size=n_vendas),
        "category": np.random.choice(
            ["Electronics", "Home", "Fashion", "Grocery", "Garden"], n_vendas
        ),
        "timestamp": [
            datetime.strptime(base_date, "%Y-%m-%d") for _ in range(n_vendas)
        ],
    }
    df_vendas = pd.DataFrame(vendas)

    # 3. Inserting Anomalies (for the Gatekeeper/Great Expectations)
    print("⚠️ Inserting intentional anomalies for quality testing...")
    # Negative amounts (Error)
    df_vendas.loc[0:500, "amount"] = -50.0
    # Null customers (Error)
    df_vendas.loc[1000:1500, "customer_id"] = np.nan
    # Massive outliers (Anomaly)
    df_vendas.loc[2000:2100, "amount"] = 99_999_999.0

    df_vendas.to_csv(f"{vendas_path}/vendas.csv", index=False)

    print(f"✅ Big Data generated successfully in: {landing_zone}")


if __name__ == "__main__":
    generate_dirty_big_data()
