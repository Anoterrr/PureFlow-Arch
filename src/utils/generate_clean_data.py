"""Module for generating clean synthetic big data in Hive-partitioned structure."""
from datetime import datetime
import pandas as pd
import numpy as np
from core.config import get_paths, BASE_DATE


def generate_clean_big_data():
    """Generates clean sales and customer data using Hive-style partitioning."""
    # Configuration and directory creation
    vendas_path, clientes_path = get_paths()

    # 1. Generate clean crm_clientes (~100k rows)
    n_customers = 100_000
    print(f"🚀 Generating {n_customers} customers (CLEAN)...")
    customers = {
        "customer_id": range(1000, 1000 + n_customers),
        "name": [f"Customer_{i}" for i in range(n_customers)],
        "email": [f"customer_{i}@example.com" for i in range(n_customers)],
        "region": np.random.choice(
            ["North", "South", "East", "West", "Central"], n_customers
        ),
        "created_at": [
            datetime.strptime(BASE_DATE, "%Y-%m-%d") for _ in range(n_customers)
        ]
    }
    df_customers = pd.DataFrame(customers)
    # Keeping JSON for consistency with the flow
    df_customers.to_json(f"{clientes_path}/clientes.json", orient="records", lines=True)

    # 2. Generate clean erp_vendas (~1M rows)
    n_vendas = 1_000_000
    print(f"🚀 Generating {n_vendas} sales records (CLEAN)...")
    vendas = {
        "order_id": range(1, n_vendas + 1),
        "customer_id": np.random.randint(1000, 1000 + n_customers, size=n_vendas),
        "amount": np.random.uniform(10.0, 5000.0, size=n_vendas),
        "category": np.random.choice(
            ["Electronics", "Home", "Fashion", "Grocery", "Garden"], n_vendas
        ),
        "timestamp": [
            datetime.strptime(BASE_DATE, "%Y-%m-%d") for _ in range(n_vendas)
        ]
    }
    df_vendas = pd.DataFrame(vendas)

    # No anomalies inserted here - this is "Good Data"
    print("✨ Data generated without anomalies.")

    df_vendas.to_csv(f"{vendas_path}/vendas.csv", index=False)
    print(f"✅ Clean Big Data generated successfully in: {vendas_path}")


if __name__ == "__main__":
    generate_clean_big_data()
