"""Module for generating synthetic big data with intentional anomalies."""
from datetime import datetime
import pandas as pd
import numpy as np
from core.config import get_paths, BASE_DATE


def generate_dirty_big_data():
    """Generates partitioned sales and customer data with anomalies."""
    # Configuration and directory creation
    vendas_path, clientes_path = get_paths()

    # 1. Generate crm_clientes (~100k rows)
    n_customers = 100_000
    print(f"🚀 Generating {n_customers} customers (DIRTY)...")
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
    print(f"🚀 Generating {n_vendas} sales records (DIRTY)...")
    vendas = {
        "order_id": range(1, n_vendas + 1),
        "customer_id": np.random.randint(
            1000, 1000 + n_customers + 500, size=n_vendas
        ),
        "amount": np.random.uniform(5.0, 2000.0, size=n_vendas),
        "category": np.random.choice(
            ["Electronics", "Home", "Fashion", "Grocery", "Garden"], n_vendas
        ),
        "timestamp": [
            datetime.strptime(BASE_DATE, "%Y-%m-%d") for _ in range(n_vendas)
        ],
    }
    df_vendas = pd.DataFrame(vendas)

    # 3. Inserting Anomalies
    print("⚠️ Inserting intentional anomalies for quality testing...")
    df_vendas.loc[0:500, "amount"] = -50.0
    df_vendas.loc[1000:1500, "customer_id"] = np.nan
    df_vendas.loc[2000:2100, "amount"] = 99_999_999.0

    df_vendas.to_csv(f"{vendas_path}/vendas.csv", index=False)
    print(f"✅ Dirty Big Data generated successfully in: {vendas_path}")


if __name__ == "__main__":
    generate_dirty_big_data()
