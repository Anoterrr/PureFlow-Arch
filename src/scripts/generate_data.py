"""Module for generating synthetic dirty data for testing purposes."""
import pandas as pd
import numpy as np


def generate_dirty_data():
    """Generates a CSV file with synthetic transactions containing intentional anomalies."""
    n_rows = 1000
    data = {
        "transaction_id": range(1, n_rows + 1),
        "customer_id": np.random.randint(100, 999, size=n_rows),
        "amount": np.random.uniform(10.0, 500.0, size=n_rows),
        "timestamp": pd.date_range(start="2026-01-01", periods=n_rows, freq="H"),
    }

    df = pd.DataFrame(data)

    # Inserting "Dirty" data (Anomalies)
    df.loc[0:10, "amount"] = -99.0  # Error: Negative value
    df.loc[20:30, "customer_id"] = np.nan  # Error: Null values
    df.loc[50:60, "amount"] = 999999.0  # Error: Extreme outlier

    df.to_csv("data/raw_external_data.csv", index=False)
    print("✅ 'Dirty' dataset generated at data/raw_external_data.csv")


if __name__ == "__main__":
    generate_dirty_data()
