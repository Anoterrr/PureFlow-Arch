import pandas as pd
import numpy as np


def generate_dirty_data():
    n_rows = 1000
    data = {
        "transaction_id": range(1, n_rows + 1),
        "customer_id": np.random.randint(100, 999, size=n_rows),
        "amount": np.random.uniform(10.0, 500.0, size=n_rows),
        "timestamp": pd.date_range(start="2026-01-01", periods=n_rows, freq="H"),
    }

    df = pd.DataFrame(data)

    # Inserindo "Sujeira" (Anomalias)
    df.loc[0:10, "amount"] = -99.0  # Erro: Valor negativo
    df.loc[20:30, "customer_id"] = np.nan  # Erro: Valores nulos
    df.loc[50:60, "amount"] = 999999.0  # Erro: Outlier absurdo

    df.to_csv("data/raw_external_data.csv", index=False)
    print("✅ Dataset 'sujo' gerado em data/raw_external_data.csv")


if __name__ == "__main__":
    generate_dirty_data()
