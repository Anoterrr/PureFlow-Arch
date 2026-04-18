"""Shared utility functions for synthetic data generation."""

import numpy as np


def generate_base_customers(n_customers):
    """Generates a base dictionary for customer data."""
    return {
        "id": range(1000, 1000 + n_customers),
        "nome": [f"Customer_{i}" for i in range(n_customers)],
        "email": [f"customer_{i}@example.com" for i in range(n_customers)],
        "cidade": np.random.choice(
            [
                "São Paulo",
                "Rio de Janeiro",
                "Belo Horizonte",
                "Curitiba",
                "Porto Alegre",
            ],
            n_customers,
        ),
        "estado": np.random.choice(["SP", "RJ", "MG", "PR", "RS"], n_customers),
    }


def generate_base_vendas(
    n_vendas, n_customers, amount_range, base_date, customer_id_offset=0
):
    """Generates a base dictionary for sales (vendas) data with requested names."""
    low_amount, high_amount = amount_range
    return {
        "id": range(1, n_vendas + 1),
        "cliente_id": np.random.randint(
            1000, 1000 + n_customers + customer_id_offset, size=n_vendas
        ),
        "preco": np.random.uniform(low_amount, high_amount, size=n_vendas),
        "produto": np.random.choice(
            ["Laptop", "Smartphone", "Tablet", "Monitor", "Keyboard"], n_vendas
        ),
        "data": [base_date] * n_vendas,
    }
