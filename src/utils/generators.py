"""Shared utility functions for synthetic data generation."""
from datetime import datetime
import numpy as np


def generate_base_customers(n_customers):
    """Generates a base dictionary for customer data."""
    return {
        "customer_id": range(1000, 1000 + n_customers),
        "name": [f"Customer_{i}" for i in range(n_customers)],
        "email": [f"customer_{i}@example.com" for i in range(n_customers)],
        "region": np.random.choice(
            ["North", "South", "East", "West", "Central"], n_customers
        ),
    }


def generate_base_vendas(n_vendas, n_customers, amount_range, base_date,
                         customer_id_offset=0):
    """Generates a base dictionary for sales (vendas) data with requested names."""
    low_amount, high_amount = amount_range
    return {
        "id": range(1, n_vendas + 1),
        "customer_id": np.random.randint(
            1000, 1000 + n_customers + customer_id_offset, size=n_vendas
        ),
        "sale_value": np.random.uniform(low_amount, high_amount, size=n_vendas),
        "category": np.random.choice(
            ["Electronics", "Home", "Fashion", "Grocery", "Garden"], n_vendas
        ),
        "sale_date": [base_date] * n_vendas,
    }
